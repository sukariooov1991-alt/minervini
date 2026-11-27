"""MINERVINI web server & mobile-first dark UI (minervini_v2 structure)."""
from __future__ import annotations

import hmac
from collections import deque
import json
import logging
import math
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, render_template, request, stream_with_context

from data.longbridge_stream import LongbridgeRealtimeClient
from data.storage_manager import StorageManager
from data.market_time import MarketTime, get_us_equity_market_close
from data.trade_levels import build_trade_levels, compute_atr, evaluate_entry_range
from data.sp500_symbols import SP500_SYMBOLS
from data.telegram_notifier import TelegramNotifier
from screener import DailyScreener
from analysis.vcp import check_vcp, check_breakout
from analysis.pullback import check_pullback
from analysis.trend_template import check_trend_template
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("minervini_v2_app")

BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, template_folder=str(BASE_DIR))


@app.before_request
def require_dashboard_access():
    """Protect the dashboard and its HTTP APIs with a Render-managed access key."""
    access_key = os.getenv("MINERVINI_ACCESS_KEY", "").strip()
    if not access_key:
        return jsonify({"error": "dashboard access is not configured"}), 503

    credentials = request.authorization
    supplied_key = credentials.password if credentials and credentials.type.lower() == "basic" else ""
    if not hmac.compare_digest(supplied_key or "", access_key):
        response = jsonify({"error": "authentication required"})
        response.status_code = 401
        response.headers["WWW-Authenticate"] = 'Basic realm="MINERVINI", charset="UTF-8"'
        return response
    return None


MARKET_STATES = ["مفتوح", "بعد الإغلاق", "تداول ليلي", "قبل الافتتاح", "مغلق"]


def _quote_timestamp_order(value: Any) -> float | None:
    """Normalize SDK quote timestamps for ordering pushes when sequence is absent."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    try:
        text_value = str(value).strip()
        if text_value.isdigit():
            number = float(text_value)
            return number if math.isfinite(number) and number > 0 else None
        parsed = pd.Timestamp(value)
        if pd.isna(parsed):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.tz_localize("UTC")
        timestamp = float(parsed.timestamp())
        return timestamp if timestamp > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _quote_timestamp_iso(value: Any) -> str | None:
    """Serialize the provider's Unix/UTC quote time unambiguously for the browser."""
    timestamp = _quote_timestamp_order(value)
    if timestamp is None:
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _latest_realtime_activity(
    last_push: float | None, last_subscription: float | None
) -> float | None:
    """Use the newest known stream activity or subscription as the watchdog baseline."""
    values = [value for value in (last_push, last_subscription) if value is not None]
    return max(values) if values else None


def _stable_pullback_levels(
    card: dict[str, Any], pullback_info: dict[str, Any], enabled: bool
) -> tuple[float | None, float | None]:
    """Freeze Pullback's first confirmed trigger/support instead of chasing each tick."""
    if not enabled:
        return None, None

    def positive(value):
        try:
            number = float(value)
            return number if math.isfinite(number) and number > 0 else None
        except (TypeError, ValueError):
            return None

    current_trigger = positive(pullback_info.get("entry_trigger_price"))
    current_support = positive(pullback_info.get("support_low"))
    anchor = positive(card.get("pullback_entry_anchor"))
    support = positive(card.get("pullback_support_low"))

    # A new lower low invalidates the previously frozen support/trigger pair.
    # If price has already turned up again, start a new confirmation from that
    # lower structural low rather than keeping a stop above the new low.
    if (
        anchor is not None
        and support is not None
        and current_support is not None
        and current_support < support
    ):
        anchor = current_trigger
        support = current_support

    if anchor is None:
        anchor = current_trigger
    if support is None or support >= anchor:
        support = current_support
    if anchor is None or support is None or support >= anchor:
        return anchor, None
    return anchor, support


class ScanController:
    def __init__(self, db_path: str = "stock_data.db", output_path: str = "watchlist.json"):
        self.db_path = db_path
        self.output_path = output_path
        self.storage = StorageManager(db_path)
        self.notifier = TelegramNotifier(state_db_path=db_path)
        self.lb_client = LongbridgeRealtimeClient(on_quote=self._on_realtime_quote)
        # RLock is required because wait_for_update() snapshots while holding the condition.
        self._lock = threading.RLock()
        self._state_cv = threading.Condition(self._lock)
        self._state_version = 0
        self._stop_event = threading.Event()
        self._running = False
        self._market_session_label = self._detect_market_session()
        self._market_status: dict[str, Any] = {
            "regime": "UNKNOWN",
            "market_favorable": False,
            "reasons": [],
        }
        self._cards: dict[str, dict[str, Any]] = {}
        # Candidate reconstruction during startup reads the live-price cache.
        self._live_prices: dict[str, float] = {}
        self._live_quote_metadata: dict[str, dict[str, Any]] = {}
        self._latest_quote_payloads: dict[str, dict[str, Any]] = {}
        # Preserve the evolving session OHLC between WebSocket pushes. Daily
        # history in SQLite is not rewritten on every quote tick.
        self._live_candles: dict[str, dict[str, Any]] = {}
        try:
            # Restore cards before Flask can serve the first /api/state or SSE snapshot.
            self._cards = {
                str(card["symbol"]).upper(): card
                for card in self.storage.load_watchlist_cards()
                if card.get("symbol")
            }
            log.info(
                "Watchlist startup restore: %s cards loaded from SQLite (db=%s)",
                len(self._cards), self.db_path,
            )
        except Exception:
            log.exception("Failed to restore persisted watchlist cards")
            self._cards = {}

        # Merge missing cards from the last complete screener snapshot. This
        # also repairs partial SQLite card tables after an interrupted write.
        try:
            output_file = Path(self.output_path)
            if output_file.is_file():
                saved = json.loads(output_file.read_text(encoding="utf-8"))
                saved_rows = saved.get("results", [])
                saved_market = saved.get("market_status", {})
                if not isinstance(saved_rows, list):
                    raise ValueError("last screener output has no valid results list")
                if not isinstance(saved_market, dict):
                    saved_market = {}
                if saved_market:
                    # Restore the last completed scan's market context alongside
                    # its cards; otherwise the dashboard reports UNKNOWN while
                    # restored cards still carry a previous market decision.
                    self._market_status = {
                        **self._market_status,
                        **saved_market,
                        "snapshot_generated_at": saved.get("generated_at"),
                    }
                recovered = 0
                for row in saved_rows:
                    if not isinstance(row, dict) or not row.get("watchlist_eligible"):
                        continue
                    symbol = str(row.get("symbol", "")).upper().strip()
                    if not symbol or symbol in self._cards:
                        continue
                    try:
                        card = self._evaluate_candidate(
                            row, bool(saved_market.get("market_favorable", False))
                        )
                        if card is None:
                            continue
                        self._cards[symbol] = card
                        self.storage.save_watchlist_card(card)
                        recovered += 1
                    except Exception:
                        log.exception("Startup recovery failed for watchlist symbol %s", symbol)
                log.warning(
                    "Watchlist startup recovery: rebuilt %s missing eligible cards from %s",
                    recovered, output_file,
                )
        except FileNotFoundError:
            log.info("No previous screener snapshot available for startup card recovery")
        except Exception:
            log.exception("Failed to recover watchlist cards from last screener output")

        # Refresh persisted explanations from the latest stored OHLCV history.
        # This prevents old vendor-centric smart_reading strings surviving a restart.
        for symbol, card in list(self._cards.items()):
            try:
                history = self.storage.get_candles_df(symbol)
                self._refresh_restored_pullback(symbol, card, history)
                card["smart_reading"] = self._build_smart_reading_from_card(card, history)
                self.storage.save_watchlist_card(card)
            except Exception:
                log.exception("Failed to refresh startup technical reading for %s", symbol)

        self._realtime_locks: dict[str, threading.Lock] = {}
        self._ws_seen_symbols: set[str] = set()
        self._latest_quote_sequence: dict[str, int] = {}
        self._latest_quote_timestamp: dict[str, float] = {}
        self._processed_quote_timestamp: dict[str, float] = {}
        self._last_ws_reconnect_monotonic = 0.0
        self._last_ws_subscription_monotonic: float | None = None
        self._ws_last_log_at: dict[str, float] = {}
        # Quote callbacks must stay short: the SDK dispatches pushes on its
        # receive path. Recalculate setups on this independent worker so one
        # slow VCP/SQLite pass cannot stall the WebSocket stream for other symbols.
        self._realtime_pending: deque[dict[str, Any]] = deque()
        self._realtime_event = threading.Event()
        # One worker cannot keep up with concurrent pushes from a 493-symbol
        # watchlist. Multiple consumers process different symbols in parallel;
        # per-symbol locks preserve each symbol's calculation order.
        self._realtime_workers = [
            threading.Thread(
                target=self._realtime_worker_loop,
                name=f"longbridge-quote-processor-{index + 1}",
                daemon=True,
            )
            for index in range(4)
        ]
        for worker in self._realtime_workers:
            worker.start()
        self._ws_connected: bool = False
        self._last_error: str | None = None
        self._scan_errors: list[dict[str, str]] = []
        self._scan_progress: dict[str, Any] = {"phase": "idle", "completed": 0, "total": 0, "percent": 0, "current_symbol": ""}
        if self._cards and self.lb_client.has_credentials():
            self._subscribe_watchlist_symbols(list(self._cards), context="startup restore")
        self._realtime_watchdog = threading.Thread(
            target=self._realtime_watchdog_loop,
            name="longbridge-realtime-watchdog",
            daemon=True,
        )
        self._realtime_watchdog.start()
        # Independent background maintenance: refresh durable research caches
        # every four hours, including holidays, and OHLCV after completed US
        # sessions. It never runs a stock scan or changes watchlist eligibility.
        self._daily_history_stop = threading.Event()
        self._daily_history_scheduler = threading.Thread(
            target=self._daily_history_scheduler_loop,
            name="daily-history-refresh-scheduler",
            daemon=True,
        )
        self._daily_history_scheduler.start()

    def _daily_history_scheduler_loop(self) -> None:
        """Refresh durable research caches and the rolling 300-bar OHLCV universe."""
        last_research_refresh = time.monotonic() - 4 * 3600
        while not self._daily_history_stop.is_set():
            try:
                now_et = MarketTime.now()
                if time.monotonic() - last_research_refresh >= 4 * 3600:
                    research_screener = DailyScreener(
                        db_path=self.db_path,
                        output_path=self.output_path,
                        symbols=SP500_SYMBOLS,
                    )
                    research_failures = research_screener.refresh_universe_research_data()
                    last_research_refresh = time.monotonic()
                    log.info(
                        "Background news/earnings refresh checked: symbols=%s news_failures=%s fundamentals_failures=%s",
                        len(SP500_SYMBOLS),
                        len(research_failures.get("news", [])),
                        len(research_failures.get("fundamentals", [])),
                    )
                market_close = get_us_equity_market_close(now_et.date())
                if market_close is None:
                    self._daily_history_stop.wait(300)
                    continue

                refresh_at = datetime.combine(
                    now_et.date(), market_close, tzinfo=now_et.tzinfo
                ) + pd.Timedelta(hours=1).to_pytimedelta()
                if now_et < refresh_at:
                    wait_seconds = min(300, max(30, (refresh_at - now_et).total_seconds()))
                    self._daily_history_stop.wait(wait_seconds)
                    continue

                # ensure_daily_history() is date-based, not TTL-based. Repeated
                # passes are cheap for symbols already current and retry providers
                # that have not yet published today's completed candle.
                screener = DailyScreener(
                    db_path=self.db_path,
                    output_path=self.output_path,
                    symbols=SP500_SYMBOLS,
                )
                failures = screener.update_universe_data(period="2y")
                log.info(
                    "Background daily history refresh completed: date=%s symbols=%s failures=%s",
                    now_et.date().isoformat(), len(SP500_SYMBOLS), len(failures),
                )
                if failures:
                    log.warning(
                        "Background daily history refresh failures: %s",
                        ", ".join(item.get("symbol", "?") for item in failures[:30]),
                    )

                # Recheck the completed daily history after each cache cooldown.
                self._daily_history_stop.wait(15 * 60)
            except Exception:
                log.exception("Background daily history scheduler failed")
                self._daily_history_stop.wait(60)

    def _refresh_restored_pullback(
        self, symbol: str, card: dict[str, Any], history: pd.DataFrame
    ) -> None:
        """Re-evaluate saved Pullback cards so obsolete entry flags cannot survive restart."""
        if history is None or history.empty or "close" not in history.columns:
            card.update({
                "pullback_passed": False,
                "pullback_candidate": False,
                "pullback_turning_up": False,
                "pullback_entry_anchor": None,
                "pullback_support_low": None,
                "trend_template_passed": False,
                "trend_conditions": {},
                "watchlist_eligible": False,
                "vcp_passed": False,
                "breakout_passed": False,
                "setup_passed": False,
                "entry_confirmation_passed": False,
                "entry_ready": False,
                "passed_all_filters": False,
                "pivot_price": None,
                "final_low": None,
                "entry_zones": [],
                "targets": [],
                "shared_stop_loss": None,
                "levels_status": "غير مكتمل — سجل الشموع غير متوفر",
                "within_entry_range": None,
                "status": "بيانات السعر غير متاحة — لا توجد إشارة دخول",
                "stage": "المرحلة 1: انتظار بيانات صالحة",
            })
            for item in card.get("filters", []):
                if item.get("name") in {"VCP Pattern", "VCP Breakout Confirmation", "Pullback Setup", "Pullback Entry Confirmation"}:
                    item["passed"] = False
                elif item.get("name") == "Market Regime Favorable":
                    item["pending"] = True
            return

        valid_closes = pd.to_numeric(history["close"], errors="coerce").dropna()
        if valid_closes.empty:
            card.update({
                "pullback_passed": False,
                "pullback_candidate": False,
                "pullback_turning_up": False,
                "pullback_entry_anchor": None,
                "pullback_support_low": None,
                "vcp_passed": False,
                "breakout_passed": False,
                "setup_passed": False,
                "entry_confirmation_passed": False,
                "entry_ready": False,
                "passed_all_filters": False,
                "pivot_price": None,
                "final_low": None,
                "entry_zones": [],
                "targets": [],
                "shared_stop_loss": None,
                "levels_status": "غير مكتمل — أسعار الإغلاق غير صالحة",
                "within_entry_range": None,
                "status": "بيانات السعر غير صالحة — لا توجد إشارة دخول",
                "stage": "المرحلة 1: انتظار بيانات صالحة",
            })
            for item in card.get("filters", []):
                if item.get("name", "").startswith("Trend Template — "):
                    item["passed"] = False
                elif item.get("name") in {
                    "VCP Pattern", "VCP Breakout Confirmation",
                    "Breakout Confirmation", "Pullback Setup",
                    "Pullback Entry Confirmation", "Pullback Check",
                }:
                    item["passed"] = False
                elif item.get("name") == "Market Regime Favorable":
                    item["pending"] = True
            return

        # Recompute the Trend Template and both setup paths from durable
        # completed candles. Persisted flags may reflect an older daily scan
        # or a transient intraday candle before restart.
        tt_info = check_trend_template(history, card.get("rs_rating"))
        trend_template_passed = bool(tt_info.get("passed", False))
        trend_conditions = dict(tt_info.get("conditions") or {})
        accumulation_passed = bool(card.get(
            "accumulation_passed",
            next((item.get("passed", False) for item in card.get("filters", []) if item.get("name") == "Accumulation"), False),
        ))
        earnings_passed = bool(card.get(
            "earnings_passed",
            next((item.get("passed", False) for item in card.get("filters", []) if item.get("name") == "Earnings Growth"), False),
        ))
        watchlist_eligible = bool(trend_template_passed and accumulation_passed and earnings_passed)
        pullback = check_pullback(history)
        pullback_passed = bool(pullback.get("passed", False))
        pullback_candidate = bool(pullback.get("candidate", False))
        completed_history = history.iloc[:-1].copy() if len(history) > 1 else history.iloc[0:0].copy()
        vcp_info = check_vcp(completed_history)
        vcp_passed = bool(vcp_info.get("passed", False))
        vcp_pivot = vcp_info.get("pivot_price") if vcp_passed else None
        breakout_passed = bool(
            vcp_passed and vcp_pivot is not None
            and check_breakout(history, pivot=vcp_pivot)
        )
        vcp_confirmed = bool(vcp_passed and breakout_passed)
        setup_passed = bool(vcp_passed or pullback_candidate)

        pullback_anchor, pullback_support = _stable_pullback_levels(
            card, pullback, pullback_passed and not vcp_confirmed
        )
        card.update({
            "vcp_passed": vcp_passed,
            "breakout_passed": breakout_passed,
            "pullback_passed": pullback_passed,
            "pullback_candidate": pullback_candidate,
            "pullback_turning_up": bool(pullback.get("turning_up", False)),
            "pullback_entry_anchor": pullback_anchor,
            "pullback_support_low": pullback_support,
        })

        current_price = float(
            self._live_prices.get(symbol)
            or pd.to_numeric(history["close"], errors="coerce").dropna().iloc[-1]
        )
        entry_confirmation = bool(vcp_confirmed or pullback_passed)
        active_pivot = vcp_pivot if vcp_confirmed else pullback_anchor
        active_low = vcp_info.get("final_low") if vcp_confirmed else pullback_support
        plan = build_trade_levels(
            symbol=symbol,
            current_price=current_price,
            pivot_price=active_pivot,
            candles_df=history,
            final_low=active_low,
        ).to_dict()
        market_favorable = bool(card.get("market_favorable", False))
        entry_ready = bool(
            watchlist_eligible
            and entry_confirmation
            and market_favorable
            and plan.get("status") == "جاهز"
            and plan.get("within_entry_range") is True
        )
        card.update({
            "current_price": round(current_price, 4),
            "price_source": (
                "Longbridge WebSocket"
                if symbol in self._live_prices
                else "Stored daily data (Daily Close)"
            ),
            # A restored card's persisted WebSocket timestamp is historical,
            # not proof that a live quote is still arriving after restart.
            "price_session": (
                getattr(self, "_live_quote_metadata", {}).get(symbol, {}).get("price_session", "regular")
                if symbol in self._live_prices else "daily-close"
            ),
            "price_timestamp": (
                getattr(self, "_live_quote_metadata", {}).get(symbol, {}).get("price_timestamp")
                if symbol in self._live_prices else None
            ),
            "price_received_at": (
                getattr(self, "_live_quote_metadata", {}).get(symbol, {}).get("price_received_at")
                if symbol in self._live_prices else None
            ),
            "price_tag": (
                getattr(self, "_live_quote_metadata", {}).get(symbol, {}).get("price_tag")
                if symbol in self._live_prices else None
            ),
            "pivot_price": plan["pivot_price"],
            "final_low": plan["final_low"],
            "entry_zones": plan["entry_zones"],
            "targets": plan["targets"],
            "shared_stop_loss": plan["shared_stop_loss"],
            "levels_status": plan["status"],
            "within_entry_range": plan["within_entry_range"],
            "notes": plan["notes"],
            "trend_template_passed": trend_template_passed,
            "trend_conditions": trend_conditions,
            "accumulation_passed": accumulation_passed,
            "earnings_passed": earnings_passed,
            "watchlist_eligible": watchlist_eligible,
            "vcp_passed": vcp_passed,
            "breakout_passed": breakout_passed,
            "setup_passed": setup_passed,
            "entry_confirmation_passed": entry_confirmation,
            "entry_ready": entry_ready,
            "passed_all_filters": entry_ready,
        })
        if setup_passed and not entry_confirmation:
            card["status"] = "النموذج قيد التأكيد — لا توجد إشارة دخول"
            card["stage"] = "المرحلة 2: تأكيد نموذج الدخول"
        elif setup_passed and not market_favorable:
            card["status"] = "النموذج تحقق — السوق غير مناسب"
            card["stage"] = "المرحلة 3: تقييم حالة السوق"
        elif setup_passed and not entry_ready:
            card["status"] = "تأكيد النموذج تحقق — مستويات الصفقة أو نطاق الدخول غير مكتمل"
            card["stage"] = "المرحلة 3: التحقق من مستويات الدخول"
        elif entry_ready:
            card["status"] = "إشارة دخول مؤكدة — النموذج والسوق والمستويات مستوفاة"
            card["stage"] = "المرحلة 4: إشارة دخول مؤكدة"
        else:
            card["status"] = "انتظار — مراقبة VCP أو Pullback"
            card["stage"] = "المرحلة 1: سهم مؤهل للمراقبة"

        for item in card.get("filters", []):
            name = item.get("name")
            if name.startswith("Trend Template — "):
                condition_name = name.split(" — ", 1)[1]
                item["passed"] = bool(trend_conditions.get(condition_name, False))
            elif name == "VCP Pattern":
                item["passed"] = vcp_passed
            elif name in {"VCP Breakout Confirmation", "Breakout Confirmation"}:
                item["passed"] = breakout_passed
            elif name == "Pullback Setup":
                item["passed"] = pullback_candidate
            elif name in {"Pullback Entry Confirmation", "Pullback Check"}:
                item["passed"] = pullback_passed
            elif name == "Market Regime Favorable":
                item["passed"] = market_favorable
                item["pending"] = not setup_passed

    def _realtime_watchdog_loop(self) -> None:
        """Retry missing subscriptions and reconnect a stale stream during trading sessions."""
        while True:
            time.sleep(30)
            if not self.lb_client.has_credentials():
                continue
            session_label = self._detect_market_session()
            # Longbridge can publish extended/overnight quotes when the regular
            # exchange calendar says closed (weekends/holidays included).
            # Keep the stream supervised; use the extended-session timeout.
            stale_after_seconds = 300 if session_label == "مفتوح" else 900
            now_mono = time.monotonic()
            with self._lock:
                # Keep the existing stream healthy during a long scan. If a
                # reconnect is needed, restore the current card set in one call;
                # defer ordinary missing-symbol sync until the completed bulk sync.
                scan_running = self._running
                symbols = sorted(self._cards)
                last_push = self.lb_client.last_message_monotonic
                last_subscription = self._last_ws_subscription_monotonic
                stream_reference = _latest_realtime_activity(last_push, last_subscription)
                last_reconnect = self._last_ws_reconnect_monotonic
                stale_stream = bool(
                    symbols
                    and stream_reference is not None
                    and now_mono - stream_reference > stale_after_seconds
                    and now_mono - last_reconnect > 120
                )
                if stale_stream:
                    self._last_ws_reconnect_monotonic = now_mono
            if not symbols or (scan_running and not stale_stream):
                continue
            try:
                if stale_stream:
                    log.warning(
                        "Longbridge WebSocket has had no quote pushes for %s seconds; reconnecting %s symbols",
                        stale_after_seconds, len(symbols),
                    )
                    self.lb_client.reconnect(symbols)
                    with self._state_cv:
                        # A new QuoteContext can restart its sequence counter;
                        # discard the old ordering state or valid new ticks may
                        # be rejected forever as older than the previous socket.
                        self._latest_quote_sequence.clear()
                        self._latest_quote_timestamp.clear()
                        self._processed_quote_timestamp.clear()
                        self._ws_seen_symbols.clear()
                        self._ws_last_log_at.clear()
                        self._last_ws_subscription_monotonic = time.monotonic()
                        self._ws_connected = False
                        self._notify_state_changed_locked()
                    continue
                restored = self.lb_client.ensure_subscriptions(symbols)
                if restored:
                    with self._lock:
                        self._last_ws_subscription_monotonic = time.monotonic()
                    log.warning(
                        "Longbridge watchdog bulk-subscribed missing symbols: %s",
                        ",".join(restored),
                    )
            except Exception:
                log.exception("Longbridge realtime watchdog failed")
                with self._state_cv:
                    self._ws_connected = False
                    self._notify_state_changed_locked()

    def _subscribe_watchlist_symbols(self, symbols: list[str], context: str) -> None:
        """Bulk-subscribe the watchlist; live card prices arrive only via WebSocket."""
        clean = sorted({str(s).upper().strip() for s in symbols if str(s).strip()})
        if not clean:
            return
        if not self.lb_client.has_credentials():
            log.warning("Longbridge realtime disabled: credentials are incomplete (%s)", context)
            return
        try:
            newly_subscribed = self.lb_client.subscribe(clean)
            if newly_subscribed:
                with self._lock:
                    self._last_ws_subscription_monotonic = time.monotonic()
            log.info(
                "Longbridge WebSocket subscription sync: requested=%s newly_subscribed=%s (%s)",
                len(clean), len(newly_subscribed), context,
            )
        except Exception:
            with self._state_cv:
                self._ws_connected = False
                self._notify_state_changed_locked()
            log.exception("Longbridge WebSocket subscription failed (%s)", context)
            return


    def _notify_state_changed_locked(self) -> None:
        self._state_version += 1
        self._state_cv.notify_all()

    @staticmethod
    def _detect_market_session() -> str:
        override = os.getenv("MARKET_SESSION_LABEL", "").strip()
        if override:
            if override in MARKET_STATES:
                return override
            log.warning("Invalid MARKET_SESSION_LABEL=%r; falling back to central MarketTime calendar", override)
        return MarketTime.session_label_ar()

    def _on_realtime_quote(self, quote: dict[str, Any]) -> None:
        """Publish each tick immediately, then queue technical work off the SDK thread."""
        sym = str(quote.get("symbol", "")).upper().strip()
        if not sym:
            return
        raw_price = quote.get("last_done")
        if raw_price is None:
            with self._lock:
                cached_price = getattr(self, "_live_prices", {}).get(sym)
            if cached_price is None:
                return
            price = float(cached_price)
        else:
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                return
        if not (price > 0 and math.isfinite(price)):
            return

        sequence = quote.get("sequence")
        try:
            sequence = int(sequence) if sequence is not None and int(sequence) > 0 else None
        except (TypeError, ValueError, OverflowError):
            sequence = None

        now_mono = time.monotonic()
        quote_order = _quote_timestamp_order(quote.get("timestamp"))
        received_at = datetime.now(timezone.utc).isoformat()
        with self._state_cv:
            previous_meta = self._live_quote_metadata.get(sym, {})
            quote_metadata = {
                "price_session": (
                    quote.get("trade_session")
                    or previous_meta.get("price_session")
                    or "regular"
                ),
                "price_timestamp": (
                    _quote_timestamp_iso(quote.get("timestamp"))
                    or previous_meta.get("price_timestamp")
                ),
                "price_tag": (
                    quote.get("tag")
                    if quote.get("tag") is not None
                    else previous_meta.get("price_tag")
                ),
                "price_received_at": received_at,
            }
            previous_sequence = self._latest_quote_sequence.get(sym)
            previous_timestamp = self._latest_quote_timestamp.get(sym)
            if sequence is not None and previous_sequence is not None and sequence <= previous_sequence:
                return
            if (
                sequence is None
                and quote_order is not None
                and previous_timestamp is not None
                and quote_order < previous_timestamp
            ):
                log.debug("Ignoring out-of-order sequence-less quote for %s", sym)
                return
            if sequence is not None:
                self._latest_quote_sequence[sym] = sequence
            if quote_order is not None:
                self._latest_quote_timestamp[sym] = max(previous_timestamp or quote_order, quote_order)
            self._ws_connected = True
            self._live_prices[sym] = price
            self._live_quote_metadata[sym] = quote_metadata
            self._latest_quote_payloads[sym] = dict(quote)
            card = self._cards.get(sym)
            if card is not None:
                card.update({
                    "current_price": round(price, 4),
                    "price_source": "Longbridge WebSocket",
                    **quote_metadata,
                    "updated_at": received_at,
                })
            should_log = now_mono - self._ws_last_log_at.get(sym, 0.0) >= 15.0
            if should_log:
                self._ws_last_log_at[sym] = now_mono
            self._notify_state_changed_locked()
        if should_log:
            log.info(
                "Longbridge WebSocket quote tick: symbol=%s price=%s session=%s sequence=%s timestamp=%s",
                sym, price, quote.get("trade_session"), sequence, quote.get("timestamp"),
            )

        # Preserve every price tick for the requested per-tick setup recalculation.
        # UI publication above never waits for this background work.
        with self._lock:
            queued_quote = dict(quote)
            queued_quote["_received_at"] = received_at
            self._realtime_pending.append(queued_quote)
            self._realtime_event.set()

    def _realtime_worker_loop(self) -> None:
        while True:
            self._realtime_event.wait()
            with self._lock:
                if not self._realtime_pending:
                    self._realtime_event.clear()
                    continue
                # Take one item at a time so several workers can drain the
                # shared queue concurrently instead of one worker stealing a
                # whole batch while the others remain idle.
                quote = self._realtime_pending.popleft()
                if not self._realtime_pending:
                    self._realtime_event.clear()
            try:
                self._process_realtime_quote(quote)
            except Exception:
                log.exception(
                    "Queued realtime processing failed for %s", quote.get("symbol")
                )

    def _merge_live_candle(
        self,
        symbol: str,
        stored_bar: dict[str, Any] | None,
        price: float,
        quote_volume: float | None,
        session_date,
    ) -> dict[str, Any]:
        """Merge a WebSocket tick into the in-memory OHLC for the current session."""
        date_key = session_date.isoformat() if hasattr(session_date, "isoformat") else str(session_date)

        def finite_or(value, fallback):
            try:
                number = float(value)
                return number if math.isfinite(number) else fallback
            except (TypeError, ValueError):
                return fallback

        with self._lock:
            cached = self._live_candles.get(symbol)
            if cached and cached.get("date") == date_key:
                bar = dict(cached)
                if stored_bar is not None:
                    # If a refreshed daily candle arrives after the first
                    # WebSocket tick, merge its official session OHLC too.
                    stored_open = finite_or(stored_bar.get("open"), bar["open"])
                    bar["open"] = stored_open
                    bar["high"] = max(
                        finite_or(bar.get("high"), price),
                        finite_or(stored_bar.get("high"), price),
                    )
                    bar["low"] = min(
                        finite_or(bar.get("low"), price),
                        finite_or(stored_bar.get("low"), price),
                    )
                    bar["volume"] = max(
                        finite_or(bar.get("volume"), 0.0),
                        finite_or(stored_bar.get("volume"), 0.0),
                    )
            elif stored_bar is not None:
                bar = {
                    "date": date_key,
                    "open": finite_or(stored_bar.get("open"), price),
                    "high": finite_or(stored_bar.get("high"), price),
                    "low": finite_or(stored_bar.get("low"), price),
                    "close": finite_or(stored_bar.get("close"), price),
                    "volume": max(0.0, finite_or(stored_bar.get("volume"), 0.0)),
                }
            else:
                # PushQuote does not carry the session open. When no daily
                # candle exists yet, the first observed tick seeds the bar.
                bar = {
                    "date": date_key,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 0.0,
                }

            bar["high"] = max(finite_or(bar.get("high"), price), price)
            bar["low"] = min(finite_or(bar.get("low"), price), price)
            bar["close"] = price
            if quote_volume is not None and math.isfinite(quote_volume) and quote_volume >= 0:
                bar["volume"] = max(
                    finite_or(bar.get("volume"), 0.0), float(quote_volume)
                )
            bar["date"] = date_key
            self._live_candles[symbol] = dict(bar)
            return dict(bar)

    def _resolve_quote_volume(
        self,
        symbol: str,
        quote: dict[str, Any],
        stored_volume: Any,
        quote_order: float | None,
    ) -> float | None:
        """Resolve cumulative volume or deduplicated incremental PushQuote volume."""
        try:
            base_volume = float(stored_volume or 0.0)
        except (TypeError, ValueError):
            base_volume = 0.0
        live_bar = getattr(self, "_live_candles", {}).get(symbol) or {}
        try:
            base_volume = max(base_volume, float(live_bar.get("volume") or 0.0))
        except (TypeError, ValueError):
            pass
        if not math.isfinite(base_volume) or base_volume < 0:
            base_volume = 0.0

        sequence = quote.get("sequence")
        try:
            sequence = int(sequence) if sequence is not None else None
        except (TypeError, ValueError, OverflowError):
            sequence = None

        raw_total = quote.get("volume")
        raw_delta = quote.get("current_volume")
        try:
            delta_hint = float(raw_delta) if raw_delta is not None else 0.0
        except (TypeError, ValueError):
            delta_hint = 0.0
        if raw_total is not None:
            try:
                total = float(raw_total)
            except (TypeError, ValueError):
                total = float("nan")
            # Protobuf numeric fields may be zero when omitted. If the SDK
            # supplies a positive current_volume delta alongside volume=0,
            # treat zero as the omitted cumulative field, not a reset.
            total_is_authoritative = (
                math.isfinite(total)
                and total >= 0
                and (total > 0 or delta_hint <= 0)
            )
            if total_is_authoritative:
                if sequence is not None:
                    seen = getattr(self, "_live_volume_sequence", None)
                    if seen is None:
                        self._live_volume_sequence = {}
                        seen = self._live_volume_sequence
                    seen[symbol] = max(seen.get(symbol, sequence), sequence)
                elif quote_order is not None:
                    seen = getattr(self, "_live_volume_timestamp", None)
                    if seen is None:
                        self._live_volume_timestamp = {}
                        seen = self._live_volume_timestamp
                    seen[symbol] = max(seen.get(symbol, quote_order), quote_order)
                return max(base_volume, total)

        if raw_delta is None:
            return None
        try:
            delta = float(raw_delta)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(delta) or delta < 0:
            return None

        if sequence is not None:
            seen = getattr(self, "_live_volume_sequence", None)
            if seen is None:
                self._live_volume_sequence = {}
                seen = self._live_volume_sequence
            previous = seen.get(symbol)
            if previous is not None and sequence <= previous:
                return base_volume
            seen[symbol] = sequence
        else:
            if quote_order is None:
                return None
            seen = getattr(self, "_live_volume_timestamp", None)
            if seen is None:
                self._live_volume_timestamp = {}
                seen = self._live_volume_timestamp
            previous = seen.get(symbol)
            if previous is not None and quote_order < previous:
                return base_volume
            seen[symbol] = max(previous or quote_order, quote_order)

        if quote.get("_replay_for_recalc"):
            return base_volume
        return base_volume + delta

    def _process_realtime_quote(self, quote: dict[str, Any]) -> None:
        sym = str(quote.get("symbol", "")).upper().strip()
        raw_price = quote.get("last_done")
        if not sym:
            return
        if raw_price is None:
            with self._lock:
                cached_price = getattr(self, "_live_prices", {}).get(sym)
            if cached_price is None:
                return
            price = float(cached_price)
        else:
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                return
        if not (price > 0 and math.isfinite(price)):
            return
        received_at = str(
            quote.get("_received_at") or datetime.now(timezone.utc).isoformat()
        )
        sequence = quote.get("sequence")
        try:
            sequence = int(sequence) if sequence is not None else None
        except (TypeError, ValueError, OverflowError):
            sequence = None

        quote_order = _quote_timestamp_order(quote.get("timestamp"))
        with self._lock:
            first_ws_push = sym not in self._ws_seen_symbols
            self._ws_seen_symbols.add(sym)
            previous_processed_timestamp = self._processed_quote_timestamp.get(sym)
            if (
                sequence is None
                and quote_order is not None
                and previous_processed_timestamp is not None
                and quote_order < previous_processed_timestamp
            ):
                return
            if quote_order is not None:
                self._processed_quote_timestamp[sym] = max(
                    previous_processed_timestamp or quote_order, quote_order
                )
        if first_ws_push:
            log.info(
                "Longbridge WebSocket first quote push: symbol=%s price=%s session=%s timestamp=%s",
                sym, price, quote.get("trade_session"), quote.get("timestamp"),
            )

        with self._lock:
            symbol_lock = self._realtime_locks.setdefault(sym, threading.Lock())

        # Serialize callbacks per symbol to prevent stale calculations winning.
        with symbol_lock:
            # Price publication already happened on the SDK callback path;
            # this worker only recalculates technical state and trade levels.

            alert_card = None
            try:
                with self._lock:
                    card_snapshot = dict(self._cards.get(sym) or {})
                candles = self.storage.get_candles_df(sym)
                live_df = candles.copy()
                if not live_df.empty:
                    live_df["date"] = pd.to_datetime(live_df["date"], errors="coerce")
                    today = MarketTime.state().trading_date
                    today_mask = live_df["date"].dt.date == today
                    if bool(today_mask.any()):
                        stored_bar = live_df.loc[today_mask].iloc[-1].to_dict()
                    else:
                        # PushQuote does not expose the official session open.
                        # Seed the evolving candle from the previous completed
                        # close instead of treating an arbitrary first tick as
                        # the market open; Pullback's turn-up proxy then compares
                        # the live price against a known completed reference.
                        previous_close = float(live_df["close"].iloc[-1])
                        stored_bar = {
                            "open": previous_close,
                            "high": previous_close,
                            "low": previous_close,
                            "close": previous_close,
                            "volume": 0.0,
                        }
                    quote_volume = self._resolve_quote_volume(
                        sym, quote, stored_bar.get("volume"), quote_order
                    )
                    live_bar = self._merge_live_candle(
                        sym, stored_bar, price, quote_volume, today
                    )
                    if bool(today_mask.any()):
                        idx = live_df.index[today_mask][-1]
                        for field in ("open", "high", "low", "close", "volume"):
                            live_df.at[idx, field] = live_bar[field]
                    else:
                        live_df = pd.concat(
                            [live_df, pd.DataFrame([{
                                **live_bar,
                                "date": pd.Timestamp(today),
                            }])],
                            ignore_index=True,
                        )
                    live_df = live_df.sort_values("date").reset_index(drop=True)

                    # VCP structure uses completed daily bars; the evolving
                    # session candle is used for pullback/breakout confirmation.
                    completed_df = live_df.iloc[:-1].copy()
                    vcp_info = check_vcp(completed_df)
                    pullback_info = check_pullback(live_df)
                    vcp_passed = bool(vcp_info.get("passed", False))
                    pivot = vcp_info.get("pivot_price") if vcp_passed else None
                    breakout_passed = bool(
                        pivot is not None and check_breakout(live_df, pivot=pivot)
                    )
                    pullback_passed = bool(pullback_info.get("passed", False))
                    pullback_anchor = pullback_info.get("entry_trigger_price")
                    pullback_low = pullback_info.get("support_low")
                else:
                    vcp_passed = False
                    pullback_passed = False
                    breakout_passed = False
                    pivot = None
                    pullback_anchor = None
                    pullback_low = None
                    pullback_info = {}
                    vcp_info = {}

                setup_passed = bool(vcp_passed or pullback_info.get("candidate", False))
                tt_info = (
                    check_trend_template(live_df, card_snapshot.get("rs_rating"))
                    if not live_df.empty else {"passed": False, "conditions": {}}
                )
                trend_template_passed = bool(tt_info.get("passed", False))
                trend_conditions = dict(tt_info.get("conditions") or {})
                accumulation_passed = bool(card_snapshot.get(
                    "accumulation_passed",
                    next((item.get("passed", False) for item in card_snapshot.get("filters", []) if item.get("name") == "Accumulation"), False),
                ))
                earnings_passed = bool(card_snapshot.get(
                    "earnings_passed",
                    next((item.get("passed", False) for item in card_snapshot.get("filters", []) if item.get("name") == "Earnings Growth"), False),
                ))
                watchlist_eligible = bool(trend_template_passed and accumulation_passed and earnings_passed)
                with self._state_cv:
                    latest_sequence = self._latest_quote_sequence.get(sym)
                    if sequence is not None and latest_sequence is not None and sequence < latest_sequence:
                        return
                    self._ws_connected = True
                    self._live_prices[sym] = price
                    card = self._cards.get(sym)
                    if card is None:
                        self._notify_state_changed_locked()
                        return

                    was_entry_ready = bool(card.get("entry_ready", False))
                    market_favorable = bool(card.get("market_favorable", False))
                    # Do not reuse a stale VCP pivot when the active path is
                    # Pullback. Until that path supplies its own anchor, levels
                    # remain explicitly unavailable rather than fabricated.
                    use_vcp_entry = bool(vcp_passed and breakout_passed)
                    use_pullback_entry = bool(pullback_passed and not use_vcp_entry)
                    stable_pullback_anchor, stable_pullback_low = _stable_pullback_levels(
                        card, pullback_info, use_pullback_entry
                    )
                    active_pivot = pivot if use_vcp_entry else stable_pullback_anchor
                    active_low = (
                        vcp_info.get("final_low") if use_vcp_entry
                        else stable_pullback_low
                    )
                    plan = build_trade_levels(
                        symbol=sym,
                        current_price=price,
                        pivot_price=active_pivot,
                        candles_df=live_df if not live_df.empty else None,
                        final_low=active_low,
                    ).to_dict() if setup_passed else {
                        "pivot_price": None,
                        "final_low": None,
                        "entry_zones": [],
                        "targets": [],
                        "shared_stop_loss": None,
                        "status": "بانتظار نموذج الدخول",
                        "within_entry_range": None,
                        "notes": ["السهم مؤهل؛ المراقبة مستمرة حتى تحقق VCP أو Pullback."],
                    }

                    entry_confirmation_passed = bool(
                        (vcp_passed and breakout_passed) or pullback_passed
                    )
                    entry_ready = bool(
                        watchlist_eligible
                        and entry_confirmation_passed
                        and market_favorable
                        and plan.get("status") == "جاهز"
                        and plan.get("within_entry_range") is True
                    )
                    latest_meta = self._live_quote_metadata.get(sym, {})
                    card.update({
                        "current_price": round(price, 4),
                        "price_source": "Longbridge WebSocket",
                        "price_session": (
                            quote.get("trade_session")
                            or latest_meta.get("price_session")
                            or card.get("price_session")
                            or "regular"
                        ),
                        "price_timestamp": (
                            _quote_timestamp_iso(quote.get("timestamp"))
                            or latest_meta.get("price_timestamp")
                            or card.get("price_timestamp")
                        ),
                        "price_tag": (
                            quote.get("tag")
                            if quote.get("tag") is not None
                            else latest_meta.get("price_tag")
                        ),
                        "price_received_at": received_at,
                        "pivot_price": plan["pivot_price"],
                        "final_low": plan["final_low"],
                        "entry_zones": plan["entry_zones"],
                        "targets": plan["targets"],
                        "shared_stop_loss": plan["shared_stop_loss"],
                        "levels_status": plan["status"],
                        "within_entry_range": plan["within_entry_range"],
                        "notes": plan["notes"],
                        "setup_passed": setup_passed,
                        "vcp_passed": vcp_passed,
                        "pullback_passed": pullback_passed,
                        "pullback_candidate": bool(pullback_info.get("candidate", False)),
                        "pullback_turning_up": bool(pullback_info.get("turning_up", False)),
                        "pullback_entry_anchor": active_pivot if use_pullback_entry else None,
                        "pullback_support_low": active_low if use_pullback_entry else None,
                        "entry_confirmation_passed": entry_confirmation_passed,
                        "trend_template_passed": trend_template_passed,
                        "trend_conditions": trend_conditions,
                        "accumulation_passed": accumulation_passed,
                        "earnings_passed": earnings_passed,
                        "watchlist_eligible": watchlist_eligible,
                        "breakout_passed": breakout_passed,
                        "entry_ready": entry_ready,
                        "passed_all_filters": entry_ready,
                        "market_favorable": market_favorable,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })
                    if not watchlist_eligible:
                        card["status"] = "شروط الاتجاه أو الأساسيات لم تعد مستوفاة — بانتظار الفحص"
                        card["stage"] = "المرحلة 1: إعادة تقييم أهلية السهم"
                    elif setup_passed and not entry_confirmation_passed:
                        card["status"] = "النموذج قيد التأكيد — لا توجد إشارة دخول"
                        card["stage"] = "المرحلة 2: تأكيد نموذج الدخول"
                    elif setup_passed and not market_favorable:
                        card["status"] = "النموذج تحقق — السوق غير مناسب"
                        card["stage"] = "المرحلة 3: تقييم حالة السوق"
                    elif setup_passed and not entry_ready:
                        card["status"] = "تأكيد النموذج تحقق — مستويات الصفقة أو نطاق الدخول غير مكتمل"
                        card["stage"] = "المرحلة 3: التحقق من مستويات الدخول"
                    elif setup_passed:
                        card["status"] = "إشارة دخول مؤكدة — النموذج والسوق والمستويات مستوفاة"
                        card["stage"] = "المرحلة 4: إشارة دخول مؤكدة"
                    else:
                        card["status"] = "انتظار — مراقبة VCP أو Pullback"
                        card["stage"] = "المرحلة 1: سهم مؤهل للمراقبة"

                    for item in card.get("filters", []):
                        name = item.get("name", "")
                        if name.startswith("Trend Template — "):
                            condition_name = name.split(" — ", 1)[1]
                            item["passed"] = bool(trend_conditions.get(condition_name, False))
                        elif name == "VCP Pattern":
                            item["passed"] = vcp_passed
                        elif name in ("Breakout Confirmation", "VCP Breakout Confirmation"):
                            item["passed"] = breakout_passed
                        elif name == "Pullback Setup":
                            item["passed"] = bool(pullback_info.get("candidate", False))
                        elif name == "Pullback Entry Confirmation":
                            item["passed"] = pullback_passed
                        elif name == "Pullback Check":
                            item["passed"] = pullback_passed
                        elif name == "Market Regime Favorable":
                            item["passed"] = market_favorable
                            item["pending"] = not setup_passed
                    card["smart_reading"] = self._build_smart_reading_from_card(card, live_df)
                    self.storage.save_watchlist_card(card)
                    if not was_entry_ready and card["entry_ready"]:
                        alert_card = dict(card)
                    self._notify_state_changed_locked()

                if alert_card is not None:
                    try:
                        self.notifier.send_stock_appearance_alert(alert_card)
                    except Exception:
                        log.exception("Telegram realtime entry alert failed for %s", sym)
            except Exception:
                # Preserve quote visibility and the last valid setup state if
                # a provider candle or calculation is malformed.
                log.exception("Realtime setup recalculation failed for %s", sym)
                with self._state_cv:
                    latest_sequence = self._latest_quote_sequence.get(sym)
                    if sequence is not None and latest_sequence is not None and sequence < latest_sequence:
                        return
                    self._ws_connected = True
                    self._live_prices[sym] = price
                    card = self._cards.get(sym)
                    if card is not None:
                        card["current_price"] = round(price, 4)
                        card["price_source"] = (
                            "Longbridge WebSocket"
                        )
                        latest_meta = self._live_quote_metadata.get(sym, {})
                        card["price_session"] = (
                            quote.get("trade_session")
                            or latest_meta.get("price_session")
                            or card.get("price_session")
                            or "regular"
                        )
                        card["price_timestamp"] = (
                            _quote_timestamp_iso(quote.get("timestamp"))
                            or latest_meta.get("price_timestamp")
                            or card.get("price_timestamp")
                        )
                        card["price_tag"] = (
                            quote.get("tag")
                            if quote.get("tag") is not None
                            else latest_meta.get("price_tag")
                        )
                        card["price_received_at"] = received_at
                        card["updated_at"] = datetime.now(timezone.utc).isoformat()
                        within_range, range_note = evaluate_entry_range(price, card.get("pivot_price"))
                        card["within_entry_range"] = within_range
                        if range_note and range_note not in card.get("notes", []):
                            card["notes"] = [range_note] + list(card.get("notes") or [])
                        card["smart_reading"] = self._build_smart_reading_from_card(card)
                        try:
                            self.storage.save_watchlist_card(card)
                        except Exception:
                            log.exception("Failed to persist realtime fallback card for %s", sym)
                    self._notify_state_changed_locked()

    def _build_smart_reading_from_card(
        self, card: dict[str, Any], candles_df: pd.DataFrame | None = None
    ) -> str:
        """Build a technical, decision-oriented explanation; never expose feed/vendor metadata."""
        import numpy as np

        sym = str(card.get("symbol") or "السهم")
        rs = card.get("rs_rating")
        regime = str(card.get("market_regime") or "غير محدد")
        within_range = card.get("within_entry_range")
        pivot = card.get("pivot_price")
        final_low = card.get("final_low")
        entries = card.get("entry_zones") or []
        targets = card.get("targets") or []
        stop = card.get("shared_stop_loss")
        notes = [str(x) for x in (card.get("notes") or []) if x]
        sections: list[str] = []

        def fmt(value: Any) -> str:
            try:
                number = float(value)
                return f"{number:.2f}" if np.isfinite(number) else "غير متاح"
            except (TypeError, ValueError):
                return "غير متاح"

        def nearest_levels(values: list[float], current: float, above: bool) -> list[float]:
            valid = sorted({
                round(float(v), 4) for v in values
                if v is not None and np.isfinite(float(v)) and float(v) > 0
                and ((float(v) >= current) if above else (float(v) <= current))
            })
            return valid[:2] if above else valid[-2:]

        # Derive support/resistance and trend from the same daily OHLCV history
        # used by the screener, rather than from the quote provider label.
        if candles_df is not None and not candles_df.empty:
            df = candles_df.copy()
            required = {"high", "low", "close"}
            if required.issubset(df.columns):
                for col in ("high", "low", "close"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["high", "low", "close"]).tail(220)
                if len(df) >= 30:
                    close = df["close"]
                    last = float(close.iloc[-1])
                    ma20 = float(close.tail(20).mean()) if len(df) >= 20 else None
                    ma50 = float(close.tail(50).mean()) if len(df) >= 50 else None
                    ma150 = float(close.tail(150).mean()) if len(df) >= 150 else None
                    ma200 = float(close.tail(200).mean()) if len(df) >= 200 else None
                    trend_parts = []
                    for label, ma in (("20 يوم", ma20), ("50 يوم", ma50), ("150 يوم", ma150), ("200 يوم", ma200)):
                        if ma is not None:
                            relation = "فوق" if last >= ma else "تحت"
                            trend_parts.append(f"{relation} متوسط {label} ({fmt(ma)})")
                    if ma20 is not None and ma50 is not None:
                        trend_state = "صاعد قصير الأجل" if ma20 > ma50 else "ضعيف/هابط قصير الأجل"
                    else:
                        trend_state = "غير مكتمل"
                    sections.append("الاتجاه والبنية: " + trend_state + "؛ " + "، ".join(trend_parts) + ".")

                    # Local swing points over the latest 60 sessions, excluding
                    # the last two bars to avoid treating an unfinished extreme as confirmed.
                    recent = df.tail(60).reset_index(drop=True)
                    lows, highs = [], []
                    for i in range(2, max(2, len(recent) - 2)):
                        lo = float(recent.loc[i, "low"])
                        hi = float(recent.loc[i, "high"])
                        if lo <= float(recent.loc[i-1:i+1, "low"].min()) and lo <= float(recent.loc[i-2:i+2, "low"].min()):
                            lows.append(lo)
                        if hi >= float(recent.loc[i-1:i+1, "high"].max()) and hi >= float(recent.loc[i-2:i+2, "high"].max()):
                            highs.append(hi)
                    supports = nearest_levels(lows, last, above=False)
                    resistances = nearest_levels(highs, last, above=True)
                    support_text = "، ".join(fmt(x) for x in reversed(supports)) if supports else "لم يُعثر على قاع محوري قريب مؤكد"
                    resistance_text = "، ".join(fmt(x) for x in resistances) if resistances else "لم تُرصد قمة محورية أعلى قريبة"
                    sections.append(f"الدعم والمقاومة: أقرب مناطق الدعم {support_text}؛ وأقرب مناطق المقاومة {resistance_text}. هذه مناطق قمم/قيعان يومية تاريخية وليست ضمانًا لارتداد السعر.")

                    # Reuse the trade-plan Wilder ATR so the explanation and
                    # actual stop fallback report the same volatility measure.
                    atr = compute_atr(df, period=14)
                    if atr is not None and np.isfinite(atr):
                        sections.append(f"التذبذب والمخاطر: متوسط المدى الحقيقي ATR(14) يقارب {fmt(atr)}؛ لذلك يجب تقييم مسافة وقف الخسارة وحجم المركز وفق هذا التذبذب.")
                else:
                    sections.append("التحليل الفني: عدد الشموع الصالحة غير كافٍ لاستخراج اتجاه ومستويات دعم ومقاومة موثوقة.")
            else:
                sections.append("التحليل الفني: بيانات OHLC المطلوبة لاستخراج الدعم والمقاومة غير مكتملة.")
        else:
            sections.append("التحليل الفني: سجل الشموع اليومية غير متاح حاليًا؛ لا يمكن تأكيد الدعم والمقاومة أو المتوسطات.")

        if card.get("setup_passed") and not card.get("entry_confirmation_passed"):
            if card.get("vcp_passed"):
                sections.append(
                    "النموذج الفني: رُصدت بنية VCP، لكن اختراق نقطة الارتكاز لم يتأكد بعد؛ لا توجد إشارة دخول."
                )
            else:
                sections.append(
                    "النموذج الفني: رُصد تراجع سعري مرشح لـPullback، لكن السعر لم يؤكد الانعطاف للأعلى بعد؛ لا توجد إشارة دخول."
                )
        elif card.get("setup_passed"):
            setup = (
                "VCP"
                if card.get("vcp_passed") and card.get("breakout_passed")
                else "Pullback"
            )
            sections.append(
                f"النموذج الفني: تأكد {setup}. نقطة الارتكاز المعتمدة {fmt(pivot)}"
                if pivot else
                f"النموذج الفني: تأكد {setup} لكن لا توجد نقطة ارتكاز صالحة؛ لذلك لا تُعرض مناطق دخول أو أهداف أو وقف خسارة مصطنعة."
            )
            if final_low:
                sections.append(f"قاع النموذج: {fmt(final_low)}؛ كسره قد يبطل بنية النموذج بحسب شكل القاعدة.")
        else:
            sections.append("النموذج الفني: السهم اجتاز شروط التأهل الأولية، لكنه ما زال بانتظار ظهور VCP أو Pullback. لا توجد إشارة دخول مكتملة حتى الآن.")

        if entries:
            sections.append("خطة الدخول: المناطق الخمس المحسوبة " + "، ".join(fmt(x) for x in entries) + ".")
        else:
            sections.append("خطة الدخول: لا توجد مناطق دخول مؤكدة حاليًا لعدم توفر ارتكاز صالح للنموذج.")
        if within_range is True:
            sections.append("موضع السعر بالنسبة للخطة: داخل نطاق الدخول المسموح؛ يظل التنفيذ مشروطًا بتأكيد النموذج والسوق.")
        elif within_range is False:
            sections.append("موضع السعر بالنسبة للخطة: خارج نطاق الشراء الممتد 5% فوق الارتكاز؛ تجنب مطاردة الحركة وانتظر فرصة جديدة.")
        else:
            sections.append("موضع السعر بالنسبة للخطة: غير قابل للتقييم قبل توفر نقطة ارتكاز صحيحة.")

        if targets:
            sections.append("الأهداف السعرية المحسوبة للمناطق الخمس: " + "، ".join(fmt(x) for x in targets) + ".")
        else:
            sections.append("الأهداف: غير محددة بعد؛ لا يصح وضع أهداف رقمية قبل اكتمال نموذج الدخول ووجود ارتكاز صالح.")
        sections.append(f"وقف الخسارة: {fmt(stop)}." if stop else "وقف الخسارة: غير محدد بعد؛ لا تعتمد صفقة دون مستوى إبطال فني واضح.")

        market_text = {
            "CORRECTION_OR_DOWNTREND": "السوق العام في تصحيح أو اتجاه هابط، ما يرفع مخاطر نجاح الاختراقات.",
            "UPTREND": "السوق العام في اتجاه صاعد، وهو عامل داعم لكنه لا يغني عن تأكيد السهم.",
            "CONFIRMED_UPTREND": "السوق العام في اتجاه صاعد مؤكد، وهو عامل داعم لكنه لا يغني عن تأكيد السهم.",
            "UPTREND_UNDER_PRESSURE": "الاتجاه العام صاعد لكنه تحت ضغط؛ يلزم تشدد أكبر في تأكيد الدخول.",
            "UNKNOWN": "حالة السوق العام غير محسومة من البيانات المتاحة.",
        }.get(regime.upper(), f"حالة السوق العام: {regime}.")
        sections.append(f"السوق والقوة النسبية: {market_text} تصنيف RS={fmt(rs)}.")
        if card.get("entry_ready"):
            sections.append("القرار الحالي: النموذج والسوق اجتازا شروط الجاهزية؛ انتظر تنفيذًا منضبطًا مع الالتزام بوقف الخسارة.")
        elif card.get("setup_passed") and not card.get("entry_confirmation_passed"):
            sections.append("القرار الحالي: توجد بنية مرشحة، لكن محفز الدخول لم يتأكد بعد؛ انتظر انعطاف السعر للأعلى.")
        elif card.get("setup_passed") and not card.get("market_favorable"):
            sections.append("القرار الحالي: تأكد النموذج لكن ظروف السوق غير مواتية؛ الأفضل عدم تفعيل دخول جديد حتى تتحسن البيئة العامة.")
        else:
            sections.append("القرار الحالي: مراقبة فقط، وليس توصية دخول. انتظر اكتمال النموذج ثم أعد تقييم السوق والمستويات.")
        if notes:
            sections.append("ملاحظات التحليل: " + " ".join(notes[:3]))
        return "\n\n".join(sections)

    def _build_filters_detail(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        filters: list[dict[str, Any]] = []
        for cond_name, passed in (row.get("trend_conditions") or {}).items():
            filters.append({
                "name": f"Trend Template — {cond_name}",
                "passed": bool(passed),
            })
        filters.extend([
            {"name": "VCP Pattern", "passed": bool(row.get("vcp_passed", False))},
            {"name": "VCP Breakout Confirmation", "passed": bool(row.get("breakout", False))},
            {"name": "Pullback Setup", "passed": bool(row.get("pullback_candidate", row.get("pullback_passed", False)))},
            {"name": "Pullback Entry Confirmation", "passed": bool(row.get("pullback_passed", False))},
            {"name": "Accumulation", "passed": bool(row.get("accumulation", False))},
            {"name": "Earnings Growth", "passed": bool(row.get("earnings_passed", False))},
            {
                "name": "Market Regime Favorable",
                "passed": bool(row.get("market_favorable", False)),
                "pending": not bool(row.get("vcp_passed", False) or row.get("pullback_candidate", row.get("pullback_passed", False))),
            },
        ])
        return filters

    def _evaluate_candidate(self, row: dict[str, Any], market_favorable: bool) -> dict[str, Any] | None:
        sym = row["symbol"]
        df = self.storage.get_candles_df(sym)
        if df.empty:
            return None

        rs_val = row.get("rs_rating")
        tt_passed = str(row.get("trend_template", "")).upper() == "PASS"
        vcp_passed = bool(row.get("vcp_passed", False))
        br = bool(row.get("breakout", False))
        pullback_info = check_pullback(df)
        pullback_passed = bool(pullback_info.get("passed", False))
        pullback_candidate = bool(pullback_info.get("candidate", False))
        ad_acc = bool(row.get("accumulation", False))
        earn_passed = bool(row.get("earnings_passed", False))

        watchlist_eligible = bool(tt_passed and ad_acc and earn_passed)
        if not watchlist_eligible:
            return None

        # Independent entry paths: VCP needs its Pivot breakout; Pullback
        # needs an observed turn-up after a recent decline.
        setup_passed = bool(vcp_passed or pullback_candidate)
        entry_confirmation_passed = bool(
            (vcp_passed and br) or pullback_passed
        )

        with self._lock:
            live_p = self._live_prices.get(sym)
            live_quote_metadata = dict(getattr(self, "_live_quote_metadata", {}).get(sym, {}))

        if live_p is not None:
            current_price = float(live_p)
            price_source = "Longbridge WebSocket"
        else:
            current_price = float(df["close"].iloc[-1])
            historical_source = str(row.get("data_source") or "Stored daily data")
            price_source = f"{historical_source} (Daily Close)"

        use_vcp_entry = bool(vcp_passed and br)
        use_pullback_entry = bool(pullback_passed and not use_vcp_entry)
        pullback_anchor, pullback_support = _stable_pullback_levels(
            {}, pullback_info, use_pullback_entry
        )
        pivot_price = row.get("pivot_price") if use_vcp_entry else pullback_anchor
        final_low = row.get("final_low") if use_vcp_entry else pullback_support
        plan = (
            build_trade_levels(
                symbol=sym,
                current_price=current_price,
                pivot_price=pivot_price,
                candles_df=df,
                final_low=final_low,
            ).to_dict()
            if setup_passed else {
                "pivot_price": pivot_price,
                "final_low": final_low,
                "entry_zones": [],
                "targets": [],
                "shared_stop_loss": None,
                "status": "بانتظار نموذج الدخول",
                "within_entry_range": None,
                "notes": ["السهم مؤهل؛ المراقبة مستمرة حتى تحقق VCP أو Pullback."],
            }
        )

        entry_ready = bool(
            entry_confirmation_passed
            and market_favorable
            and plan.get("status") == "جاهز"
            and plan.get("within_entry_range") is True
        )
        if not watchlist_eligible:
            card_status = "شروط الاتجاه أو الأساسيات لم تعد مستوفاة — بانتظار الفحص"
            card_stage = "المرحلة 1: إعادة تقييم أهلية السهم"
        elif not setup_passed:
            card_status = "انتظار — مراقبة VCP أو Pullback"
            card_stage = "المرحلة 1: سهم مؤهل للمراقبة"
        elif not entry_confirmation_passed:
            card_status = "النموذج قيد التأكيد — لا توجد إشارة دخول"
            card_stage = "المرحلة 2: تأكيد نموذج الدخول"
        elif not market_favorable:
            card_status = "النموذج تحقق — السوق غير مناسب"
            card_stage = "المرحلة 3: تقييم حالة السوق"
        elif not entry_ready:
            card_status = "تأكيد النموذج تحقق — مستويات الصفقة أو نطاق الدخول غير مكتمل"
            card_stage = "المرحلة 3: التحقق من مستويات الدخول"
        else:
            card_status = "إشارة دخول مؤكدة — النموذج والسوق والمستويات مستوفاة"
            card_stage = "المرحلة 4: إشارة دخول مؤكدة"

        card = {
            "symbol": sym,
            "current_price": round(current_price, 4),
            "price_source": price_source,
            "price_session": live_quote_metadata.get("price_session", "regular") if live_p is not None else "daily-close",
            "price_timestamp": live_quote_metadata.get("price_timestamp") if live_p is not None else None,
            "price_received_at": live_quote_metadata.get("price_received_at") if live_p is not None else None,
            "pivot_price": plan["pivot_price"],
            "final_low": plan["final_low"],
            "rs_rating": rs_val,
            "market_regime": row.get("market_regime", "UNKNOWN"),
            "status": card_status,
            "stage": card_stage,
            "entry_zones": plan["entry_zones"],
            "targets": plan["targets"],
            "shared_stop_loss": plan["shared_stop_loss"],
            "levels_status": plan["status"],
            "within_entry_range": plan["within_entry_range"],
            "notes": plan["notes"],
            "filters": self._build_filters_detail({
                **row,
                "pullback_passed": pullback_passed,
                "pullback_candidate": pullback_candidate,
            }),
            "watchlist_eligible": True,
            "trend_template_passed": tt_passed,
            "trend_conditions": dict(row.get("trend_conditions") or {}),
            "accumulation_passed": ad_acc,
            "earnings_passed": earn_passed,
            "setup_passed": setup_passed,
            "entry_confirmation_passed": entry_confirmation_passed,
            "breakout_passed": br,
            "vcp_passed": vcp_passed,
            "pullback_passed": pullback_passed,
            "pullback_candidate": pullback_candidate,
            "pullback_turning_up": bool(pullback_info.get("turning_up", False)),
            "pullback_entry_anchor": pullback_anchor if use_pullback_entry else None,
            "pullback_support_low": pullback_support if use_pullback_entry else None,
            "market_favorable": bool(market_favorable),
            "entry_ready": entry_ready,
            "passed_all_filters": entry_ready,
            "shown_on_main_page": True,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        card["smart_reading"] = self._build_smart_reading_from_card(card, df)
        return card

    def _merge_latest_quote_into_card_locked(self, card: dict[str, Any]) -> bool:
        """Preserve and reprocess a tick that arrived before its scan card existed."""
        symbol = str(card.get("symbol") or "").upper()
        latest_quote = self._latest_quote_payloads.get(symbol)
        if latest_quote is None:
            return False
        latest_price = self._live_prices.get(symbol)
        if latest_price is None:
            return False
        latest_meta = self._live_quote_metadata.get(symbol, {})
        card.update({
            "current_price": round(float(latest_price), 4),
            "price_source": "Longbridge WebSocket",
            **latest_meta,
        })
        # The quote worker may already have consumed this tick while no card
        # existed. Requeue it now so technical state and trade levels catch up.
        queued_quote = dict(latest_quote)
        queued_quote.setdefault("_received_at", latest_meta.get("price_received_at"))
        queued_quote["_replay_for_recalc"] = True
        self._realtime_pending.append(queued_quote)
        self._realtime_event.set()
        return True

    def _update_scan_progress(self, progress: dict[str, Any]) -> None:
        total = int(progress.get("total") or 0)
        completed = int(progress.get("completed") or 0)
        with self._state_cv:
            self._scan_progress = {
                **progress,
                "percent": min(100, int(completed * 100 / total)) if total else 0,
            }
            if progress.get("error"):
                self._scan_errors.append({
                    "symbol": str(progress.get("current_symbol") or "SYSTEM"),
                    "phase": str(progress.get("phase") or "unknown"),
                    "message": str(progress["error"]),
                })
                self._scan_errors = self._scan_errors[-50:]
            self._notify_state_changed_locked()

    def run_once(self, symbols: list[str] | None = None, update: bool = True) -> None:
        with self._state_cv:
            self._running = True
            self._last_error = None
            # Keep existing watchlist cards visible while the next scan runs.
            # They are reconciled against the new eligible universe after a
            # complete scan, so a temporary scan gap does not blank the UI.
            self._market_session_label = self._detect_market_session()
            self._notify_state_changed_locked()
        try:
            screener = DailyScreener(db_path=self.db_path, output_path=self.output_path, symbols=symbols)
            def publish_batch(rows: list[dict[str, Any]], market_status: dict[str, Any]) -> None:
                qualified = []
                for row in rows:
                    if not row.get("watchlist_eligible"): continue
                    try:
                        card = self._evaluate_candidate(row, bool(market_status.get("market_favorable", False)))
                        if card: qualified.append(card)
                    except Exception:
                        log.exception("Candidate card construction failed for %s", row.get("symbol"))
                if not qualified: return
                with self._state_cv:
                    for card in qualified:
                        symbol = str(card["symbol"]).upper()
                        # A tick can arrive before the scan card is published.
                        self._merge_latest_quote_into_card_locked(card)
                        self._cards[symbol] = card
                        try:
                            self.storage.save_watchlist_card(card)
                        except Exception:
                            log.exception("Failed to persist watchlist card for %s", symbol)
                    self._notify_state_changed_locked()
                for card in qualified:
                    if not card.get("entry_ready"):
                        continue
                    try: self.notifier.send_stock_appearance_alert(card)
                    except Exception: log.exception("Telegram alert failed for %s", card.get("symbol"))
                # Do not subscribe per analysis batch. All newly eligible
                # symbols are collected first, then sent in one bulk request
                # after reconciliation below.
            result_df, failures, market_status = screener.run_screen(
                update=update, progress_callback=self._update_scan_progress,
                batch_callback=publish_batch, stop_callback=self._stop_event.is_set, batch_size=5,
            )
            removed: set[str] = set()
            with self._state_cv:
                self._market_status = market_status
                if not self._stop_event.is_set():
                    # A symbol absent from results is removable only when its
                    # download and analysis completed successfully. A transient
                    # provider/analysis failure is not evidence of lost eligibility.
                    uncertain_symbols = {
                        str(item.get("symbol", "")).upper().strip()
                        for item in failures
                        if item.get("symbol")
                    }
                    # Only remove a persisted card when that exact symbol was
                    # fully evaluated in this scan. The result DataFrame contains
                    # Trend-Template passes only, so absence from it is NOT proof
                    # that a previously eligible symbol failed the strategy.
                    evaluated_symbols = {
                        str(symbol).upper().strip()
                        for symbol in (market_status.get("evaluated_symbols") or [])
                        if str(symbol).strip()
                    }
                    eligible_symbols = {
                        str(symbol).upper().strip()
                        for symbol in (
                            result_df.loc[
                                result_df["watchlist_eligible"].fillna(False).astype(bool),
                                "symbol",
                            ].tolist()
                            if result_df is not None
                            and not result_df.empty
                            and {"watchlist_eligible", "symbol"}.issubset(result_df.columns)
                            else []
                        )
                    }
                    # A missing/incomplete result or an interrupted scan must
                    # never turn into a mass deletion of the saved watchlist.
                    removable_symbols = evaluated_symbols - uncertain_symbols
                    removed = (set(self._cards) & removable_symbols) - eligible_symbols
                    log.info(
                        "Watchlist reconciliation: evaluated=%s eligible=%s uncertain=%s removed=%s",
                        len(evaluated_symbols), len(eligible_symbols),
                        len(uncertain_symbols), len(removed),
                    )
                    for stale_symbol in sorted(removed):
                        log.warning(
                            "Watchlist reconciliation removing %s: fully_evaluated=true, "
                            "eligible_in_current_scan=false",
                            stale_symbol,
                        )
                        self._cards.pop(stale_symbol, None)
                        self._live_prices.pop(stale_symbol, None)
                        self._live_quote_metadata.pop(stale_symbol, None)
                        self._latest_quote_payloads.pop(stale_symbol, None)
                        self._live_candles.pop(stale_symbol, None)
                        self._latest_quote_sequence.pop(stale_symbol, None)
                        self._latest_quote_timestamp.pop(stale_symbol, None)
                        self._processed_quote_timestamp.pop(stale_symbol, None)
                        self._ws_seen_symbols.discard(stale_symbol)
                        self._ws_last_log_at.pop(stale_symbol, None)
                        try:
                            self.storage.delete_watchlist_card(stale_symbol)
                        except Exception:
                            log.exception("Failed to delete stale persisted card for %s", stale_symbol)
                self._notify_state_changed_locked()
            if removed and self.lb_client.has_credentials():
                try:
                    self.lb_client.unsubscribe(sorted(removed))
                except Exception:
                    log.exception("Failed to unsubscribe reconciled stale watchlist symbols")
            # One bulk subscription request for the complete current watchlist.
            # Existing subscriptions are retained; only genuinely new symbols
            # are sent to the already-running WebSocket context.
            if self.lb_client.has_credentials():
                with self._lock:
                    watchlist_symbols = sorted(self._cards)
                self._subscribe_watchlist_symbols(
                    watchlist_symbols, context="completed watchlist bulk sync"
                )
            download_errors = sum(1 for failure in failures if failure.get("phase") == "download")
            analysis_errors = sum(1 for failure in failures if failure.get("phase") == "analysis_error")
            relative_strength_errors = sum(1 for failure in failures if failure.get("phase") == "relative_strength")
            fundamentals_warnings = sum(1 for failure in failures if failure.get("phase") == "fundamentals")
            history_warnings = sum(1 for failure in failures if failure.get("phase") == "history")
            overall_status = (
                "STOPPED" if self._stop_event.is_set()
                else "COMPLETED_WITH_ERRORS" if (download_errors or analysis_errors or relative_strength_errors)
                else "COMPLETED_WITH_WARNINGS" if (fundamentals_warnings or history_warnings)
                else "SUCCESS"
            )
            log.info(
                "STEP=FULL_SCAN STATUS=%s result_rows=%s download_errors=%s "
                "analysis_errors=%s relative_strength_errors=%s fundamentals_warnings=%s history_warnings=%s",
                overall_status,
                0 if result_df is None else len(result_df),
                download_errors, analysis_errors, relative_strength_errors, fundamentals_warnings, history_warnings,
            )
            for failure in failures:
                log.error(
                    "STEP=SCAN_DIAGNOSTIC STATUS=FAILED phase=%s symbol=%s reason=%s",
                    failure.get("phase", "unknown"),
                    failure.get("symbol", "UNKNOWN"),
                    failure.get("error", "No error detail supplied"),
                )
        except Exception as exc:
            log.exception("Scan cycle failed")
            with self._state_cv:
                self._last_error = str(exc)
                self._notify_state_changed_locked()
        finally:
            with self._state_cv:
                was_stopped = self._stop_event.is_set()
                self._running = False
                if was_stopped: self._scan_progress = {**self._scan_progress, "phase":"stopped"}
                elif self._last_error: self._scan_progress = {**self._scan_progress, "phase":"error"}
                else: self._scan_progress = {**self._scan_progress, "phase":"completed", "percent":100, "completed":self._scan_progress.get("total",0)}
                self._notify_state_changed_locked()

    def start_scan(self, symbols: list[str] | None = None, update: bool = True) -> bool:
        # Reserve the running state before starting the worker to prevent two
        # simultaneous HTTP requests from launching duplicate scans.
        with self._state_cv:
            if self._running:
                return False
            self._stop_event.clear()
            self._running = True
            self._last_error = None
            self._scan_errors = []
            self._scan_progress = {"phase": "starting", "completed": 0, "total": len(symbols or SP500_SYMBOLS) * 2, "percent": 0, "current_symbol": ""}
            self._notify_state_changed_locked()
            t = threading.Thread(
                target=self.run_once,
                kwargs={"symbols": symbols, "update": update},
                daemon=True,
            )
            try:
                t.start()
            except Exception:
                self._running = False
                self._notify_state_changed_locked()
                raise
        return True

    def resume_pending_scan(self) -> bool:
        """Resume an interrupted full-universe update after a Gunicorn worker restart."""
        symbols = SP500_SYMBOLS
        if not self.storage.has_scan_checkpoint("daily_universe_update", symbols):
            return False
        log.warning("Resuming interrupted daily data update from its SQLite checkpoint")
        return self.start_scan(symbols=symbols, update=True)

    def stop_scan(self) -> None:
        self._stop_event.set()

    def remove_card(self, symbol: str) -> None:
        clean = symbol.upper().strip()
        log.warning("Watchlist card manually deleted through API: %s", clean)
        with self._state_cv:
            self._cards.pop(clean, None)
            self._live_prices.pop(clean, None)
            self._live_quote_metadata.pop(clean, None)
            self._latest_quote_payloads.pop(clean, None)
            self._live_candles.pop(clean, None)
            self._latest_quote_sequence.pop(clean, None)
            self._latest_quote_timestamp.pop(clean, None)
            self._processed_quote_timestamp.pop(clean, None)
            self._ws_seen_symbols.discard(clean)
            self._ws_last_log_at.pop(clean, None)
            try:
                self.storage.delete_watchlist_card(clean)
            except Exception:
                log.exception("Failed to delete persisted watchlist card for %s", clean)
            self._notify_state_changed_locked()
        if self.lb_client.has_credentials():
            try:
                self.lb_client.unsubscribe([clean])
            except Exception as exc:
                log.warning("Longbridge unsubscribe error for %s: %s", clean, exc)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._market_session_label = self._detect_market_session()
            return {
                "brand": "minervini",
                "version": self._state_version,
                "running": self._running,
                "ws_connected": self._ws_connected,
                "market_session": self._market_session_label,
                "market_status": self._market_status,
                "last_error": self._last_error,
                "scan_progress": self._scan_progress,
                "scan_errors": list(self._scan_errors),
                "cards": list(self._cards.values()),
            }

    def wait_for_update(self, last_version: int, timeout: float = 15.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        with self._state_cv:
            while self._state_version <= last_version:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._state_cv.wait(timeout=remaining)
            return self.snapshot()


controller = ScanController(
    db_path=os.getenv("DB_PATH", "stock_data.db"),
    output_path=os.getenv("OUTPUT_PATH", "watchlist.json"),
)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state():
    return jsonify(controller.snapshot())


@app.route("/api/stream")
def api_stream():
    def event_stream():
        snap = controller.snapshot()
        last_ver = snap["version"]
        yield f"data: {json.dumps(snap, ensure_ascii=False, default=str)}\n\n"
        while True:
            updated = controller.wait_for_update(last_ver, timeout=15.0)
            last_ver = updated["version"]
            yield f"data: {json.dumps(updated, ensure_ascii=False, default=str)}\n\n"

    return Response(
        stream_with_context(event_stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/scan/start", methods=["POST"])
def api_start_scan():
    body = request.get_json(silent=True)
    if body is None:
        if request.data:
            return jsonify({"error": "request body must be valid JSON"}), 400
        body = {}
    if not isinstance(body, dict):
        return jsonify({"error": "request body must be a JSON object"}), 400
    symbols = body.get("symbols")
    if isinstance(symbols, str):
        symbols = [symbols]
    if symbols is not None:
        if not isinstance(symbols, list) or any(not isinstance(sym, str) for sym in symbols):
            return jsonify({"error": "symbols must be an array of ticker strings"}), 400
        normalized = [sym.upper().strip() for sym in symbols]
        if (
            not normalized
            or len(normalized) > 500
            or any(not re.fullmatch(r"\^?[A-Z0-9][A-Z0-9.-]{0,14}", sym) for sym in normalized)
        ):
            return jsonify({"error": "symbols must contain 1 to 500 valid ticker symbols"}), 400
        symbols = sorted(set(normalized))

    update = body.get("update", True)
    if not isinstance(update, bool):
        return jsonify({"error": "update must be a boolean"}), 400
    started = controller.start_scan(symbols=symbols, update=update)
    return jsonify({"started": started, "running": controller.snapshot()["running"]})


@app.route("/api/scan/stop", methods=["POST"])
def api_stop_scan():
    controller.stop_scan()
    return jsonify({"stopped": True})


@app.route("/api/cards/<symbol>", methods=["DELETE"])
def api_delete_card(symbol: str):
    controller.remove_card(symbol)
    return jsonify({"deleted": symbol.upper()})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
