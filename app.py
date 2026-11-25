"""MINERVINI web server & mobile-first dark UI (minervini_v2 structure)."""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from flask import Flask, Response, jsonify, render_template, request, stream_with_context

from data.longbridge_stream import LongbridgeRealtimeClient
from data.storage_manager import StorageManager
from data.trade_levels import build_trade_levels, evaluate_entry_range
from data.sp500_symbols import SP500_SYMBOLS
from data.telegram_notifier import TelegramNotifier
from screener import DailyScreener
from analysis.vcp import check_vcp, check_breakout
from analysis.pullback import check_pullback
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("minervini_v2_app")

BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, template_folder=str(BASE_DIR))

MARKET_STATES = ["مفتوح", "بعد الإغلاق", "تداول ليلي", "قبل الافتتاح"]


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
        try:
            # Restore cards before Flask can serve the first /api/state or SSE snapshot.
            self._cards = {
                str(card["symbol"]).upper(): card
                for card in self.storage.load_watchlist_cards()
                if card.get("symbol")
            }
            if self._cards:
                log.info("Restored %s watchlist cards from SQLite", len(self._cards))
        except Exception:
            log.exception("Failed to restore persisted watchlist cards")
            self._cards = {}
        self._live_prices: dict[str, float] = {}
        self._realtime_locks: dict[str, threading.Lock] = {}
        self._ws_connected: bool = False
        self._last_error: str | None = None
        self._scan_errors: list[dict[str, str]] = []
        self._scan_progress: dict[str, Any] = {"phase": "idle", "completed": 0, "total": 0, "percent": 0, "current_symbol": ""}

    def _notify_state_changed_locked(self) -> None:
        self._state_version += 1
        self._state_cv.notify_all()

    @staticmethod
    def _detect_market_session() -> str:
        override = os.getenv("MARKET_SESSION_LABEL", "").strip()
        if override:
            if override in MARKET_STATES:
                return override
            log.warning("Invalid MARKET_SESSION_LABEL=%r; falling back to NY session clock", override)
        now_ny = datetime.now(ZoneInfo("America/New_York"))
        if now_ny.weekday() >= 5:
            return "بعد الإغلاق"
        minutes = now_ny.hour * 60 + now_ny.minute
        if 4 * 60 <= minutes < 9 * 60 + 30:
            return "قبل الافتتاح"
        if 9 * 60 + 30 <= minutes < 16 * 60:
            return "مفتوح"
        if 16 * 60 <= minutes < 20 * 60:
            return "بعد الإغلاق"
        return "تداول ليلي"

    def _on_realtime_quote(self, quote: dict[str, Any]) -> None:
        sym = str(quote.get("symbol", "")).upper().strip()
        raw_price = quote.get("last_done")
        if not sym or raw_price is None:
            return
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            return
        if not (price > 0 and math.isfinite(price)):
            return

        with self._lock:
            symbol_lock = self._realtime_locks.setdefault(sym, threading.Lock())

        # Serialize callbacks per symbol to prevent stale calculations winning.
        with symbol_lock:
            alert_card = None
            try:
                candles = self.storage.get_candles_df(sym)
                live_df = candles.copy()
                if not live_df.empty:
                    live_df["date"] = pd.to_datetime(live_df["date"], errors="coerce")
                    today = datetime.now(ZoneInfo("America/New_York")).date()
                    today_mask = live_df["date"].dt.date == today
                    raw_volume = quote.get("volume")
                    try:
                        quote_volume = float(raw_volume) if raw_volume is not None else None
                        if quote_volume is not None and (not math.isfinite(quote_volume) or quote_volume < 0):
                            quote_volume = None
                    except (TypeError, ValueError):
                        quote_volume = None

                    if bool(today_mask.any()):
                        idx = live_df.index[today_mask][-1]
                        live_df.at[idx, "high"] = max(float(live_df.at[idx, "high"]), price)
                        live_df.at[idx, "low"] = min(float(live_df.at[idx, "low"]), price)
                        live_df.at[idx, "close"] = price
                        if quote_volume is not None:
                            live_df.at[idx, "volume"] = max(float(live_df.at[idx, "volume"]), quote_volume)
                    else:
                        live_df = pd.concat([live_df, pd.DataFrame([{
                            "date": pd.Timestamp(today),
                            "open": price,
                            "high": price,
                            "low": price,
                            "close": price,
                            "volume": quote_volume if quote_volume is not None else 0.0,
                        }])], ignore_index=True)
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
                else:
                    vcp_passed = False
                    pullback_passed = False
                    breakout_passed = False
                    pivot = None
                    vcp_info = {}

                setup_passed = bool(vcp_passed or pullback_passed)
                with self._state_cv:
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
                    active_pivot = pivot if vcp_passed else None
                    active_low = vcp_info.get("final_low") if vcp_passed else None
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

                    card.update({
                        "current_price": round(price, 4),
                        "price_source": "Longbridge WebSocket",
                        "pivot_price": plan["pivot_price"],
                        "final_low": plan["final_low"],
                        "entry_zones": plan["entry_zones"],
                        "targets": plan["targets"],
                        "shared_stop_loss": plan["shared_stop_loss"],
                        "levels_status": plan["status"],
                        "within_entry_range": plan["within_entry_range"],
                        "notes": plan["notes"],
                        "setup_passed": setup_passed,
                        "entry_ready": bool(setup_passed and market_favorable),
                        "passed_all_filters": bool(setup_passed and market_favorable),
                        "market_favorable": market_favorable,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })
                    if setup_passed and not market_favorable:
                        card["status"] = "النموذج تحقق — السوق غير مناسب"
                        card["stage"] = "المرحلة 3: تقييم حالة السوق"
                    elif setup_passed:
                        card["status"] = "إشارة مكتملة — النموذج والسوق مؤهلان"
                        card["stage"] = "المرحلة 4: إشارة جاهزة للتقييم"
                    else:
                        card["status"] = "انتظار — مراقبة VCP أو Pullback"
                        card["stage"] = "المرحلة 1: سهم مؤهل للمراقبة"

                    for item in card.get("filters", []):
                        name = item.get("name")
                        if name == "VCP Pattern":
                            item["passed"] = vcp_passed
                        elif name == "Breakout Confirmation":
                            item["passed"] = breakout_passed
                        elif name == "Pullback Check":
                            item["passed"] = pullback_passed
                        elif name == "Market Regime Favorable":
                            item["passed"] = market_favorable
                            item["pending"] = not setup_passed
                    card["smart_reading"] = self._build_smart_reading_from_card(card)
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
                    self._ws_connected = True
                    self._live_prices[sym] = price
                    card = self._cards.get(sym)
                    if card is not None:
                        card["current_price"] = round(price, 4)
                        card["price_source"] = "Longbridge WebSocket"
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

    def _build_smart_reading_from_card(self, card: dict[str, Any]) -> str:
        sym = card.get("symbol", "")
        cur = card.get("current_price")
        src = card.get("price_source", "Stored daily data")
        rs = card.get("rs_rating")
        regime = card.get("market_regime")
        lvl_status = card.get("levels_status")
        within_range = card.get("within_entry_range")
        filters = card.get("filters") or []
        failed_filters = [f.get("name", "") for f in filters if not f.get("passed", False)]

        if within_range is False:
            range_note = "السعر الحالي خارج نطاق الدخول المحدد (5%) — لا تطارد السعر."
        elif within_range is True:
            range_note = "السعر الحالي ضمن نطاق الدخول المحدد (5%)."
        else:
            range_note = "حالة نطاق الدخول غير محددة لعدم توفر نقطة ارتكاز."

        if card.get("setup_passed") and card.get("entry_ready"):
            filters_note = "اكتمل نموذج الدخول واجتازت حالة السوق شرط الملاءمة."
        elif card.get("setup_passed"):
            filters_note = "تحقق أحد نموذجي الدخول، لكن حالة السوق لا تسمح حاليًا."
        else:
            filters_note = "السهم مؤهل للمراقبة؛ ننتظر تحقق VCP أو Pullback قبل تقييم السوق."

        return (
            f"السهم {sym} (السعر الحالي: {cur} عبر {src}) — {filters_note} "
            f"حالة السوق العام: {regime}، وتصنيف القوة النسبية RS={rs}. "
            f"{range_note} حالة المستويات: {lvl_status}."
        )

    def _build_filters_detail(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        filters: list[dict[str, Any]] = []
        for cond_name, passed in (row.get("trend_conditions") or {}).items():
            filters.append({
                "name": f"Trend Template — {cond_name}",
                "passed": bool(passed),
            })
        filters.extend([
            {"name": "VCP Pattern", "passed": bool(row.get("vcp_passed", False))},
            {"name": "Breakout Confirmation", "passed": bool(row.get("breakout", False))},
            {"name": "Pullback Check", "passed": bool(row.get("pullback_passed", False))},
            {"name": "Accumulation", "passed": bool(row.get("accumulation", False))},
            {"name": "Earnings Growth", "passed": bool(row.get("earnings_passed", False))},
            {
                "name": "Market Regime Favorable",
                "passed": bool(row.get("market_favorable", False)),
                "pending": not bool(row.get("vcp_passed", False) or row.get("pullback_passed", False)),
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
        pullback_passed = bool(row.get("pullback_passed", False))
        ad_acc = bool(row.get("accumulation", False))
        earn_passed = bool(row.get("earnings_passed", False))

        watchlist_eligible = bool(tt_passed and ad_acc and earn_passed)
        if not watchlist_eligible:
            return None

        # VCP and Pullback are alternative setup paths, not cumulative gates.
        setup_passed = bool(vcp_passed or pullback_passed)
        entry_ready = bool(setup_passed and market_favorable)

        with self._lock:
            live_p = self._live_prices.get(sym)

        if live_p is not None:
            current_price = float(live_p)
            price_source = "Longbridge WebSocket"
        else:
            current_price = float(df["close"].iloc[-1])
            historical_source = str(row.get("data_source") or "Stored daily data")
            price_source = f"{historical_source} (Daily Close)"

        pivot_price = row.get("pivot_price")
        final_low = row.get("final_low")
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

        if not setup_passed:
            card_status = "انتظار — مراقبة VCP أو Pullback"
            card_stage = "المرحلة 1: سهم مؤهل للمراقبة"
        elif not market_favorable:
            card_status = "النموذج تحقق — السوق غير مناسب"
            card_stage = "المرحلة 3: تقييم حالة السوق"
        else:
            card_status = "إشارة مكتملة — النموذج والسوق مؤهلان"
            card_stage = "المرحلة 4: إشارة جاهزة للتقييم"

        card = {
            "symbol": sym,
            "current_price": round(current_price, 4),
            "price_source": price_source,
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
            "filters": self._build_filters_detail(row),
            "watchlist_eligible": True,
            "setup_passed": setup_passed,
            "market_favorable": bool(market_favorable),
            "entry_ready": entry_ready,
            "passed_all_filters": entry_ready,
            "shown_on_main_page": True,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        card["smart_reading"] = self._build_smart_reading_from_card(card)
        return card

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
                if self.lb_client.has_credentials():
                    try:
                        self.lb_client.subscribe([card["symbol"] for card in qualified])
                        with self._state_cv:
                            self._ws_connected = True
                            self._notify_state_changed_locked()
                    except Exception: log.exception("Longbridge subscribe failed after batch")
            result_df, failures, market_status = screener.run_screen(
                update=update, progress_callback=self._update_scan_progress,
                batch_callback=publish_batch, stop_callback=self._stop_event.is_set, batch_size=5,
            )
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
                    eligible_symbols = (
                        set(result_df.loc[result_df.get("watchlist_eligible", False) == True, "symbol"].astype(str))
                        if result_df is not None and not result_df.empty and "watchlist_eligible" in result_df
                        else set()
                    )
                    removed = set(self._cards) - eligible_symbols - uncertain_symbols
                    for stale_symbol in removed:
                        self._cards.pop(stale_symbol, None)
                        self._live_prices.pop(stale_symbol, None)
                        try:
                            self.storage.delete_watchlist_card(stale_symbol)
                        except Exception:
                            log.exception("Failed to delete stale persisted card for %s", stale_symbol)
                self._notify_state_changed_locked()
            download_errors = sum(1 for failure in failures if failure.get("phase") == "download")
            analysis_errors = sum(1 for failure in failures if failure.get("phase") == "analysis")
            log.info(
                "Scan finished: %s stocks passed Trend Template, %s download errors, %s analysis errors",
                0 if result_df is None else len(result_df),
                download_errors,
                analysis_errors,
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
        with self._state_cv:
            self._cards.pop(clean, None)
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
        yield f"data: {json.dumps(snap, ensure_ascii=False)}\n\n"
        while True:
            updated = controller.wait_for_update(last_ver, timeout=15.0)
            last_ver = updated["version"]
            yield f"data: {json.dumps(updated, ensure_ascii=False)}\n\n"

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
    body = request.get_json(silent=True) or {}
    symbols = body.get("symbols")
    if isinstance(symbols, str):
        symbols = [symbols]
    if symbols is not None and not isinstance(symbols, list):
        return jsonify({"error": "symbols must be an array of ticker symbols"}), 400
    if isinstance(symbols, list):
        symbols = [str(sym).upper().strip() for sym in symbols if str(sym).strip()]
        if not symbols:
            symbols = None
    update = bool(body.get("update", True))
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
