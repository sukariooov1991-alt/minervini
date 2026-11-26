"""Longbridge realtime price & WebSocket adapter for MINERVINI."""
from __future__ import annotations

import os
import math
import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable

log = logging.getLogger(__name__)


def _value(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _valid_price(value: Any) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if math.isfinite(price) and price > 0 else None


def _timestamp_order(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, datetime):
        try:
            return value.timestamp()
        except (OverflowError, OSError, ValueError):
            return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError, OverflowError, OSError):
            return 0.0


def _session_label(value: Any) -> str | None:
    if value is None:
        return None
    try:
        code = int(value)
    except (TypeError, ValueError):
        code = None
    labels = {0: "regular", 1: "pre-market", 2: "post-market", 3: "overnight"}
    if code in labels:
        return labels[code]
    text = str(getattr(value, "name", value)).lower()
    if "pre" in text:
        return "pre-market"
    if "post" in text:
        return "post-market"
    if "night" in text or "overnight" in text:
        return "overnight"
    if "normal" in text or "trading" in text:
        return "regular"
    return text


class LongbridgeRealtimeClient:
    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        access_token: str | None = None,
        on_quote: Callable[[dict[str, Any]], None] | None = None,
        default_market: str = "US",
    ):
        self.app_key = (app_key or os.getenv("LONGBRIDGE_APP_KEY", "")).strip()
        self.app_secret = (app_secret or os.getenv("LONGBRIDGE_APP_SECRET", "")).strip()
        self.access_token = (access_token or os.getenv("LONGBRIDGE_ACCESS_TOKEN", "")).strip()
        self.default_market = (default_market or os.getenv("LONGBRIDGE_MARKET", "US")).upper().strip()
        self.on_quote = on_quote
        self._context_lock = threading.RLock()
        self._ctx: Any = None
        self._subscribed_symbols: list[str] = []
        self._sub_type_quote: Any = None
        self._last_message_monotonic: float | None = None

    def has_credentials(self) -> bool:
        return bool(self.app_key and self.app_secret and self.access_token)

    @property
    def last_message_monotonic(self) -> float | None:
        return self._last_message_monotonic

    def _normalize_symbol(self, symbol: str) -> str:
        s = str(symbol).upper().strip()
        suffix = f".{self.default_market}"
        return s if s.endswith(suffix) else f"{s}{suffix}"

    def _denormalize_symbol(self, lb_symbol: str) -> str:
        s = str(lb_symbol).upper().strip()
        suffix = f".{self.default_market}"
        return s[:-len(suffix)] if s.endswith(suffix) else s

    def connect(self) -> None:
        with self._context_lock:
            if self._ctx is not None:
                return
            if not self.has_credentials():
                raise RuntimeError(
                    "Missing Longbridge credentials (LONGBRIDGE_APP_KEY, LONGBRIDGE_APP_SECRET, LONGBRIDGE_ACCESS_TOKEN)."
                )
            try:
                from longbridge.openapi import Config, QuoteContext, SubType  # type: ignore
            except ImportError as exc:
                raise RuntimeError("Longbridge SDK is not installed (pip install longbridge).") from exc

            enable_overnight = os.getenv("LONGBRIDGE_ENABLE_OVERNIGHT", "true").strip().lower() in {
                "1", "true", "yes", "on",
            }
            config = Config.from_apikey(
                self.app_key,
                self.app_secret,
                self.access_token,
                enable_overnight=enable_overnight,
            )
            ctx = QuoteContext(config)

            def _handle_quote(symbol: str, event: Any) -> None:
                # Count every received PushQuote as stream activity, including
                # volume-only updates that are not valid price ticks.
                self._last_message_monotonic = time.monotonic()
                if self.on_quote is None:
                    return

                # PushQuote is an incremental WebSocket message, not the full
                # SecurityQuote snapshot. Its session is identified by
                # trade_session; pre/post/overnight nested quote objects belong
                # to the pull quote response and are not part of PushQuote.
                last_done = _valid_price(_value(event, "last_done"))
                if last_done is None:
                    # A push that changes only volume/turnover is not a new
                    # price tick; do not fabricate a price from missing fields.
                    return

                payload = {
                    "symbol": self._denormalize_symbol(symbol),
                    "raw_symbol": symbol,
                    "last_done": last_done,
                    "sequence": _value(event, "sequence"),
                    "timestamp": _value(event, "timestamp"),
                    "volume": _value(event, "volume"),
                    "turnover": _value(event, "turnover"),
                    "current_volume": _value(event, "current_volume"),
                    "current_turnover": _value(event, "current_turnover"),
                    "trade_session": _session_label(_value(event, "trade_session")) or "regular",
                    "trade_status": _value(event, "trade_status"),
                    "tag": _value(event, "tag"),
                    "source": "longbridge_websocket",
                }
                self.on_quote(payload)

            ctx.set_on_quote(_handle_quote)
            self._ctx = ctx
            self._sub_type_quote = SubType.Quote
            self._subscribed_symbols = []

    def ensure_subscriptions(self, symbols: list[str]) -> list[str]:
        """Idempotently subscribe new symbols through the single persistent context.

        The local set is authoritative for this QuoteContext lifetime. Calling
        QuoteContext.subscriptions() for every scan batch caused repeated control
        requests and fragmented subscriptions into one-symbol operations.
        """
        with self._context_lock:
            desired = sorted({
                self._normalize_symbol(s) for s in symbols if s and str(s).strip()
            })
            if not desired:
                return []
            if self._ctx is None:
                self.connect()

            active = set(self._subscribed_symbols)
            missing = [symbol for symbol in desired if symbol not in active]
            if not missing:
                return []

            # Submit all new symbols in one SDK subscribe call. Only record them
            # after the SDK accepts the request, so the watchdog can retry errors.
            self._ctx.subscribe(missing, [self._sub_type_quote])
            self._subscribed_symbols = sorted(active | set(missing))
            log.info(
                "Longbridge WebSocket bulk subscribe accepted: new=%s total=%s",
                len(missing), len(self._subscribed_symbols),
            )
            return [self._denormalize_symbol(symbol) for symbol in missing]

    def subscribe(self, symbols: list[str]) -> list[str]:
        return self.ensure_subscriptions(symbols)

    def unsubscribe(self, symbols: list[str]) -> None:
        with self._context_lock:
            cleaned = sorted({
                self._normalize_symbol(s) for s in symbols if s and str(s).strip()
            })
            if not cleaned or self._ctx is None:
                return
            active = set(self._subscribed_symbols)
            remove = [symbol for symbol in cleaned if symbol in active]
            if not remove:
                return
            self._ctx.unsubscribe(remove, [self._sub_type_quote])
            self._subscribed_symbols = sorted(active - set(remove))

    @staticmethod
    def _latest_session_quote(quote: Any) -> tuple[float | None, Any, str]:
        # Longbridge returns separate US pre-market, post-market and overnight
        # quotes. last_done alone is the regular-session last trade and can be
        # stale while an extended session is actively trading.
        candidates: list[tuple[float, int, float, Any, str]] = []
        regular_price = _valid_price(_value(quote, "last_done"))
        regular_ts = _value(quote, "timestamp")
        regular_order = _timestamp_order(regular_ts)
        if regular_price is not None:
            candidates.append((regular_order, 0, regular_price, regular_ts, "regular"))

        for field, label, priority in (
            ("pre_market_quote", "pre-market", 1),
            ("post_market_quote", "post-market", 2),
            ("over_night_quote", "overnight", 3),
            ("overnight_quote", "overnight", 3),
        ):
            session_quote = _value(quote, field)
            if session_quote is None:
                continue
            price = _valid_price(_value(session_quote, "last_done"))
            if price is None:
                continue
            timestamp = _value(session_quote, "timestamp")
            order = _timestamp_order(timestamp)
            candidates.append((order, priority, price, timestamp, label))

        if not candidates:
            return None, None, "unavailable"
        _, _, price, timestamp, session = max(candidates, key=lambda item: (item[0], item[1]))
        return price, timestamp, session

    def reconnect(self, symbols: list[str]) -> list[str]:
        """Recreate the QuoteContext and restore the complete active subscription set."""
        self.close()
        return self.ensure_subscriptions(symbols)

    def close(self) -> None:
        with self._context_lock:
            ctx = self._ctx
            self._ctx = None
            self._subscribed_symbols = []
            self._sub_type_quote = None
            self._last_message_monotonic = None
            if ctx is not None and hasattr(ctx, "close"):
                try:
                    ctx.close()
                except Exception:
                    pass
