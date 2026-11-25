"""Longbridge realtime price & WebSocket adapter for MINERVINI."""
from __future__ import annotations

import os
import math
import logging
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
        self._ctx: Any = None
        self._subscribed_symbols: list[str] = []
        self._sub_type_quote: Any = None

    def has_credentials(self) -> bool:
        return bool(self.app_key and self.app_secret and self.access_token)

    def _normalize_symbol(self, symbol: str) -> str:
        s = str(symbol).upper().strip()
        suffix = f".{self.default_market}"
        return s if s.endswith(suffix) else f"{s}{suffix}"

    def _denormalize_symbol(self, lb_symbol: str) -> str:
        s = str(lb_symbol).upper().strip()
        suffix = f".{self.default_market}"
        return s[:-len(suffix)] if s.endswith(suffix) else s

    def connect(self) -> None:
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
            if self.on_quote is None:
                return
            last_done = _valid_price(_value(event, "last_done"))
            if last_done is None:
                return
            payload = {
                "symbol": self._denormalize_symbol(symbol),
                "raw_symbol": symbol,
                "last_done": last_done,
                "timestamp": _value(event, "timestamp"),
                "volume": _value(event, "volume"),
                "turnover": _value(event, "turnover"),
                "trade_session": _value(event, "trade_session"),
                "trade_status": _value(event, "trade_status"),
                "source": "longbridge_websocket",
            }
            self.on_quote(payload)

        ctx.set_on_quote(_handle_quote)
        self._ctx = ctx
        self._sub_type_quote = SubType.Quote
        self._subscribed_symbols = []

    def ensure_subscriptions(self, symbols: list[str]) -> list[str]:
        """Verify server-side subscriptions and restore missing quote subscriptions."""
        desired = sorted({
            self._normalize_symbol(s) for s in symbols if s and str(s).strip()
        })
        if not desired:
            return []
        if self._ctx is None:
            self.connect()

        active: set[str] = set()
        try:
            for item in self._ctx.subscriptions():
                symbol = self._normalize_symbol(_value(item, "symbol", ""))
                sub_types = _value(item, "sub_types", []) or []
                if self._sub_type_quote in sub_types:
                    active.add(symbol)
        except Exception:
            # A failed subscription query indicates that this context can no
            # longer be trusted. Recreate it, then rebuild the full desired set.
            log.exception("Longbridge subscription verification failed; reconnecting QuoteContext")
            self.close()
            self.connect()
            active = set()

        missing = [symbol for symbol in desired if symbol not in active]
        if missing:
            self._ctx.subscribe(missing, [self._sub_type_quote])
            log.warning("Longbridge restored %s missing quote subscriptions", len(missing))

        self._subscribed_symbols = sorted(active | set(desired))
        return [self._denormalize_symbol(symbol) for symbol in missing]

    def subscribe(self, symbols: list[str]) -> list[str]:
        # Idempotent and server-verified: repeated eligible batches do not
        # duplicate subscriptions, while missing subscriptions are restored.
        return self.ensure_subscriptions(symbols)

    def unsubscribe(self, symbols: list[str]) -> None:
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
        candidates: list[tuple[int, float, Any, str]] = []
        regular_price = _valid_price(_value(quote, "last_done"))
        regular_ts = _value(quote, "timestamp")
        regular_order = _timestamp_order(regular_ts)
        if regular_price is not None:
            candidates.append((regular_order, regular_price, regular_ts, "regular"))

        for field, label in (
            ("pre_market_quote", "pre-market"),
            ("post_market_quote", "post-market"),
            ("over_night_quote", "overnight"),
            ("overnight_quote", "overnight"),
        ):
            session_quote = _value(quote, field)
            if session_quote is None:
                continue
            price = _valid_price(_value(session_quote, "last_done"))
            if price is None:
                continue
            timestamp = _value(session_quote, "timestamp")
            order = _timestamp_order(timestamp)
            candidates.append((order, price, timestamp, label))

        if not candidates:
            return None, None, "unavailable"
        _, price, timestamp, session = max(candidates, key=lambda item: item[0])
        return price, timestamp, session

    def get_realtime_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        cleaned = sorted({
            self._normalize_symbol(s) for s in symbols if s and str(s).strip()
        })
        if not cleaned:
            return []
        if self._ctx is None:
            self.connect()
        response = self._ctx.quote(cleaned)
        items: list[dict[str, Any]] = []
        for quote in response:
            price, timestamp, session = self._latest_session_quote(quote)
            items.append({
                "symbol": self._denormalize_symbol(_value(quote, "symbol", "")),
                "last_done": price,
                "timestamp": timestamp,
                "trade_session": session,
                "prev_close": _value(quote, "prev_close"),
                "source": "longbridge_quote_api",
            })
        return items

    def close(self) -> None:
        ctx = self._ctx
        self._ctx = None
        self._subscribed_symbols = []
        self._sub_type_quote = None
        if ctx is not None and hasattr(ctx, "close"):
            try:
                ctx.close()
            except Exception:
                pass
