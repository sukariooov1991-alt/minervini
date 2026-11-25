"""Longbridge realtime price & WebSocket adapter for MINERVINI.

Responsibility:
- Longbridge realtime prices and market quotes
- WebSocket connection and subscriptions

Daily historical candles use the Yahoo Finance -> Stooq -> Nasdaq -> Longbridge
fallback chain (with optional Alpha Vantage last). Earnings data and news continue
to use Yahoo Finance.
"""
from __future__ import annotations

import os
import math
from typing import Any, Callable


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

    def has_credentials(self) -> bool:
        return bool(self.app_key and self.app_secret and self.access_token)

    def _normalize_symbol(self, symbol: str) -> str:
        s = symbol.upper().strip()
        suffix = f".{self.default_market}"
        if s.endswith(suffix):
            return s
        return f"{s}{suffix}"

    def _denormalize_symbol(self, lb_symbol: str) -> str:
        s = lb_symbol.upper().strip()
        suffix = f".{self.default_market}"
        if s.endswith(suffix):
            return s[: -len(suffix)]
        return s

    def connect(self) -> None:
        if not self.has_credentials():
            raise RuntimeError(
                "Missing Longbridge credentials (LONGBRIDGE_APP_KEY, LONGBRIDGE_APP_SECRET, LONGBRIDGE_ACCESS_TOKEN)."
            )
        try:
            from longbridge.openapi import Config, QuoteContext, SubType  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "Longbridge SDK is not installed (pip install longbridge)."
            ) from exc

        config = Config(
            app_key=self.app_key,
            app_secret=self.app_secret,
            access_token=self.access_token,
        )
        ctx = QuoteContext(config)

        def _handle_quote(symbol: str, event: Any) -> None:
            if self.on_quote is None:
                return
            last_done = getattr(event, "last_done", None)
            try:
                last_done = float(last_done) if last_done is not None else None
            except (TypeError, ValueError):
                return
            if last_done is not None and (not math.isfinite(last_done) or last_done <= 0):
                return
            payload = {
                "symbol": self._denormalize_symbol(symbol),
                "raw_symbol": symbol,
                "last_done": last_done,
                "timestamp": str(getattr(event, "timestamp", "")),
                "volume": getattr(event, "volume", None),
                "turnover": getattr(event, "turnover", None),
                "source": "longbridge_websocket",
            }
            self.on_quote(payload)

        ctx.set_on_quote(_handle_quote)
        self._ctx = ctx
        self._sub_type_quote = SubType.Quote

    def subscribe(self, symbols: list[str]) -> None:
        cleaned = [self._normalize_symbol(s) for s in symbols if s and s.strip()]
        if not cleaned:
            return
        if self._ctx is None:
            self.connect()
        self._ctx.subscribe(cleaned, [self._sub_type_quote])
        self._subscribed_symbols = sorted(set(self._subscribed_symbols) | set(cleaned))

    def unsubscribe(self, symbols: list[str]) -> None:
        cleaned = [self._normalize_symbol(s) for s in symbols if s and s.strip()]
        if not cleaned or self._ctx is None:
            return
        self._ctx.unsubscribe(cleaned, [self._sub_type_quote])
        self._subscribed_symbols = [s for s in self._subscribed_symbols if s not in set(cleaned)]

    def get_realtime_quotes(self, symbols: list[str]) -> list[dict[str, Any]]:
        cleaned = [self._normalize_symbol(s) for s in symbols if s and s.strip()]
        if not cleaned:
            return []
        if self._ctx is None:
            self.connect()
        resp = self._ctx.quote(cleaned)
        items: list[dict[str, Any]] = []
        for q in resp:
            last_done = getattr(q, "last_done", None)
            prev_close = getattr(q, "prev_close", None)
            items.append({
                "symbol": self._denormalize_symbol(getattr(q, "symbol", "")),
                "last_done": last_done,
                "prev_close": float(prev_close) if prev_close is not None else None,
                "timestamp": str(getattr(q, "timestamp", "")),
                "source": "longbridge",
            })
        return items

    def close(self) -> None:
        ctx = self._ctx
        self._ctx = None
        if ctx is not None and hasattr(ctx, "close"):
            try:
                ctx.close()
            except Exception:
                pass
