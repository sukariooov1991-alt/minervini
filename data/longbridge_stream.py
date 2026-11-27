"""Longbridge realtime price & WebSocket adapter for MINERVINI."""
from __future__ import annotations

import os
import math
import logging
import threading
import time
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
        self._sub_types: list[Any] = []
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
                volume_value = _value(event, "volume")
                current_volume_value = _value(event, "current_volume")
                has_volume_update = (
                    (volume_value is not None and volume_value != 0)
                    or (current_volume_value is not None and current_volume_value != 0)
                )
                if last_done is None and not has_volume_update:
                    # Ignore messages with neither a valid price nor a volume update.
                    return

                log.info(
                    "STEP=WEBSOCKET_QUOTE_PUSH STATUS=RECEIVED symbol=%s last_done=%s timestamp=%s session=%s sequence=%s",
                    self._denormalize_symbol(symbol), last_done,
                    _value(event, "timestamp"), _session_label(_value(event, "trade_session")),
                    _value(event, "sequence"),
                )
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
                    "trade_session": _session_label(_value(event, "trade_session")),
                    "trade_status": _value(event, "trade_status"),
                    "tag": _value(event, "tag"),
                    "source": "longbridge_websocket",
                }
                try:
                    self.on_quote(payload)
                except Exception:
                    log.exception(
                        "STEP=WEBSOCKET_QUOTE_CALLBACK STATUS=FAILED symbol=%s price=%s",
                        payload["symbol"], payload["last_done"],
                    )

            def _handle_trades(symbol: str, event: Any) -> None:
                # Trade pushes carry each executed trade, including overnight.
                # Quote pushes alone are incremental quote-field changes and
                # are not a guaranteed per-trade tape.
                self._last_message_monotonic = time.monotonic()
                if self.on_quote is None:
                    return
                trades = _value(event, "trades")
                if trades is None:
                    trades = _value(event, "trade", [])
                try:
                    trade_items = list(trades or [])
                except TypeError:
                    trade_items = []
                log.info(
                    "STEP=WEBSOCKET_TRADE_PUSH STATUS=RECEIVED symbol=%s trades=%s",
                    self._denormalize_symbol(symbol), len(trade_items),
                )
                for trade in trade_items:
                    price = _valid_price(_value(trade, "price"))
                    if price is None:
                        continue
                    trade_volume = _value(trade, "volume")
                    payload = {
                        "symbol": self._denormalize_symbol(symbol),
                        "raw_symbol": symbol,
                        "last_done": price,
                        # Quote and trade sequence spaces must not be compared.
                        # Timestamp ordering is shared by both push streams.
                        "sequence": None,
                        "timestamp": _value(trade, "timestamp"),
                        "volume": None,
                        "turnover": None,
                        "current_volume": trade_volume,
                        "current_turnover": None,
                        "trade_session": _session_label(
                            _value(trade, "trade_session")
                            if _value(trade, "trade_session") is not None
                            else _value(event, "trade_session")
                        ),
                        "trade_status": None,
                        "tag": 0,
                        "source": "longbridge_trade_websocket",
                    }
                    try:
                        self.on_quote(payload)
                    except Exception:
                        log.exception(
                            "STEP=WEBSOCKET_TRADE_CALLBACK STATUS=FAILED symbol=%s price=%s",
                            payload["symbol"], price,
                        )

            ctx.set_on_quote(_handle_quote)
            ctx.set_on_trades(_handle_trades)
            self._ctx = ctx
            self._sub_types = [SubType.Quote, SubType.Trade]
            self._subscribed_symbols = []
            log.info(
                "STEP=WEBSOCKET_CONNECT STATUS=SUCCESS overnight_enabled=%s push_types=Quote,Trade",
                enable_overnight,
            )

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
            # The SDK exposes the provider's active subscription list. Reconcile
            # against it before deciding that a symbol is already subscribed;
            # a successful local subscribe() call is not proof that the server
            # activated both Quote and Trade pushes.
            can_verify = callable(getattr(self._ctx, "subscriptions", None))
            actual_map: dict[str, set[str]] = {}
            if can_verify:
                try:
                    for item in self._ctx.subscriptions() or []:
                        actual_symbol = str(_value(item, "symbol", "")).upper()
                        raw_types = _value(item, "sub_types", []) or []
                        actual_map[actual_symbol] = {
                            str(getattr(value, "name", value)).split(".")[-1]
                            for value in raw_types
                        }
                    active = {
                        symbol for symbol in active
                        if {"Quote", "Trade"}.issubset(actual_map.get(symbol, set()))
                    }
                    active.update(
                        symbol for symbol, types in actual_map.items()
                        if {"Quote", "Trade"}.issubset(types)
                    )
                    self._subscribed_symbols = sorted(active)
                except Exception:
                    log.exception("STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=FAILED")
                    # Do not discard a previously confirmed local state because
                    # a transient verification request failed.
                    can_verify = False

            missing = [symbol for symbol in desired if symbol not in active]
            if not missing:
                return []

            # Submit every missing symbol together through the one persistent
            # QuoteContext. Do not mark it active until provider state confirms
            # both requested push types.
            try:
                self._ctx.subscribe(missing, self._sub_types)
            except Exception:
                log.exception(
                    "STEP=WEBSOCKET_SUBSCRIBE STATUS=FAILED requested=%s symbols=%s",
                    len(missing), ",".join(self._denormalize_symbol(s) for s in missing),
                )
                raise
            log.info(
                "STEP=WEBSOCKET_SUBSCRIBE STATUS=REQUEST_SENT requested=%s total_local=%s push_types=Quote,Trade symbols=%s",
                len(missing), len(active | set(missing)),
                ",".join(self._denormalize_symbol(s) for s in missing),
            )

            if can_verify:
                try:
                    actual_map = {}
                    for item in self._ctx.subscriptions() or []:
                        actual_symbol = str(_value(item, "symbol", "")).upper()
                        raw_types = _value(item, "sub_types", []) or []
                        actual_map[actual_symbol] = {
                            str(getattr(value, "name", value)).split(".")[-1]
                            for value in raw_types
                        }
                    confirmed = {
                        symbol for symbol, types in actual_map.items()
                        if {"Quote", "Trade"}.issubset(types)
                    }
                    unconfirmed = [symbol for symbol in missing if symbol not in confirmed]
                    self._subscribed_symbols = sorted(active | (confirmed & set(desired)))
                    if unconfirmed:
                        log.error(
                            "STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=INCOMPLETE retry_required=%s actual=%s",
                            ",".join(unconfirmed), sorted((s, sorted(v)) for s, v in actual_map.items()),
                        )
                    else:
                        log.info(
                            "STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=CONFIRMED symbols=%s push_types=Quote,Trade",
                            len(missing),
                        )
                    # Return unconfirmed symbols too: caller refreshes the
                    # watchdog baseline, and the next watchdog pass retries them.
                    return [self._denormalize_symbol(s) for s in missing]
                except Exception:
                    log.exception("STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=FAILED")
                    # Verification failed: don't claim the new subscriptions.
                    self._subscribed_symbols = sorted(active)
                    return [self._denormalize_symbol(s) for s in missing]

            # Compatibility for older SDK builds without subscriptions().
            log.warning("STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=UNAVAILABLE; using SDK subscribe acceptance")
            self._subscribed_symbols = sorted(active | set(missing))
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
            self._ctx.unsubscribe(remove, self._sub_types)
            self._subscribed_symbols = sorted(active - set(remove))

    def reconnect(self, symbols: list[str]) -> list[str]:
        """Recreate the QuoteContext and restore the complete active subscription set."""
        self.close()
        return self.ensure_subscriptions(symbols)

    def close(self) -> None:
        with self._context_lock:
            ctx = self._ctx
            self._ctx = None
            self._subscribed_symbols = []
            self._sub_types = []
            self._last_message_monotonic = None
            if ctx is not None and hasattr(ctx, "close"):
                try:
                    ctx.close()
                except Exception:
                    pass
