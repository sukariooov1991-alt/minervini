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
        self._symbol_last_message_monotonic: dict[str, float] = {}
        self._activity_lock = threading.Lock()

    def has_credentials(self) -> bool:
        return bool(self.app_key and self.app_secret and self.access_token)

    @property
    def last_message_monotonic(self) -> float | None:
        return self._last_message_monotonic

    @property
    def symbol_last_message_monotonic(self) -> dict[str, float]:
        """Return a snapshot of the last raw WebSocket push time per symbol."""
        with self._activity_lock:
            return dict(self._symbol_last_message_monotonic)

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
                received_mono = time.monotonic()
                self._last_message_monotonic = received_mono
                with self._activity_lock:
                    self._symbol_last_message_monotonic[self._denormalize_symbol(symbol)] = received_mono
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
                # A PushTrades event may bundle several executions. Forward
                # its newest valid execution into the same ordered tick pipeline.
                received_mono = time.monotonic()
                self._last_message_monotonic = received_mono
                display_symbol = self._denormalize_symbol(symbol)
                with self._activity_lock:
                    self._symbol_last_message_monotonic[display_symbol] = received_mono
                if self.on_quote is None:
                    return

                raw_trades = _value(event, "trades", None)
                if raw_trades is None:
                    raw_trades = _value(event, "trade", [])
                try:
                    trades = list(raw_trades or [])
                except TypeError:
                    trades = []
                valid_trades = []
                for item in trades:
                    price = _valid_price(_value(item, "price"))
                    if price is None:
                        continue
                    try:
                        order = float(_value(item, "timestamp") or 0)
                        volume = int(_value(item, "volume") or 0)
                    except (TypeError, ValueError, OverflowError):
                        continue
                    if not math.isfinite(order) or order <= 0 or volume <= 0:
                        continue
                    valid_trades.append((order, item, price))
                if not valid_trades:
                    return

                _, latest_trade, price = max(valid_trades, key=lambda item: item[0])
                latest_session = _session_label(_value(latest_trade, "trade_session"))
                # The first subscription push may contain recent executions
                # from several sessions. Count only records belonging to the
                # newest execution's session, never mix pre/regular/post ticks.
                session_trade_count = sum(
                    1 for _, item, _ in valid_trades
                    if _session_label(_value(item, "trade_session")) == latest_session
                )
                payload = {
                    "symbol": display_symbol,
                    "raw_symbol": symbol,
                    "last_done": price,
                    "sequence": _value(event, "sequence"),
                    "timestamp": _value(latest_trade, "timestamp"),
                    "volume": None,
                    "current_volume": None,
                    "trade_session": latest_session,
                    "trade_status": None,
                    "tag": None,
                    "source": "longbridge_trade_websocket",
                    "trade_count_delta": session_trade_count,
                    "trade_count_sequence": _value(event, "sequence"),
                }
                log.info(
                    "STEP=WEBSOCKET_TRADE_PUSH STATUS=RECEIVED symbol=%s price=%s timestamp=%s session=%s sequence=%s",
                    display_symbol, price, payload["timestamp"],
                    payload["trade_session"], payload["sequence"],
                )
                try:
                    self.on_quote(payload)
                except Exception:
                    log.exception(
                        "STEP=WEBSOCKET_TRADE_CALLBACK STATUS=FAILED symbol=%s price=%s",
                        display_symbol, price,
                    )

            trade_sub_type = getattr(SubType, "Trade", None)
            set_on_trades = getattr(ctx, "set_on_trades", None)
            if trade_sub_type is None or not callable(set_on_trades):
                raise RuntimeError(
                    "Installed Longbridge SDK does not expose Trade push subscriptions."
                )
            ctx.set_on_quote(_handle_quote)
            ctx.set_on_trades(_handle_trades)
            self._ctx = ctx
            self._sub_types = [SubType.Quote, trade_sub_type]
            self._subscribed_symbols = []
            log.info(
                "STEP=WEBSOCKET_CONNECT STATUS=SUCCESS overnight_enabled=%s push_types=Quote,Trade",
                enable_overnight,
            )

    def ensure_subscriptions(self, symbols: list[str]) -> list[str]:
        """Idempotently subscribe new symbols through the single persistent context.

        Reconcile the cached local set with provider-confirmed Quote
        subscriptions once per bulk sync. Never treat a local subscribe request
        as active until the provider confirms the Quote push type when verification
        is available.
        """
        with self._context_lock:
            desired = sorted({
                self._normalize_symbol(s) for s in symbols if s and str(s).strip()
            })
            if not desired:
                return []
            if self._ctx is None:
                self.connect()

            required_types = {
                str(getattr(value, "name", value)).split(".")[-1]
                for value in self._sub_types
            }
            if not required_types:
                raise RuntimeError("Longbridge subscription types are not configured.")
            active = set(self._subscribed_symbols)
            # The SDK exposes the provider's active subscription list. Reconcile
            # against it before deciding that a symbol is already subscribed;
            # a successful local subscribe() call is not proof that the server
            # activated the Quote push.
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
                        if required_types.issubset(actual_map.get(symbol, set()))
                    }
                    active.update(
                        symbol for symbol, types in actual_map.items()
                        if required_types.issubset(types)
                    )
                    self._subscribed_symbols = sorted(active)
                except Exception:
                    log.exception("STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=FAILED")
                    # The verification API exists but is temporarily unavailable.
                    # Keep the previously confirmed local state, retry the
                    # subscription request, and still attempt post-submit
                    # verification instead of silently trusting local acceptance.

            missing = [symbol for symbol in desired if symbol not in active]
            if not missing:
                return []

            # Submit every missing symbol together through the one persistent
            # QuoteContext. Do not mark it active until provider state confirms
            # the requested Quote push type.
            try:
                self._ctx.subscribe(missing, self._sub_types)
            except Exception:
                log.exception(
                    "STEP=WEBSOCKET_SUBSCRIBE STATUS=FAILED requested=%s push_types=%s symbols=%s",
                    len(missing), ",".join(sorted(required_types)),
                    ",".join(self._denormalize_symbol(s) for s in missing),
                )
                raise
            log.info(
                "STEP=WEBSOCKET_SUBSCRIBE STATUS=REQUEST_SENT requested=%s total_local=%s push_types=%s symbols=%s",
                len(missing), len(active | set(missing)),
                ",".join(sorted(required_types)),
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
                        if required_types.issubset(types)
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
                            "STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=CONFIRMED symbols=%s push_types=%s",
                            len(missing), ",".join(sorted(required_types)),
                        )
                    # Return unconfirmed symbols too: caller refreshes the
                    # watchdog baseline, and the next watchdog pass retries them.
                    return [self._denormalize_symbol(s) for s in missing]
                except Exception:
                    log.exception("STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=FAILED")
                    # Verification failed: don't claim the new subscriptions.
                    self._subscribed_symbols = sorted(active)
                    return [self._denormalize_symbol(s) for s in missing]

            # Compatibility is allowed only when the installed SDK does not
            # expose subscriptions() at all. A transient provider verification
            # error must never be treated as proof of an active subscription.
            log.warning("STEP=WEBSOCKET_SUBSCRIBE_VERIFY STATUS=UNAVAILABLE; using SDK subscribe acceptance")
            self._subscribed_symbols = sorted(active | set(missing))
            return [self._denormalize_symbol(symbol) for symbol in missing]

    def subscribe(self, symbols: list[str]) -> list[str]:
        return self.ensure_subscriptions(symbols)

    def resubscribe(self, symbols: list[str]) -> dict[str, bool]:
        """Force a provider unsubscribe/subscribe cycle and verify Quote."""
        with self._context_lock:
            targets = sorted({
                self._normalize_symbol(s) for s in symbols if s and str(s).strip()
            })
            if not targets:
                return {}
            if self._ctx is None:
                self.connect()
            required_types = {
                str(getattr(value, "name", value)).split(".")[-1]
                for value in self._sub_types
            }
            results: dict[str, bool] = {}
            for symbol in targets:
                display = self._denormalize_symbol(symbol)
                try:
                    self._ctx.unsubscribe([symbol], self._sub_types)
                    log.warning("STEP=WEBSOCKET_RESUBSCRIBE STATUS=UNSUBSCRIBE_SENT symbol=%s", display)
                except Exception:
                    log.exception("STEP=WEBSOCKET_RESUBSCRIBE STATUS=UNSUBSCRIBE_FAILED symbol=%s", display)
                try:
                    self._ctx.subscribe([symbol], self._sub_types)
                    log.warning("STEP=WEBSOCKET_RESUBSCRIBE STATUS=SUBSCRIBE_SENT symbol=%s push_types=Quote,Trade", display)
                    verified = False
                    subscriptions = getattr(self._ctx, "subscriptions", None)
                    if callable(subscriptions):
                        actual = {}
                        for item in subscriptions() or []:
                            actual_symbol = str(_value(item, "symbol", "")).upper()
                            raw_types = _value(item, "sub_types", []) or []
                            actual[actual_symbol] = {
                                str(getattr(value, "name", value)).split(".")[-1]
                                for value in raw_types
                            }
                        verified = required_types.issubset(actual.get(symbol, set()))
                    else:
                        log.error("STEP=WEBSOCKET_RESUBSCRIBE STATUS=UNVERIFIABLE symbol=%s reason=subscriptions_api_missing", display)
                    results[display] = verified
                    if verified:
                        self._subscribed_symbols = sorted(set(self._subscribed_symbols) | {symbol})
                        log.info("STEP=WEBSOCKET_RESUBSCRIBE STATUS=CONFIRMED symbol=%s push_types=%s", display, ",".join(sorted(required_types)))
                    else:
                        self._subscribed_symbols = [item for item in self._subscribed_symbols if item != symbol]
                        log.error("STEP=WEBSOCKET_RESUBSCRIBE STATUS=NOT_CONFIRMED symbol=%s expected=%s", display, ",".join(sorted(required_types)))
                except Exception:
                    results[display] = False
                    log.exception("STEP=WEBSOCKET_RESUBSCRIBE STATUS=FAILED symbol=%s", display)
            return results

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
            with self._activity_lock:
                self._symbol_last_message_monotonic.clear()
            if ctx is not None and hasattr(ctx, "close"):
                try:
                    ctx.close()
                except Exception:
                    pass
