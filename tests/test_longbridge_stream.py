import unittest
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from data.longbridge_stream import LongbridgeRealtimeClient


class FakeQuoteContext:
    def __init__(self, subscriptions=None):
        self.active = list(subscriptions or [])
        self.subscribe_calls = []

    def subscriptions(self):
        return list(self.active)

    def subscribe(self, symbols, sub_types):
        self.subscribe_calls.append((list(symbols), list(sub_types)))
        for symbol in symbols:
            self.active.append(SimpleNamespace(symbol=symbol, sub_types=list(sub_types)))


class LongbridgeQuoteTests(unittest.TestCase):
    def test_websocket_push_preserves_sequence_and_session(self):
        captured = {}

        class FakeConfig:
            @staticmethod
            def from_apikey(*args, **kwargs):
                return object()

        class FakeSubType:
            Quote = object()
            Trade = object()
            Trade = object()

        class FakeContext:
            def __init__(self, config):
                pass

            def set_on_quote(self, callback):
                captured["callback"] = callback
            def set_on_trades(self, callback):
                captured["trades_callback"] = callback

        fake_module = ModuleType("longbridge.openapi")
        fake_module.Config = FakeConfig
        fake_module.QuoteContext = FakeContext
        fake_module.SubType = FakeSubType
        client = LongbridgeRealtimeClient(
            app_key="key", app_secret="secret", access_token="token",
            on_quote=lambda payload: captured.setdefault("quotes", []).append(payload),
        )
        fake_package = ModuleType("longbridge")
        fake_package.__path__ = []
        with patch.dict(sys.modules, {"longbridge": fake_package, "longbridge.openapi": fake_module}):
            client.connect()

        event = SimpleNamespace(
            last_done="123.45", sequence=987654321, timestamp=1795683600,
            trade_session=1, volume=1000, turnover="123450", trade_status=0,
        )
        captured["callback"]("AAPL.US", event)
        self.assertEqual(len(captured["quotes"]), 1)
        payload = captured["quotes"][0]
        self.assertEqual(payload["symbol"], "AAPL")
        self.assertEqual(payload["last_done"], 123.45)
        self.assertEqual(payload["sequence"], 987654321)
        self.assertEqual(payload["trade_session"], "pre-market")

    def test_volume_only_push_is_forwarded_without_inventing_a_price(self):
        captured = {}

        class FakeConfig:
            @staticmethod
            def from_apikey(*args, **kwargs):
                return object()

        class FakeSubType:
            Quote = object()
            Trade = object()

        class FakeContext:
            def __init__(self, config):
                pass
            def set_on_quote(self, callback):
                captured["callback"] = callback
            def set_on_trades(self, callback):
                captured["trades_callback"] = callback

        fake_module = ModuleType("longbridge.openapi")
        fake_module.Config = FakeConfig
        fake_module.QuoteContext = FakeContext
        fake_module.SubType = FakeSubType
        client = LongbridgeRealtimeClient(
            app_key="key", app_secret="secret", access_token="token",
            on_quote=lambda payload: captured.setdefault("quotes", []).append(payload),
        )
        fake_package = ModuleType("longbridge")
        fake_package.__path__ = []
        with patch.dict(sys.modules, {"longbridge": fake_package, "longbridge.openapi": fake_module}):
            client.connect()

        event = SimpleNamespace(
            last_done=None, sequence=12, timestamp=1795683601,
            trade_session=0, volume=0, current_volume=250,
            turnover=None, current_turnover="1000", trade_status=0, tag=0,
        )
        captured["callback"]("AAPL.US", event)
        self.assertEqual(len(captured["quotes"]), 1)
        self.assertIsNone(captured["quotes"][0]["last_done"])
        self.assertEqual(captured["quotes"][0]["current_volume"], 250)

    def test_reconnect_recreates_context_and_restores_subscriptions(self):
        client = LongbridgeRealtimeClient(
            app_key="key", app_secret="secret", access_token="token"
        )

        class FakeContext:
            def __init__(self):
                self.closed = False
                self.subscribed = []
            def close(self):
                self.closed = True
            def subscribe(self, symbols, sub_types):
                self.subscribed.extend(symbols)

        old_context = FakeContext()
        new_context = FakeContext()
        client._ctx = old_context
        client._subscribed_symbols = ["AAPL.US"]
        client._sub_types = [object(), object()]

        def connect():
            client._ctx = new_context
            client._sub_types = [object(), object()]
            client._subscribed_symbols = []

        client.connect = connect
        restored = client.reconnect(["AAPL", "MSFT"])
        self.assertTrue(old_context.closed)
        self.assertEqual(restored, ["AAPL", "MSFT"])
        self.assertEqual(new_context.subscribed, ["AAPL.US", "MSFT.US"])

    def test_trade_push_forwards_each_executed_price_tick(self):
        captured = {}

        class FakeConfig:
            @staticmethod
            def from_apikey(*args, **kwargs):
                return object()

        class FakeSubType:
            Quote = object()
            Trade = object()
            Trade = object()

        class FakeContext:
            def __init__(self, config):
                pass
            def set_on_quote(self, callback):
                captured["quote_callback"] = callback
            def set_on_trades(self, callback):
                captured["trade_callback"] = callback

        fake_module = ModuleType("longbridge.openapi")
        fake_module.Config = FakeConfig
        fake_module.QuoteContext = FakeContext
        fake_module.SubType = FakeSubType
        client = LongbridgeRealtimeClient(
            app_key="key", app_secret="secret", access_token="token",
            on_quote=lambda payload: captured.setdefault("quotes", []).append(payload),
        )
        fake_package = ModuleType("longbridge")
        fake_package.__path__ = []
        with patch.dict(sys.modules, {"longbridge": fake_package, "longbridge.openapi": fake_module}):
            client.connect()

        captured["trade_callback"](
            "AAPL.US",
            SimpleNamespace(trades=[
                SimpleNamespace(price="190.25", volume=10, timestamp=1795683602, trade_session=3),
                SimpleNamespace(price="190.30", volume=5, timestamp=1795683603, trade_session=3),
            ]),
        )
        self.assertEqual([q["last_done"] for q in captured["quotes"]], [190.25, 190.30])
        self.assertEqual([q["timestamp"] for q in captured["quotes"]], [1795683602, 1795683603])
        self.assertTrue(all(q["sequence"] is None for q in captured["quotes"]))
        self.assertEqual(captured["quotes"][0]["trade_session"], "overnight")
        self.assertEqual(captured["quotes"][0]["source"], "longbridge_trade_websocket")

    def test_websocket_push_without_last_done_is_not_a_price_tick(self):
        captured = {}

        class FakeConfig:
            @staticmethod
            def from_apikey(*args, **kwargs):
                return object()

        class FakeSubType:
            Quote = object()
            Trade = object()

        class FakeContext:
            def __init__(self, config):
                pass

            def set_on_quote(self, callback):
                captured["callback"] = callback
            def set_on_trades(self, callback):
                captured["trades_callback"] = callback

        fake_module = ModuleType("longbridge.openapi")
        fake_module.Config = FakeConfig
        fake_module.QuoteContext = FakeContext
        fake_module.SubType = FakeSubType
        client = LongbridgeRealtimeClient(
            app_key="key", app_secret="secret", access_token="token",
            on_quote=lambda payload: captured.setdefault("quotes", []).append(payload),
        )
        fake_package = ModuleType("longbridge")
        fake_package.__path__ = []
        with patch.dict(sys.modules, {"longbridge": fake_package, "longbridge.openapi": fake_module}):
            client.connect()
        captured["callback"]("AAPL.US", SimpleNamespace(volume=0, current_volume=0, sequence=987654322))
        self.assertEqual(captured.get("quotes", []), [])
        self.assertIsNotNone(client.last_message_monotonic)

    def test_subscription_sync_is_idempotent_and_adds_only_missing_symbols(self):
        quote_type = object()
        ctx = FakeQuoteContext([
            SimpleNamespace(symbol="AAPL.US", sub_types=[quote_type])
        ])
        client = LongbridgeRealtimeClient()
        client._ctx = ctx
        client._sub_types = [quote_type, object()]
        # The adapter's in-memory set is authoritative for this context life.
        client._subscribed_symbols = ["AAPL.US"]

        added = client.ensure_subscriptions(["AAPL", "MSFT"])
        self.assertEqual(added, ["MSFT"])
        self.assertEqual(ctx.subscribe_calls, [(["MSFT.US"], client._sub_types)])

        added_again = client.ensure_subscriptions(["AAPL", "MSFT"])
        self.assertEqual(added_again, [])
        self.assertEqual(len(ctx.subscribe_calls), 1)


if __name__ == "__main__":
    unittest.main()
