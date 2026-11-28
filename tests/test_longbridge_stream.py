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
    def test_installed_sdk_exposes_quote_push_api(self):
        from longbridge.openapi import QuoteContext, SubType

        self.assertTrue(callable(getattr(QuoteContext, "set_on_quote", None)))
        self.assertTrue(callable(getattr(QuoteContext, "set_on_trades", None)))
        self.assertIsNotNone(getattr(SubType, "Quote", None))
        self.assertIsNotNone(getattr(SubType, "Trade", None))

    def test_websocket_push_preserves_sequence_and_session(self):
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


    def test_trade_push_forwards_latest_execution_and_batch_count(self):
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
            on_quote=lambda payload: captured.setdefault("ticks", []).append(payload),
        )
        fake_package = ModuleType("longbridge")
        fake_package.__path__ = []
        with patch.dict(sys.modules, {"longbridge": fake_package, "longbridge.openapi": fake_module}):
            client.connect()

        captured["trades_callback"]("AAPL.US", SimpleNamespace(
            sequence=777,
            trades=[
                SimpleNamespace(price="123.40", volume=5, timestamp=100, trade_session=1),
                SimpleNamespace(price="123.55", volume=2, timestamp=101, trade_session=1),
            ],
        ))

        payload = captured["ticks"][0]
        self.assertEqual(payload["last_done"], 123.55)
        self.assertEqual(payload["timestamp"], 101)
        self.assertEqual(payload["sequence"], 777)
        self.assertEqual(payload["trade_session"], "pre-market")
        self.assertEqual(payload["source"], "longbridge_trade_websocket")
        self.assertEqual(payload["trade_count_delta"], 2)
        self.assertEqual(payload["trade_count_sequence"], 777)
        self.assertIsNone(payload["volume"])

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
        client._sub_types = [object()]

        def connect():
            client._ctx = new_context
            client._sub_types = [object(), object()]
            client._subscribed_symbols = []

        client.connect = connect
        restored = client.reconnect(["AAPL", "MSFT"])
        self.assertTrue(old_context.closed)
        self.assertEqual(restored, ["AAPL", "MSFT"])
        self.assertEqual(new_context.subscribed, ["AAPL.US", "MSFT.US"])

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
        quote_type = SimpleNamespace(name="Quote")
        trade_type = SimpleNamespace(name="Trade")
        ctx = FakeQuoteContext([
            SimpleNamespace(symbol="AAPL.US", sub_types=[quote_type, trade_type])
        ])
        client = LongbridgeRealtimeClient()
        client._ctx = ctx
        client._sub_types = [quote_type, trade_type]
        client._subscribed_symbols = ["AAPL.US"]

        added = client.ensure_subscriptions(["AAPL", "MSFT"])
        self.assertEqual(added, ["MSFT"])
        self.assertEqual(ctx.subscribe_calls, [(["MSFT.US"], client._sub_types)])
        self.assertEqual(set(client._subscribed_symbols), {"AAPL.US", "MSFT.US"})

        added_again = client.ensure_subscriptions(["AAPL", "MSFT"])
        self.assertEqual(added_again, [])
        self.assertEqual(len(ctx.subscribe_calls), 1)

    def test_transient_subscription_verification_error_is_retried_not_trusted(self):
        quote_type = SimpleNamespace(name="Quote")
        trade_type = SimpleNamespace(name="Trade")

        class FlakyVerificationContext(FakeQuoteContext):
            def __init__(self):
                super().__init__()
                self.verification_calls = 0

            def subscriptions(self):
                self.verification_calls += 1
                if self.verification_calls <= 2:
                    raise RuntimeError("temporary provider verification failure")
                return super().subscriptions()

        ctx = FlakyVerificationContext()
        client = LongbridgeRealtimeClient()
        client._ctx = ctx
        client._sub_types = [quote_type, trade_type]

        first = client.ensure_subscriptions(["AAPL"])
        self.assertEqual(first, ["AAPL"])
        self.assertEqual(client._subscribed_symbols, [])

        second = client.ensure_subscriptions(["AAPL"])
        self.assertEqual(second, [])
        self.assertEqual(client._subscribed_symbols, ["AAPL.US"])
        self.assertEqual(len(ctx.subscribe_calls), 1)

    def test_provider_unconfirmed_subscription_is_not_marked_active_and_is_retried(self):
        quote_type = SimpleNamespace(name="Quote")
        trade_type = SimpleNamespace(name="Trade")

        class DelayedQuoteContext(FakeQuoteContext):
            def __init__(self):
                super().__init__()
                self.confirm_after = 2

            def subscriptions(self):
                if len(self.subscribe_calls) < self.confirm_after:
                    return []
                return super().subscriptions()

        ctx = DelayedQuoteContext()
        client = LongbridgeRealtimeClient()
        client._ctx = ctx
        client._sub_types = [quote_type, trade_type]

        first = client.ensure_subscriptions(["AAPL"])
        self.assertEqual(first, ["AAPL"])
        self.assertEqual(client._subscribed_symbols, [])

        second = client.ensure_subscriptions(["AAPL"])
        self.assertEqual(second, ["AAPL"])
        self.assertEqual(client._subscribed_symbols, ["AAPL.US"])
        self.assertEqual(len(ctx.subscribe_calls), 2)


    def test_raw_push_activity_is_tracked_per_symbol(self):
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
            on_quote=lambda payload: None,
        )
        fake_package = ModuleType("longbridge")
        fake_package.__path__ = []
        with patch.dict(sys.modules, {"longbridge": fake_package, "longbridge.openapi": fake_module}):
            client.connect()

        captured["callback"](
            "AAPL.US",
            SimpleNamespace(last_done=None, volume=0, current_volume=10),
        )
        activity = client.symbol_last_message_monotonic
        self.assertIn("AAPL", activity)
        self.assertGreater(activity["AAPL"], 0)

    def test_resubscribe_verifies_quote_with_provider(self):
        quote_type = SimpleNamespace(name="Quote")
        trade_type = SimpleNamespace(name="Trade")

        class ResubscribeContext(FakeQuoteContext):
            def unsubscribe(self, symbols, sub_types):
                self.active = [item for item in self.active if item.symbol not in symbols]

        ctx = ResubscribeContext([
            SimpleNamespace(symbol="AAPL.US", sub_types=[quote_type, trade_type])
        ])
        client = LongbridgeRealtimeClient()
        client._ctx = ctx
        client._sub_types = [quote_type, trade_type]
        client._subscribed_symbols = ["AAPL.US"]

        result = client.resubscribe(["AAPL"])
        self.assertEqual(result, {"AAPL": True})
        self.assertEqual(len(ctx.subscribe_calls), 1)
        self.assertEqual(ctx.subscribe_calls[0][0], ["AAPL.US"])



if __name__ == "__main__":
    unittest.main()
