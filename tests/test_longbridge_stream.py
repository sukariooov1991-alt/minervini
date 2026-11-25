import unittest
from types import SimpleNamespace

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
    def test_latest_extended_session_quote_wins(self):
        quote = SimpleNamespace(
            symbol="AAPL.US",
            last_done="200",
            timestamp=100,
            pre_market_quote=SimpleNamespace(last_done="201", timestamp=150),
            post_market_quote=SimpleNamespace(last_done="202", timestamp=250),
            over_night_quote=SimpleNamespace(last_done="203", timestamp=350),
        )
        price, timestamp, session = LongbridgeRealtimeClient._latest_session_quote(quote)
        self.assertEqual(price, 203.0)
        self.assertEqual(timestamp, 350)
        self.assertEqual(session, "overnight")

    def test_regular_quote_is_used_when_extended_quotes_missing(self):
        quote = SimpleNamespace(
            last_done="199.50",
            timestamp=123,
            pre_market_quote=None,
            post_market_quote=None,
            over_night_quote=None,
        )
        self.assertEqual(
            LongbridgeRealtimeClient._latest_session_quote(quote),
            (199.5, 123, "regular"),
        )

    def test_subscription_sync_is_idempotent_and_adds_only_missing_symbols(self):
        quote_type = object()
        ctx = FakeQuoteContext([
            SimpleNamespace(symbol="AAPL.US", sub_types=[quote_type])
        ])
        client = LongbridgeRealtimeClient()
        client._ctx = ctx
        client._sub_type_quote = quote_type

        added = client.ensure_subscriptions(["AAPL", "MSFT"])
        self.assertEqual(added, ["MSFT"])
        self.assertEqual(ctx.subscribe_calls, [(["MSFT.US"], [quote_type])])

        added_again = client.ensure_subscriptions(["AAPL", "MSFT"])
        self.assertEqual(added_again, [])
        self.assertEqual(len(ctx.subscribe_calls), 1)


if __name__ == "__main__":
    unittest.main()
