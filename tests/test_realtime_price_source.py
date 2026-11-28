import threading
import unittest
from collections import deque

from app import ScanController


class RealtimePriceSourceTests(unittest.TestCase):
    def make_controller(self):
        controller = ScanController.__new__(ScanController)
        controller._lock = threading.RLock()
        controller._state_cv = threading.Condition(controller._lock)
        controller._cards = {"AAPL": {"symbol": "AAPL", "current_price": 100.0}}
        controller._latest_quote_timestamp = {}
        controller._ws_connected = False
        controller._ws_last_log_at = {}
        controller._realtime_pending = deque()
        controller._realtime_event = threading.Event()
        controller._state_version = 0
        return controller

    def test_quote_last_done_updates_the_displayed_live_price(self):
        controller = self.make_controller()
        controller._on_realtime_quote({
            "symbol": "AAPL",
            "last_done": 101.25,
            "timestamp": 1795683601,
            "source": "longbridge_websocket",
            "volume": 1000,
            "current_volume": 20,
        })
        self.assertEqual(controller._cards["AAPL"]["current_price"], 101.25)
        self.assertEqual(controller._cards["AAPL"]["price_source"], "Longbridge WebSocket")
        self.assertEqual(controller._cards["AAPL"]["price_feed_source"], "longbridge_websocket")
        self.assertEqual(controller._realtime_pending[-1]["last_done"], 101.25)
        self.assertTrue(controller._realtime_pending[-1]["_is_price_tick"])


    def test_trade_push_counts_executions_without_overwriting_quote_price(self):
        controller = self.make_controller()
        controller._on_realtime_quote({
            "symbol": "AAPL",
            "last_done": 101.35,
            "timestamp": 1795683603,
            "source": "longbridge_trade_websocket",
            "trade_session": "regular",
            "trade_count_delta": 2,
            "trade_count_sequence": 12345,
        })
        self.assertEqual(controller._cards["AAPL"]["current_price"], 100.0)
        self.assertNotEqual(controller._cards["AAPL"].get("price_feed_source"), "longbridge_trade_websocket")
        self.assertEqual(controller._cards["AAPL"]["trade_count"], 2)
        self.assertEqual(controller._realtime_pending, deque())

    def test_volume_only_quote_does_not_overwrite_price_or_timestamp(self):
        controller = self.make_controller()
        controller._cards["AAPL"]["price_timestamp"] = "2026-11-27T15:00:00+00:00"
        controller._on_realtime_quote({
            "symbol": "AAPL",
            "last_done": None,
            "timestamp": 1795683602,
            "source": "longbridge_websocket",
            "current_volume": 20,
        })
        self.assertEqual(controller._cards["AAPL"]["current_price"], 100.0)
        self.assertEqual(controller._cards["AAPL"]["price_timestamp"], "2026-11-27T15:00:00+00:00")
        self.assertIsNone(controller._realtime_pending[-1]["last_done"])
        self.assertFalse(controller._realtime_pending[-1]["_is_price_tick"])


if __name__ == "__main__":
    unittest.main()
