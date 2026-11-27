import threading
import unittest
from collections import deque
from unittest.mock import patch

from app import ScanController


class RealtimePriceSourceTests(unittest.TestCase):
    def make_controller(self):
        controller = ScanController.__new__(ScanController)
        controller._lock = threading.RLock()
        controller._state_cv = threading.Condition(controller._lock)
        controller._cards = {"AAPL": {"symbol": "AAPL", "current_price": 100.0}}
        controller._latest_quote_sequence = {}
        controller._latest_quote_timestamp = {}
        controller._latest_quote_source = {}
        controller._ws_connected = False
        controller._ws_last_log_at = {}
        controller._realtime_pending = deque()
        controller._realtime_event = threading.Event()
        controller._state_version = 0
        return controller

    def test_quote_last_done_never_overwrites_displayed_trade_price(self):
        controller = self.make_controller()
        controller._on_realtime_quote({
            "symbol": "AAPL",
            "last_done": 101.25,
            "timestamp": 1795683601,
            "source": "longbridge_websocket",
            "volume": 1000,
            "current_volume": 20,
        })

        self.assertEqual(controller._cards["AAPL"]["current_price"], 100.0)
        self.assertEqual(controller._cards["AAPL"]["price_source"], None)
        self.assertIsNone(controller._realtime_pending[-1]["last_done"])
        self.assertFalse(controller._realtime_pending[-1]["_is_price_tick"])

    def test_executed_trade_is_the_only_source_that_updates_displayed_price(self):
        controller = self.make_controller()
        controller._on_realtime_quote({
            "symbol": "AAPL",
            "last_done": 101.25,
            "timestamp": 1795683602,
            "source": "longbridge_trade_websocket",
            "current_volume": 5,
        })

        self.assertEqual(controller._cards["AAPL"]["current_price"], 101.25)
        self.assertEqual(controller._cards["AAPL"]["price_source"], "Longbridge WebSocket")
        self.assertEqual(controller._realtime_pending[-1]["last_done"], 101.25)
        self.assertTrue(controller._realtime_pending[-1]["_is_price_tick"])


if __name__ == "__main__":
    unittest.main()
