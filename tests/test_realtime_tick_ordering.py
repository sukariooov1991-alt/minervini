import threading
import unittest
from collections import deque

from app import (
    _is_out_of_order_realtime_tick,
    _pop_next_realtime_quote,
    _realtime_timestamp_baseline,
    ScanController,
)


class RealtimeTickOrderingTests(unittest.TestCase):
    def test_older_quote_timestamp_is_rejected(self):
        self.assertTrue(_is_out_of_order_realtime_tick(100.0, 101.0))

    def test_same_timestamp_is_not_rejected(self):
        self.assertFalse(_is_out_of_order_realtime_tick(101.0, 101.0))

    def test_newer_quote_timestamp_is_accepted(self):
        self.assertFalse(_is_out_of_order_realtime_tick(102.0, 101.0))

    def test_parallel_workers_keep_each_symbols_queue_fifo(self):
        pending = deque([
            {"symbol": "AAPL", "last_done": 100.0},
            {"symbol": "AAPL", "last_done": 101.0},
            {"symbol": "MSFT", "last_done": 200.0},
        ])
        active = {"AAPL"}
        quote, symbol = _pop_next_realtime_quote(pending, active)
        self.assertEqual(symbol, "MSFT")
        self.assertEqual(quote["last_done"], 200.0)
        self.assertIsNone(_pop_next_realtime_quote(pending, active)[0])
        active.discard("AAPL")
        first, first_symbol = _pop_next_realtime_quote(pending, active)
        self.assertEqual(first_symbol, "AAPL")
        self.assertEqual(first["last_done"], 100.0)
        active.discard("AAPL")
        second, second_symbol = _pop_next_realtime_quote(pending, active)
        self.assertEqual(second_symbol, "AAPL")
        self.assertEqual(second["last_done"], 101.0)

    def test_first_push_uses_persisted_card_timestamp_as_baseline(self):
        self.assertEqual(
            _realtime_timestamp_baseline(None, "2026-11-27T11:15:00+00:00"),
            1795778100.0,
        )

    def test_in_memory_timestamp_takes_precedence_over_persisted_timestamp(self):
        self.assertEqual(
            _realtime_timestamp_baseline(200.0, "2026-11-27T11:15:00+00:00"),
            200.0,
        )

    def test_volume_only_quote_does_not_advance_price_timestamp(self):
        controller = ScanController.__new__(ScanController)
        controller._lock = threading.RLock()
        controller._state_cv = threading.Condition(controller._lock)
        controller._state_version = 0
        controller._cards = {"AAPL": {
            "symbol": "AAPL", "current_price": 190.0,
            "price_timestamp": None, "price_source": "Stored daily data",
        }}
        controller._latest_quote_timestamp = {}
        controller._ws_last_log_at = {}
        controller._ws_connected = False
        controller._realtime_pending = deque()
        controller._realtime_event = threading.Event()
        controller._on_realtime_quote({
            "symbol": "AAPL", "last_done": None, "sequence": 99,
            "timestamp": 200, "current_volume": 500,
            "source": "longbridge_websocket",
        })
        self.assertEqual(controller._cards["AAPL"]["current_price"], 190.0)
        self.assertNotIn("AAPL", controller._latest_quote_timestamp)
        controller._on_realtime_quote({
            "symbol": "AAPL", "last_done": 190.25, "sequence": 100,
            "timestamp": 199, "source": "longbridge_websocket",
        })
        self.assertEqual(controller._cards["AAPL"]["current_price"], 190.25)
        self.assertEqual(controller._latest_quote_timestamp["AAPL"], 199.0)

    def test_missing_timestamps_preserve_arrival_order(self):
        self.assertFalse(_is_out_of_order_realtime_tick(None, 101.0))


if __name__ == "__main__":
    unittest.main()
