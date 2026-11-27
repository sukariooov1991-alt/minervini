import unittest
from collections import deque

from app import (
    _is_out_of_order_realtime_tick,
    _pop_next_realtime_quote,
    _realtime_timestamp_baseline,
)


class RealtimeTickOrderingTests(unittest.TestCase):
    def test_older_timestamp_is_rejected_across_sources(self):
        self.assertTrue(
            _is_out_of_order_realtime_tick(
                100.0, 101.0, "longbridge_trade_websocket", "longbridge_websocket"
            )
        )

    def test_same_second_quote_cannot_overwrite_trade(self):
        self.assertTrue(
            _is_out_of_order_realtime_tick(
                101.0, 101.0, "longbridge_websocket", "longbridge_trade_websocket"
            )
        )

    def test_same_second_trade_supersedes_quote(self):
        self.assertFalse(
            _is_out_of_order_realtime_tick(
                101.0, 101.0, "longbridge_trade_websocket", "longbridge_websocket"
            )
        )

    def test_newer_quote_is_accepted_after_trade(self):
        self.assertFalse(
            _is_out_of_order_realtime_tick(
                102.0, 101.0, "longbridge_websocket", "longbridge_trade_websocket"
            )
        )

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

    def test_missing_timestamps_preserve_arrival_order(self):
        self.assertFalse(
            _is_out_of_order_realtime_tick(
                None, 101.0, "longbridge_websocket", "longbridge_trade_websocket"
            )
        )


if __name__ == "__main__":
    unittest.main()
