import unittest

from app import _is_out_of_order_realtime_tick


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

    def test_missing_timestamps_preserve_arrival_order(self):
        self.assertFalse(
            _is_out_of_order_realtime_tick(
                None, 101.0, "longbridge_websocket", "longbridge_trade_websocket"
            )
        )


if __name__ == "__main__":
    unittest.main()
