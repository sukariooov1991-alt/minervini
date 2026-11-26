import unittest
from datetime import date
from threading import RLock

from app import ScanController


class RealtimeCandleTests(unittest.TestCase):
    def _controller(self):
        controller = ScanController.__new__(ScanController)
        controller._lock = RLock()
        controller._live_candles = {}
        return controller

    def test_live_candle_retains_open_high_low_and_volume_between_ticks(self):
        controller = self._controller()
        session = date(2026, 11, 26)

        first = controller._merge_live_candle("AAPL", None, 100.0, 1000.0, session)
        second = controller._merge_live_candle("AAPL", None, 101.0, 1200.0, session)
        third = controller._merge_live_candle("AAPL", None, 99.0, 1100.0, session)

        self.assertEqual(first["open"], 100.0)
        self.assertEqual(third["open"], 100.0)
        self.assertEqual(third["high"], 101.0)
        self.assertEqual(third["low"], 99.0)
        self.assertEqual(third["close"], 99.0)
        self.assertEqual(third["volume"], 1200.0)

    def test_stored_session_open_is_preserved_when_seeding_live_candle(self):
        controller = self._controller()
        session = date(2026, 11, 26)
        stored = {
            "open": 98.0, "high": 100.0, "low": 97.0,
            "close": 99.0, "volume": 500.0,
        }

        bar = controller._merge_live_candle("MSFT", stored, 99.5, 600.0, session)

        self.assertEqual(bar["open"], 98.0)
        self.assertEqual(bar["high"], 100.0)
        self.assertEqual(bar["low"], 97.0)
        self.assertEqual(bar["close"], 99.5)
        self.assertEqual(bar["volume"], 600.0)


if __name__ == "__main__":
    unittest.main()
