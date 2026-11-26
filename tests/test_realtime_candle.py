import unittest
from datetime import date
from threading import RLock

import pandas as pd

from app import ScanController, _stable_pullback_levels


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

    def test_startup_revalidation_clears_obsolete_pullback_entry_levels(self):
        controller = ScanController.__new__(ScanController)
        controller._live_prices = {}
        closes = [100.0 + i * 0.2 for i in range(80)]
        history = pd.DataFrame({
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": closes,
            "high": [value + 1.0 for value in closes],
            "low": [value - 1.0 for value in closes],
            "close": closes,
            "volume": [100000.0] * 80,
        })
        card = {
            "symbol": "AAPL",
            "vcp_passed": False,
            "breakout_passed": False,
            "pullback_passed": True,
            "pullback_candidate": True,
            "market_favorable": True,
            "entry_ready": True,
            "pivot_price": 115.0,
            "entry_zones": [115.0, 116.0, 117.0, 118.0, 119.0],
            "targets": [116.0, 117.0, 118.0, 119.0, 120.0],
            "shared_stop_loss": 110.0,
            "filters": [
                {"name": "Pullback Setup", "passed": True},
                {"name": "Pullback Entry Confirmation", "passed": True},
            ],
        }

        controller._refresh_restored_pullback("AAPL", card, history)

        self.assertFalse(card["pullback_passed"])
        self.assertFalse(card["pullback_candidate"])
        self.assertFalse(card["entry_confirmation_passed"])
        self.assertFalse(card["entry_ready"])
        self.assertIsNone(card["pivot_price"])
        self.assertEqual(card["entry_zones"], [])
        self.assertEqual(card["targets"], [])
        self.assertIsNone(card["shared_stop_loss"])

    def test_pullback_entry_anchor_is_frozen_across_price_ticks(self):
        first_anchor, first_low = _stable_pullback_levels(
            {},
            {"entry_trigger_price": 100.0, "support_low": 95.0},
            True,
        )
        next_anchor, next_low = _stable_pullback_levels(
            {
                "pullback_entry_anchor": first_anchor,
                "pullback_support_low": first_low,
            },
            {"entry_trigger_price": 104.0, "support_low": 94.0},
            True,
        )
        self.assertEqual((first_anchor, first_low), (100.0, 95.0))
        self.assertEqual((next_anchor, next_low), (100.0, 95.0))
        self.assertEqual(_stable_pullback_levels({}, {}, False), (None, None))

    def test_previous_completed_close_is_a_safe_reference_when_session_open_is_unknown(self):
        controller = self._controller()
        session = date(2026, 11, 27)
        previous_close_reference = {
            "open": 100.0, "high": 100.0, "low": 100.0,
            "close": 100.0, "volume": 0.0,
        }
        bar = controller._merge_live_candle(
            "AAPL", previous_close_reference, 101.0, 500.0, session
        )
        self.assertEqual(bar["open"], 100.0)
        self.assertEqual(bar["close"], 101.0)
        self.assertEqual(bar["high"], 101.0)
        self.assertEqual(bar["low"], 100.0)

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
