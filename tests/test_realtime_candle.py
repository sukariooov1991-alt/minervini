import unittest
from datetime import date, datetime
from threading import RLock
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

from app import ScanController, _latest_realtime_activity, _quote_timestamp_order, _stable_pullback_levels
from data.trade_levels import compute_atr


class RealtimeCandleTests(unittest.TestCase):
    def _controller(self):
        controller = ScanController.__new__(ScanController)
        controller._lock = RLock()
        controller._live_candles = {}
        return controller

    def test_watchdog_uses_new_subscription_time_after_quiet_reconnect(self):
        self.assertEqual(_latest_realtime_activity(100.0, 200.0), 200.0)
        self.assertEqual(_latest_realtime_activity(250.0, 200.0), 250.0)
        self.assertEqual(_latest_realtime_activity(None, 200.0), 200.0)
        self.assertIsNone(_latest_realtime_activity(None, None))

    def test_sequence_less_quote_timestamps_are_orderable(self):
        older = _quote_timestamp_order("2026-11-26T12:00:00Z")
        newer = _quote_timestamp_order("2026-11-26T12:00:01Z")
        self.assertIsNotNone(older)
        self.assertIsNotNone(newer)
        self.assertLess(older, newer)
        self.assertIsNone(_quote_timestamp_order("not-a-timestamp"))

    def test_market_session_label_respects_holidays_and_early_closes(self):
        with patch.dict("os.environ", {"MARKET_SESSION_LABEL": ""}):
            with patch("app.datetime") as mocked_datetime:
                mocked_datetime.now.return_value = datetime(
                    2026, 11, 26, 12, 8, tzinfo=ZoneInfo("America/New_York")
                )
                self.assertEqual(ScanController._detect_market_session(), "مغلق")

                mocked_datetime.now.return_value = datetime(
                    2026, 11, 27, 14, 0, tzinfo=ZoneInfo("America/New_York")
                )
                self.assertEqual(ScanController._detect_market_session(), "بعد الإغلاق")

                mocked_datetime.now.return_value = datetime(
                    2026, 11, 30, 10, 0, tzinfo=ZoneInfo("America/New_York")
                )
                self.assertEqual(ScanController._detect_market_session(), "مفتوح")

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

    def test_smart_reading_uses_the_same_wilder_atr_as_trade_levels(self):
        controller = ScanController.__new__(ScanController)
        close = [100.0 + (i % 5) for i in range(40)]
        frame = pd.DataFrame({
            "high": [value + (2.0 if i == 20 else 1.0) for i, value in enumerate(close)],
            "low": [value - 1.0 for value in close],
            "close": close,
        })
        expected_atr = compute_atr(frame, period=14)

        reading = controller._build_smart_reading_from_card(
            {"symbol": "TEST", "market_regime": "UNKNOWN"}, frame
        )

        self.assertIsNotNone(expected_atr)
        self.assertIn(f"ATR(14) يقارب {expected_atr:.2f}", reading)

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
            {"entry_trigger_price": 104.0, "support_low": 96.0},
            True,
        )
        self.assertEqual((first_anchor, first_low), (100.0, 95.0))
        self.assertEqual((next_anchor, next_low), (100.0, 95.0))
        self.assertEqual(_stable_pullback_levels({}, {}, False), (None, None))

    def test_pullback_anchor_resets_after_support_makes_a_new_low(self):
        anchor, support = _stable_pullback_levels(
            {
                "pullback_entry_anchor": 100.0,
                "pullback_support_low": 95.0,
            },
            {"entry_trigger_price": 97.0, "support_low": 93.0},
            True,
        )
        self.assertEqual(anchor, 97.0)
        self.assertEqual(support, 93.0)

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
