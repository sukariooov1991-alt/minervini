import unittest

import pandas as pd

from analysis.market_filter import check_market_regime
from analysis.pullback import check_pullback
from data.trade_levels import build_trade_levels


class EntryDecisionTests(unittest.TestCase):
    def _index(self):
        close = [100.0 + i * 0.2 for i in range(220)]
        return pd.DataFrame({"close": close, "volume": [1_000_000 + i * 1000 for i in range(220)]})

    def test_market_breadth_is_required_and_thresholded(self):
        index = self._index()
        missing = check_market_regime(index)
        self.assertFalse(missing["market_favorable"])
        self.assertFalse(missing["breadth_ok"])
        weak = check_market_regime(index, breadth_pct=24.9)
        self.assertFalse(weak["market_favorable"])
        threshold = check_market_regime(index, breadth_pct=25.0)
        self.assertTrue(threshold["breadth_ok"])

    def _pullback_frame(self, current_open, current_close):
        close = [100.0] * 74 + [99.5, 99.0, 98.7, 98.2, 98.0]
        low = [value - 0.5 for value in close]
        high = [value + 0.5 for value in close]
        close.append(current_close)
        low.append(min(current_open, current_close) - 0.4)
        high.append(max(current_open, current_close) + 0.4)
        open_prices = [value for value in close[:-1]] + [current_open]
        return pd.DataFrame({
            "open": open_prices,
            "high": high,
            "low": low,
            "close": close,
            "volume": [100000.0] * len(close),
        })

    def test_pullback_is_independent_and_passes_after_turn_up(self):
        frame = self._pullback_frame(98.1, 98.8)
        result = check_pullback(frame)
        self.assertTrue(result["candidate"])
        self.assertTrue(result["pullback_detected"])
        self.assertTrue(result["turning_up"])
        self.assertTrue(result["passed"])
        self.assertEqual(result["entry_trigger_price"], 98.8)
        self.assertLess(result["support_low"], result["entry_trigger_price"])

    def test_pullback_waits_while_current_candle_is_falling(self):
        frame = self._pullback_frame(98.5, 98.1)
        result = check_pullback(frame)
        self.assertTrue(result["candidate"])
        self.assertFalse(result["turning_up"])
        self.assertFalse(result["passed"])
        self.assertIsNone(result["entry_trigger_price"])

    def test_trade_plan_without_anchor_is_never_complete(self):
        frame = pd.DataFrame({
            "high": [101.0 + i * 0.1 for i in range(60)],
            "low": [99.0 + i * 0.1 for i in range(60)],
            "close": [100.0 + i * 0.1 for i in range(60)],
            "volume": [100000.0] * 60,
        })
        plan = build_trade_levels("TEST", 105.0, None, frame, final_low=98.0)
        self.assertNotEqual(plan.status, "جاهز")
        self.assertEqual(plan.entry_zones, [])
        self.assertEqual(plan.targets, [])


if __name__ == "__main__":
    unittest.main()
