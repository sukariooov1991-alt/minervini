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

    def test_pullback_requires_reclaim_of_previous_high(self):
        n = 80
        close = [100.0] * (n - 6) + [99.0, 98.5, 98.0, 98.4, 98.7, 99.0]
        high = [v + 0.4 for v in close]
        low = [v - 0.4 for v in close]
        frame = pd.DataFrame({"close": close, "high": high, "low": low, "volume": [100000.0] * n})
        result = check_pullback(frame, require_volatility_contraction=False, proximity_pct=5.0)
        self.assertFalse(result["passed"])
        self.assertFalse(result["turning_up"])
        self.assertIsNotNone(result["entry_trigger_price"])
        self.assertIsNotNone(result["support_low"])

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
