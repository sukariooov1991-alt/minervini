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

    def test_pullback_is_manual_review_only_without_source_defined_trigger(self):
        frame = pd.DataFrame({
            "close": [100.0, 99.0, 98.0],
            "high": [101.0, 100.0, 99.0],
            "low": [99.0, 98.0, 97.0],
            "volume": [100000.0] * 3,
        })
        result = check_pullback(frame)
        self.assertFalse(result["passed"])
        self.assertFalse(result["entry_eligible"])
        self.assertIn("manual review", result["reason"].lower())

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
