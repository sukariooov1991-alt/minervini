import unittest

import pandas as pd

from data.trade_levels import build_trade_levels, compute_atr


class TradeLevelsAtrFallbackTests(unittest.TestCase):
    @staticmethod
    def _history():
        rows = []
        close = 100.0
        for _ in range(30):
            rows.append({
                "open": close,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 100_000.0,
            })
        return pd.DataFrame(rows)

    def test_structural_low_remains_preferred_over_atr_fallback(self):
        history = self._history()
        plan = build_trade_levels(
            symbol="TEST",
            current_price=100.0,
            pivot_price=100.0,
            candles_df=history,
            final_low=97.0,
        )
        self.assertEqual(plan.shared_stop_loss, 97.0)
        self.assertEqual(plan.stop_source, "structural")

    def test_atr_fallback_is_used_when_structural_low_is_missing(self):
        history = self._history()
        atr = compute_atr(history, period=14)
        plan = build_trade_levels(
            symbol="TEST",
            current_price=100.0,
            pivot_price=100.0,
            candles_df=history,
            final_low=None,
        )
        self.assertIsNotNone(atr)
        self.assertEqual(plan.stop_source, "atr")
        self.assertAlmostEqual(plan.shared_stop_loss, 100.0 - 2.0 * atr, places=4)
        self.assertTrue(any("2.0 × ATR(14)" in note for note in plan.notes))

    def test_invalid_structural_low_uses_atr_fallback(self):
        history = self._history()
        plan = build_trade_levels(
            symbol="TEST",
            current_price=100.0,
            pivot_price=100.0,
            candles_df=history,
            final_low=101.0,
        )
        self.assertEqual(plan.stop_source, "atr")
        self.assertLess(plan.shared_stop_loss, 100.0)

    def test_targets_below_two_r_do_not_mark_the_plan_ready(self):
        history = self._history()
        plan = build_trade_levels(
            symbol="TEST",
            current_price=100.0,
            pivot_price=100.0,
            candles_df=history,
            final_low=95.0,
            target_calculator=lambda entries, *args: [
                entry + (entry - 95.0) * 1.5 for entry in entries
            ],
        )
        self.assertEqual(plan.status, "غير مكتمل")
        self.assertTrue(any("أقل من 2R" in note for note in plan.notes))

    def test_missing_structural_low_and_missing_atr_leave_stop_unavailable(self):
        plan = build_trade_levels(
            symbol="TEST",
            current_price=100.0,
            pivot_price=100.0,
            candles_df=None,
            final_low=None,
        )
        self.assertIsNone(plan.shared_stop_loss)
        self.assertIsNone(plan.stop_source)
        self.assertEqual(plan.status, "غير مكتمل")
        self.assertTrue(any("الفرصة تبقى معروضة" in note for note in plan.notes))


if __name__ == "__main__":
    unittest.main()
