import unittest
from unittest.mock import patch

import pandas as pd

from analysis.vcp import check_breakout, check_vcp


class VCPSourceAlignmentTests(unittest.TestCase):
    def _frame(self):
        frame = pd.DataFrame({
            "open": [100.0] * 90,
            "high": [110.0] * 90,
            "low": [90.0] * 90,
            "close": [100.0] * 90,
            "volume": [500.0] * 75 + [1000.0] * 15,
        })
        frame.loc[75:84, "volume"] = 1000.0
        return frame

    def _contractions(self):
        return [
            {
                "high_idx": 60, "low_idx": 70,
                "high": 120.0, "low": 96.0, "depth_pct": 20.0,
            },
            {
                "high_idx": 85, "low_idx": 87,
                "high": 110.0, "low": 101.2, "depth_pct": 8.0,
            },
        ]

    def _check_with_patched_structure(self, frame):
        with (
            patch("analysis.vcp._find_swing_pivots", return_value=[]),
            patch("analysis.vcp._extract_contractions", return_value=self._contractions()),
        ):
            return check_vcp(frame)

    def test_low_volume_outside_final_contraction_does_not_confirm_dry_up(self):
        frame = self._frame()
        # The final contraction is quieter than the 50-day average, but the
        # only exceptionally quiet day occurs before that contraction begins.
        frame.loc[83, "volume"] = 100.0
        frame.loc[85:87, "volume"] = 400.0

        result = self._check_with_patched_structure(frame)

        self.assertFalse(result["volume_dry_up"])
        self.assertFalse(result["passed"])

    def test_quiet_day_inside_final_contraction_confirms_dry_up_even_if_long_slope_is_positive(self):
        frame = self._frame()
        frame.loc[85:87, "volume"] = [500.0, 300.0, 500.0]

        result = self._check_with_patched_structure(frame)

        self.assertTrue(result["volume_dry_up"])
        self.assertTrue(result["passed"])
        self.assertGreater(result["volume_slope_norm"], 0.0)


    def test_relative_final_contraction_over_ten_percent_is_not_rejected_by_an_unsupported_cap(self):
        frame = self._frame()
        frame.loc[85:87, "volume"] = [500.0, 300.0, 500.0]
        contractions = self._contractions()
        contractions[-1]["low"] = 96.8
        contractions[-1]["depth_pct"] = 12.0

        with (
            patch("analysis.vcp._find_swing_pivots", return_value=[]),
            patch("analysis.vcp._extract_contractions", return_value=contractions),
        ):
            result = check_vcp(frame)

        self.assertTrue(result["passed"])
        self.assertAlmostEqual(result["contraction_depths"][-1], 12.0)

    def test_breakout_requires_volume_above_50_day_average_not_135_percent(self):
        frame = pd.DataFrame({
            "high": [102.0] * 51,
            "low": [99.0] * 51,
            "close": [101.5] * 51,
            "volume": [100.0] * 50 + [101.0],
        })

        self.assertTrue(check_breakout(frame, pivot=100.0))

        frame.loc[50, "volume"] = 100.0
        self.assertFalse(check_breakout(frame, pivot=100.0))

    def test_breakout_does_not_require_unsupported_close_position_or_price_buffer(self):
        frame = pd.DataFrame({
            "high": [110.0] * 51,
            "low": [90.0] * 51,
            "close": [100.1] * 51,
            "volume": [100.0] * 50 + [101.0],
        })

        self.assertTrue(check_breakout(frame, pivot=100.0))
        self.assertFalse(check_breakout(frame, pivot=100.0, min_close_pos=0.65))
        self.assertFalse(check_breakout(frame, pivot=100.0, breakout_buffer_pct=0.25))


if __name__ == "__main__":
    unittest.main()
