import unittest
import pandas as pd

from analysis.frame_utils import ordered_frame


class OrderedFrameTests(unittest.TestCase):
    def test_sorts_date_column_and_preserves_original_frame(self):
        source = pd.DataFrame({
            "date": ["2026-11-27", "2026-11-25", "2026-11-26"],
            "close": [3.0, 1.0, 2.0],
        })
        result = ordered_frame(source, reset_index=True)
        self.assertEqual(result["close"].tolist(), [1.0, 2.0, 3.0])
        self.assertEqual(source["close"].tolist(), [3.0, 1.0, 2.0])

    def test_sorts_datetime_index_without_resetting_when_requested(self):
        source = pd.DataFrame(
            {"close": [3.0, 1.0, 2.0]},
            index=pd.to_datetime(["2026-11-27", "2026-11-25", "2026-11-26"]),
        )
        result = ordered_frame(source)
        self.assertTrue(result.index.is_monotonic_increasing)
        self.assertEqual(result["close"].tolist(), [1.0, 2.0, 3.0])
        self.assertIsInstance(result.index, pd.DatetimeIndex)


if __name__ == "__main__":
    unittest.main()
