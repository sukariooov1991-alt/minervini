import unittest
from datetime import date, time

from data.us_market_calendar import get_us_equity_market_close, us_equity_market_holidays


class USMarketCalendarTests(unittest.TestCase):
    def test_thanksgiving_is_closed_and_black_friday_is_early_close(self):
        self.assertIsNone(get_us_equity_market_close(date(2026, 11, 26)))
        self.assertEqual(get_us_equity_market_close(date(2026, 11, 27)), time(13, 0))
        self.assertEqual(get_us_equity_market_close(date(2026, 11, 30)), time(16, 0))

    def test_good_friday_and_weekends_are_closed(self):
        self.assertIsNone(get_us_equity_market_close(date(2026, 4, 3)))
        self.assertIsNone(get_us_equity_market_close(date(2026, 11, 28)))

    def test_independence_day_observed_holiday_and_prior_early_close(self):
        holidays = us_equity_market_holidays(2026)
        self.assertIn(date(2026, 7, 3), holidays)
        self.assertIsNone(get_us_equity_market_close(date(2026, 7, 3)))
        self.assertEqual(get_us_equity_market_close(date(2026, 7, 2)), time(13, 0))


if __name__ == "__main__":
    unittest.main()
