import unittest
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from data.market_time import (
    MarketSession, MarketTime, get_us_equity_market_close,
    is_trading_day, us_equity_early_closes, us_equity_market_holidays,
)


class MarketTimeTests(unittest.TestCase):
    def test_thanksgiving_and_black_friday(self):
        self.assertIsNone(get_us_equity_market_close(date(2026, 11, 26)))
        self.assertEqual(get_us_equity_market_close(date(2026, 11, 27)), time(13, 0))
        self.assertEqual(get_us_equity_market_close(date(2026, 11, 30)), time(16, 0))

    def test_good_friday_weekend_and_juneteenth(self):
        self.assertFalse(is_trading_day(date(2026, 4, 3)))
        self.assertFalse(is_trading_day(date(2026, 11, 28)))
        self.assertIn(date(2026, 6, 19), us_equity_market_holidays(2026))

    def test_independence_day_holiday_and_year_specific_early_close(self):
        self.assertIn(date(2026, 7, 3), us_equity_market_holidays(2026))
        # NYSE/Nasdaq list July 2, 2026 as a normal trading day, not an early close.
        self.assertEqual(get_us_equity_market_close(date(2026, 7, 2)), time(16, 0))
        ny = ZoneInfo("America/New_York")
        self.assertEqual(
            MarketTime.state(datetime(2026, 7, 2, 14, 0, tzinfo=ny)).session,
            MarketSession.REGULAR,
        )
        # The published 2028 calendar explicitly lists July 3 as a 1pm close.
        self.assertEqual(get_us_equity_market_close(date(2028, 7, 3)), time(13, 0))

    def test_session_boundaries_and_dst_timezone(self):
        ny = ZoneInfo("America/New_York")
        self.assertEqual(MarketTime.state(datetime(2026, 11, 27, 8, 0, tzinfo=ny)).session, MarketSession.PRE_MARKET)
        self.assertEqual(MarketTime.state(datetime(2026, 11, 27, 9, 30, tzinfo=ny)).session, MarketSession.REGULAR)
        self.assertEqual(MarketTime.state(datetime(2026, 11, 27, 13, 0, tzinfo=ny)).session, MarketSession.POST_MARKET)
        self.assertEqual(MarketTime.state(datetime(2026, 11, 30, 20, 0, tzinfo=ny)).session, MarketSession.OVERNIGHT)
        self.assertEqual(MarketTime.state(datetime(2026, 11, 27, 20, 0, tzinfo=ny)).session, MarketSession.CLOSED)
        self.assertEqual(MarketTime.state(datetime(2026, 11, 26, 12, 0, tzinfo=ny)).session, MarketSession.CLOSED)

    def test_naive_datetime_is_interpreted_as_new_york(self):
        state = MarketTime.state(datetime(2026, 11, 27, 10, 0))
        self.assertEqual(state.now.tzinfo.key, "America/New_York")
        self.assertEqual(state.session, MarketSession.REGULAR)

    def test_early_closes_are_only_trading_days(self):
        for day in us_equity_early_closes(2026):
            self.assertTrue(is_trading_day(day))

    def test_early_close_after_hours_ends_at_5pm(self):
        ny = ZoneInfo("America/New_York")
        self.assertEqual(MarketTime.state(datetime(2026, 11, 27, 16, 59, tzinfo=ny)).session, MarketSession.POST_MARKET)
        self.assertEqual(MarketTime.state(datetime(2026, 11, 27, 17, 0, tzinfo=ny)).session, MarketSession.CLOSED)

    def test_overnight_session_belongs_to_next_trading_date(self):
        ny = ZoneInfo("America/New_York")
        sunday = MarketTime.state(datetime(2026, 11, 29, 20, 30, tzinfo=ny))
        monday = MarketTime.state(datetime(2026, 11, 30, 2, 30, tzinfo=ny))
        friday_night = MarketTime.state(datetime(2026, 11, 27, 20, 30, tzinfo=ny))
        self.assertEqual(sunday.session, MarketSession.OVERNIGHT)
        self.assertEqual(sunday.trading_date, date(2026, 11, 30))
        self.assertEqual(monday.session, MarketSession.OVERNIGHT)
        self.assertEqual(monday.trading_date, date(2026, 11, 30))
        self.assertEqual(friday_night.session, MarketSession.CLOSED)


if __name__ == "__main__":
    unittest.main()
