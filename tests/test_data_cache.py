import unittest
from unittest.mock import patch

import pandas as pd

from data.data_provider import DataProvider
from data.storage_manager import StorageManager


class PermanentDataCacheTests(unittest.TestCase):
    def setUp(self):
        self.storage = StorageManager(":memory:")

    def test_daily_history_keeps_only_latest_300_completed_bars(self):
        dates = pd.bdate_range("2025-01-01", periods=305)
        rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in dates
        ]
        self.storage.upsert_candles_bulk("AAPL", rows)
        frame = self.storage.get_candles_df("AAPL")
        self.assertEqual(len(frame), 300)
        self.assertEqual(frame.iloc[0]["date"], pd.Timestamp(dates[5]))
        self.assertEqual(frame.iloc[-1]["date"], pd.Timestamp(dates[-1]))

    def test_cache_entries_survive_new_storage_manager_instance(self):
        self.storage.set_cache("fundamentals:AAPL", [{"eps_current": 2.5}])
        self.assertEqual(
            self.storage.get_cache("fundamentals:AAPL"),
            [{"eps_current": 2.5}],
        )

    def test_duplicate_news_or_earnings_events_are_not_inserted_twice(self):
        event = {"title": "Quarterly results", "source": "test"}
        self.storage.add_event("AAPL", "earnings", "2026-10-30", event)
        self.storage.add_event("AAPL", "earnings", "2026-10-30", event)
        self.assertEqual(len(self.storage.get_events("AAPL", "earnings", limit=10)), 1)

    def test_incremental_recent_window_is_accepted_and_merged(self):
        initial_dates = pd.bdate_range("2024-01-01", periods=300)
        initial_rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in initial_dates
        ]
        self.storage.upsert_candles_bulk("NVDA", initial_rows)
        new_dates = pd.bdate_range(initial_dates[-1] + pd.offsets.BDay(1), periods=10)
        recent = pd.DataFrame({
            "open": [11.0] * 10,
            "high": [12.0] * 10,
            "low": [10.0] * 10,
            "close": [11.5] * 10,
            "volume": [2000.0] * 10,
        }, index=new_dates)
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "NVDA"
        provider.storage = self.storage
        provider.last_source = None
        provider._fetch_yahoo = lambda period: recent
        self.assertEqual(provider.fetch_and_store_daily_candles("10d", append=True), 10)
        stored = self.storage.get_candles_df("NVDA")
        self.assertEqual(len(stored), 300)
        self.assertEqual(stored.iloc[-1]["date"], pd.Timestamp(new_dates[-1]))
        self.assertEqual(stored.iloc[0]["date"], pd.Timestamp(initial_dates[10]))

    def test_fresh_300_bar_cache_skips_historical_provider(self):
        dates = pd.bdate_range("2025-01-01", periods=300)
        rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in dates
        ]
        self.storage.upsert_candles_bulk("MSFT", rows)
        self.storage.set_cache("daily_refresh:MSFT", {"ok": True})
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "MSFT"
        provider.storage = self.storage
        provider.last_source = None
        with patch.object(provider, "fetch_and_store_daily_candles", side_effect=AssertionError("provider should not be called")):
            self.assertEqual(provider.ensure_daily_history(), 0)
        self.assertEqual(provider.last_source, "SQLite daily cache")


if __name__ == "__main__":
    unittest.main()
