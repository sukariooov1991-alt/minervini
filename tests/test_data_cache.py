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

    def test_news_and_earnings_event_history_is_not_trimmed(self):
        for idx in range(501):
            self.storage.add_event(
                "AAPL", "news", f"2026-01-{(idx % 28) + 1:02d}T{idx:04d}",
                {"title": f"News item {idx}"},
            )
        events = self.storage.get_events("AAPL", "news", limit=1000)
        self.assertEqual(len(events), 501)

    def test_incremental_recent_window_is_accepted_and_merged(self):
        initial_dates = pd.bdate_range("2024-01-01", periods=300)
        initial_rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in initial_dates
        ]
        self.storage.upsert_candles_bulk("NVDA", initial_rows)
        self.storage.set_cache("daily_source:NVDA", "Yahoo Finance")
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

    def test_initial_full_fetch_persists_only_latest_300_completed_bars(self):
        dates = pd.bdate_range(
            end=pd.Timestamp.now().normalize() - pd.offsets.BDay(1),
            periods=501,
        )
        frame = pd.DataFrame({
            "open": [10.0] * 501,
            "high": [11.0] * 501,
            "low": [9.0] * 501,
            "close": [10.5] * 501,
            "volume": [1000.0] * 501,
        }, index=dates)
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "FIRST"
        provider.storage = self.storage
        provider.last_source = None
        provider._fetch_yahoo = lambda period: frame

        fetched = provider.fetch_and_store_daily_candles("2y", append=True)
        stored = self.storage.get_candles_df("FIRST")
        self.assertEqual(fetched, 501)
        self.assertEqual(len(stored), 300)
        self.assertEqual(stored.iloc[0]["date"], pd.Timestamp(dates[201]))
        self.assertEqual(stored.iloc[-1]["date"], pd.Timestamp(dates[-1]))
        self.assertEqual(self.storage.get_cache("daily_source:FIRST"), "Yahoo Finance")

    def test_provider_change_rebases_full_history_instead_of_mixing_prices(self):
        old_dates = pd.bdate_range("2024-01-01", periods=300)
        old_rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in old_dates
        ]
        self.storage.upsert_candles_bulk("SWITCH", old_rows)
        self.storage.set_cache("daily_source:SWITCH", "Yahoo Finance")

        new_dates = pd.bdate_range(
            end=pd.Timestamp.now().normalize() - pd.offsets.BDay(1),
            periods=300,
        )
        full_new = pd.DataFrame({
            "open": [20.0] * 300,
            "high": [21.0] * 300,
            "low": [19.0] * 300,
            "close": [20.5] * 300,
            "volume": [2000.0] * 300,
        }, index=new_dates)
        recent_new = full_new.tail(10)
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "SWITCH"
        provider.storage = self.storage
        provider.last_source = None
        provider._fetch_yahoo = lambda period: (_ for _ in ()).throw(RuntimeError("Yahoo unavailable"))
        requested = []
        def stooq(period):
            requested.append(period)
            return full_new if period == "2y" else recent_new
        provider._fetch_stooq = stooq

        provider.fetch_and_store_daily_candles("10d", append=True)
        stored = self.storage.get_candles_df("SWITCH")
        self.assertEqual(requested, ["10d", "2y"])
        self.assertEqual(len(stored), 300)
        self.assertEqual(stored.iloc[0]["date"], pd.Timestamp(new_dates[0]))
        self.assertEqual(stored.iloc[-1]["close"], 20.5)
        self.assertEqual(self.storage.get_cache("daily_source:SWITCH"), "Stooq")

    def test_fresh_300_bar_cache_skips_historical_provider(self):
        dates = pd.bdate_range("2025-01-01", periods=300)
        rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in dates
        ]
        self.storage.upsert_candles_bulk("MSFT", rows)
        self.storage.set_cache("daily_source:MSFT", "Yahoo Finance")
        self.storage.set_cache("daily_refresh:MSFT", {"last_refresh": pd.Timestamp.now(tz="UTC").isoformat()})
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "MSFT"
        provider.storage = self.storage
        provider.last_source = None
        with patch.object(provider, "fetch_and_store_daily_candles", side_effect=AssertionError("provider should not be called")):
            self.assertEqual(provider.ensure_daily_history(), 0)
        self.assertEqual(provider.last_source, "SQLite daily cache")


    def test_partial_history_uses_full_window_to_reach_300_bars(self):
        dates = pd.bdate_range("2025-01-01", periods=260)
        rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in dates
        ]
        self.storage.upsert_candles_bulk("PART", rows)
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "PART"
        provider.storage = self.storage
        provider.last_source = None
        requested = []
        provider.fetch_and_store_daily_candles = lambda period, append=False: requested.append((period, append)) or 40

        self.assertEqual(provider.ensure_daily_history(initial_period="2y"), 40)
        self.assertEqual(requested, [("2y", True)])

    def test_young_stock_with_known_source_uses_incremental_window(self):
        dates = pd.bdate_range("2025-01-01", periods=200)
        rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in dates
        ]
        self.storage.upsert_candles_bulk("IPO", rows)
        self.storage.set_cache("daily_source:IPO", "Yahoo Finance")
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "IPO"
        provider.storage = self.storage
        provider.last_source = None
        requested = []
        provider.fetch_and_store_daily_candles = lambda period, append=False: requested.append(period) or 5

        self.assertEqual(provider.ensure_daily_history(), 5)
        self.assertEqual(requested, ["10d"])

    def test_recent_failed_refresh_is_not_mistaken_for_fresh_success(self):
        dates = pd.bdate_range("2025-01-01", periods=300)
        rows = [
            (day.strftime("%Y-%m-%d"), 10.0, 11.0, 9.0, 10.5, 1000.0)
            for day in dates
        ]
        self.storage.upsert_candles_bulk("FAIL", rows)
        self.storage.set_cache(
            "daily_refresh:FAIL",
            {"last_failed_attempt": pd.Timestamp.now(tz="UTC").isoformat()},
        )
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "FAIL"
        provider.storage = self.storage
        provider.last_source = None
        with patch.object(provider, "fetch_and_store_daily_candles", side_effect=AssertionError("must respect retry cooldown")):
            with self.assertRaisesRegex(RuntimeError, "refresh failed"):
                provider.ensure_daily_history()
        self.assertNotEqual(provider.last_source, "SQLite daily cache")

    def test_empty_fundamentals_response_is_cached_briefly(self):
        provider = DataProvider.__new__(DataProvider)
        provider.symbol = "EMPTY"
        provider.storage = self.storage
        calls = []
        provider._fetch_fundamentals_dataframe = lambda: calls.append(1) or pd.DataFrame()

        self.assertTrue(provider.get_fundamentals_dataframe().empty)
        self.assertTrue(provider.get_fundamentals_dataframe().empty)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
