import unittest
from threading import RLock

from app import ScanController


class SnapshotIsolationTests(unittest.TestCase):
    def test_snapshot_detaches_nested_card_state_from_realtime_mutations(self):
        controller = ScanController.__new__(ScanController)
        controller._lock = RLock()
        controller._market_session_label = "مفتوح"
        controller._detect_market_session = lambda: "مفتوح"
        controller._state_version = 1
        controller._running = False
        controller._ws_connected = True
        controller._market_status = {}
        controller._last_error = None
        controller._scan_progress = {}
        controller._scan_errors = []
        controller._cards = {
            "AAPL": {
                "symbol": "AAPL",
                "filters": [{"name": "Trend Template", "passed": False}],
                "entry_zones": [100.0, 101.0],
            }
        }

        snapshot = controller.snapshot()
        controller._cards["AAPL"]["filters"][0]["passed"] = True
        controller._cards["AAPL"]["entry_zones"].append(102.0)

        self.assertFalse(snapshot["cards"][0]["filters"][0]["passed"])
        self.assertEqual(snapshot["cards"][0]["entry_zones"], [100.0, 101.0])


    def test_snapshot_does_not_query_sqlite_on_realtime_reads(self):
        controller = ScanController.__new__(ScanController)
        controller._lock = RLock()
        controller._market_session_label = "مفتوح"
        controller._detect_market_session = lambda: "مفتوح"
        controller._state_version = 1
        controller._running = True
        controller._ws_connected = True
        controller._market_status = {}
        controller._last_error = None
        controller._scan_progress = {"stage": "candidate_analysis", "completed": 12, "total": 493}
        controller._scan_errors = []
        controller._cards = {}

        class StorageMustNotBeRead:
            def load_watchlist_cards(self):
                raise AssertionError("snapshot must not perform SQLite I/O")

        controller.storage = StorageMustNotBeRead()
        snapshot = controller.snapshot()
        self.assertEqual(snapshot["scan_progress"]["completed"], 12)
        self.assertEqual(snapshot["cards"], [])



    def test_snapshot_keeps_market_and_candidate_progress_separate(self):
        controller = ScanController.__new__(ScanController)
        controller._lock = RLock()
        controller._market_session_label = "مغلق"
        controller._detect_market_session = lambda: "مغلق"
        controller._state_version = 2
        controller._running = True
        controller._ws_connected = False
        controller._market_status = {}
        controller._last_error = None
        controller._scan_progress = {"stage": "candidate_analysis", "completed": 1, "total": 493}
        controller._scan_progress_by_stage = {
            "market_data": {"stage": "market_data", "completed": 493, "total": 493, "percent": 100},
            "candidate_analysis": {"stage": "candidate_analysis", "completed": 1, "total": 493, "percent": 0},
        }
        controller._scan_errors = []
        controller._cards = {}

        snapshot = controller.snapshot()
        self.assertEqual(snapshot["scan_progress_by_stage"]["market_data"]["percent"], 100)
        self.assertEqual(snapshot["scan_progress_by_stage"]["candidate_analysis"]["completed"], 1)
        self.assertEqual(snapshot["scan_progress_by_stage"]["candidate_analysis"]["percent"], 0)



if __name__ == "__main__":
    unittest.main()
