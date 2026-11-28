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



if __name__ == "__main__":
    unittest.main()
