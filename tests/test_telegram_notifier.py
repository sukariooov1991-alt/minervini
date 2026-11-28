import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from data.telegram_notifier import TelegramNotifier


class TelegramNotifierConcurrencyTests(unittest.TestCase):
    def test_notification_requires_a_complete_in_range_plan_with_valid_risk(self):
        card = {
            "symbol": "AAPL",
            "passed_all_filters": True,
            "shown_on_main_page": True,
            "entry_zones": [100, 101, 102, 103, 104],
            "targets": [110, 113, 116, 119, 122],
            "shared_stop_loss": 95,
            "levels_status": "جاهز",
            "within_entry_range": True,
        }
        self.assertTrue(TelegramNotifier.is_eligible_for_notification(card))
        self.assertFalse(TelegramNotifier.is_eligible_for_notification({
            **card, "levels_status": "غير مكتمل"
        }))
        self.assertFalse(TelegramNotifier.is_eligible_for_notification({
            **card, "within_entry_range": False
        }))
        self.assertFalse(TelegramNotifier.is_eligible_for_notification({
            **card, "shared_stop_loss": 89
        }))

    def test_concurrent_entry_alerts_send_only_once_per_symbol(self):
        notifier = TelegramNotifier.__new__(TelegramNotifier)
        notifier.bot_token = "token"
        notifier.chat_id = "chat"
        notifier.timeout = 1
        notifier.state_db_path = ":memory:"
        notifier._sent_symbols = set()
        notifier._send_lock = threading.Lock()
        calls = []
        calls_lock = threading.Lock()

        class FakeResponse:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def read(self):
                return json.dumps({"ok": True}).encode()

        def fake_urlopen(*args, **kwargs):
            with calls_lock:
                calls.append(1)
            return FakeResponse()

        card = {
            "symbol": "AAPL",
            "passed_all_filters": True,
            "shown_on_main_page": True,
            "entry_zones": [100, 101, 102, 103, 104],
            "targets": [110, 113, 116, 119, 122],
            "shared_stop_loss": 95,
            "levels_status": "جاهز",
            "within_entry_range": True,
        }
        with patch("data.telegram_notifier.request.urlopen", side_effect=fake_urlopen):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(
                    lambda _: notifier.send_stock_appearance_alert(card),
                    range(2),
                ))

        self.assertEqual(sum(results), 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("AAPL", notifier._sent_symbols)


if __name__ == "__main__":
    unittest.main()
