import base64
import os
import unittest
from unittest.mock import patch

from app import app, controller


class ScanAPIValidationTests(unittest.TestCase):
    def setUp(self):
        self.access_key_patcher = patch.dict(
            os.environ, {"MINERVINI_ACCESS_KEY": "test-secret"}
        )
        self.access_key_patcher.start()
        self.addCleanup(self.access_key_patcher.stop)
        self.client = app.test_client()
        token = base64.b64encode(b"admin:test-secret").decode("ascii")
        self.client.environ_base["HTTP_AUTHORIZATION"] = f"Basic {token}"

    def test_empty_symbol_list_is_rejected_instead_of_starting_full_scan(self):
        with patch.object(controller, "start_scan") as start_scan:
            response = self.client.post("/api/scan/start", json={"symbols": []})
        self.assertEqual(response.status_code, 400)
        start_scan.assert_not_called()

    def test_update_must_be_a_boolean_not_a_truthy_string(self):
        with patch.object(controller, "start_scan") as start_scan:
            response = self.client.post(
                "/api/scan/start", json={"symbols": ["AAPL"], "update": "false"}
            )
        self.assertEqual(response.status_code, 400)
        start_scan.assert_not_called()

    def test_valid_custom_symbol_list_is_normalized_and_forwarded(self):
        with (
            patch.object(controller, "start_scan", return_value=True) as start_scan,
            patch.object(controller, "snapshot", return_value={"running": True}),
        ):
            response = self.client.post(
                "/api/scan/start",
                json={"symbols": ["aapl", "BRK.B", "^GSPC"], "update": False},
            )
        self.assertEqual(response.status_code, 200)
        start_scan.assert_called_once_with(
            symbols=["AAPL", "BRK.B", "^GSPC"], update=False
        )

    def test_non_object_json_body_is_rejected(self):
        response = self.client.post(
            "/api/scan/start", data='["AAPL"]', content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
