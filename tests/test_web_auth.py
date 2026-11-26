import base64
import os
import unittest
from unittest.mock import patch

from app import app, controller


class DashboardAccessTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def _auth_header(self, password):
        token = base64.b64encode(f"admin:{password}".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    def test_missing_access_key_fails_closed(self):
        with patch.dict(os.environ, {"MINERVINI_ACCESS_KEY": ""}):
            response = self.client.get("/api/state")
        self.assertEqual(response.status_code, 503)

    def test_dashboard_and_mutating_apis_reject_requests_without_key(self):
        with patch.dict(os.environ, {"MINERVINI_ACCESS_KEY": "test-secret"}):
            for method, path in (
                ("get", "/"),
                ("get", "/api/state"),
                ("post", "/api/scan/start"),
                ("post", "/api/scan/stop"),
                ("delete", "/api/cards/AAPL"),
            ):
                response = getattr(self.client, method)(path)
                self.assertEqual(response.status_code, 401, path)
                self.assertIn("Basic", response.headers.get("WWW-Authenticate", ""))

    def test_valid_key_allows_dashboard_and_state_api(self):
        with patch.dict(os.environ, {"MINERVINI_ACCESS_KEY": "test-secret"}):
            with patch.object(controller, "snapshot", return_value={"running": False, "version": 1}):
                page = self.client.get("/", headers=self._auth_header("test-secret"))
                state = self.client.get("/api/state", headers=self._auth_header("test-secret"))
        self.assertEqual(page.status_code, 200)
        self.assertEqual(state.status_code, 200)
        self.assertEqual(state.get_json()["version"], 1)

    def test_wrong_key_is_rejected(self):
        with patch.dict(os.environ, {"MINERVINI_ACCESS_KEY": "test-secret"}):
            response = self.client.get("/api/state", headers=self._auth_header("wrong"))
        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
