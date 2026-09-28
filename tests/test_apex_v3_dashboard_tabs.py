"""Dashboard HTTP surface dispatches to the canonical V3 tab projections."""
import unittest
from unittest.mock import patch

from apex.ui.dashboard import server
from apex.ui.dashboard.api import project_tab


class DashboardTabTests(unittest.TestCase):
    def test_projection_uses_real_snapshot_keys(self):
        snapshot = {
            "system_overview": {"state": "HEALTHY"},
            "execution_mode": {"mode": "live"},
            "execution_health": {"source": "state"},
            "manager_db": {"trades": [1]},
            "market_data": {"ok": 5},
            "source_registry": [{"source": "Gate"}],
            "learning_v2": {"samples": 3},
            "function_health": {"ready": True},
            "incidents": [],
            "trade_stats": {"closed": 1},
            "rows": [{"strategy": "FAST"}],
            "funnels": [{"strategy": "FAST"}],
        }
        self.assertEqual(project_tab("overview", snapshot)["system"]["state"], "HEALTHY")
        self.assertEqual(project_tab("execution", snapshot)["health"]["source"], "state")
        self.assertEqual(project_tab("manager", snapshot)["positions"]["trades"], [1])
        self.assertEqual(project_tab("market", snapshot)["market_data"]["ok"], 5)
        self.assertEqual(project_tab("learning", snapshot)["authority"], "ADVISORY")
        self.assertEqual(project_tab("health", snapshot)["functions"]["ready"], True)
        self.assertEqual(project_tab("trades", snapshot)["stats"]["closed"], 1)
        self.assertEqual(project_tab("strategies", snapshot)["funnels"][0]["strategy"], "FAST")

    def _request(self, path, authenticated=True):
        handler = object.__new__(server.Handler)
        handler.path = path
        handler._auth = lambda query: authenticated
        response = []
        handler._json = lambda payload, status=200: response.append((status, payload))
        handler._html = lambda payload, status=200: response.append((status, payload))
        with patch.object(server, "build_dashboard", return_value={"execution_mode": {"mode": "live"}}) as build:
            handler.do_GET()
        return response[0], build

    def test_authenticated_tab_route_uses_real_projection(self):
        (status, payload), build = self._request("/api/dashboard/execution?key=valid")
        self.assertEqual(status, 200)
        self.assertEqual(payload["mode"], {"mode": "live"})
        build.assert_called_once()

    def test_unknown_and_unauthorized_tabs_never_build_snapshot(self):
        (status, payload), build = self._request("/api/dashboard/shadow?key=valid")
        self.assertEqual((status, payload), (404, {"error": "unknown_dashboard_tab"}))
        build.assert_not_called()
        (status, _), build = self._request("/api/dashboard/execution?key=bad", False)
        self.assertEqual(status, 403)
        build.assert_not_called()
