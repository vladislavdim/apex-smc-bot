import time
import unittest
from unittest.mock import patch

import stats_server


class DashboardSingleFlightTests(unittest.TestCase):
    def test_dashboard_fetch_window_is_memory_bounded(self):
        self.assertEqual(stats_server.MAX_DASHBOARD_EVENTS, 5_000)

    def test_page_discloses_persisted_stale_snapshot(self):
        from apex.ui.dashboard.page import HTML

        self.assertIn("includes('STALE')", HTML)
        self.assertIn("Данные устарели", HTML)

    def test_main_does_not_eager_warm_dashboard(self):
        import inspect
        source = inspect.getsource(stats_server.main)
        self.assertNotIn("dashboard-cache-warm", source)
        self.assertNotIn("build_dashboard()", source)

    def setUp(self):
        with stats_server._DASHBOARD_CACHE_LOCK:
            stats_server._DASHBOARD_CACHE.clear()
            stats_server._DASHBOARD_PERSIST_CHECKED.clear()

    def test_routine_release_selector_cannot_reset_stable_cohort(self):
        calls = []

        def build(*args):
            calls.append(args)
            return {"summary": {"attempts": 7}, "available_releases": ["old"]}

        with patch.object(stats_server, "_build_dashboard_uncached", side_effect=build), \
             patch.object(stats_server, "_store_dashboard", wraps=stats_server._store_dashboard), \
             patch.object(stats_server, "_connect", side_effect=RuntimeError("no database")):
            first = stats_server.build_dashboard(days=1, release="latest")
            second = stats_server.build_dashboard(days=90, release="all")

        self.assertEqual(len(calls), 1)
        self.assertEqual(first["cohort_mode"], "stable")
        self.assertEqual(second["summary"]["attempts"], 7)
        self.assertEqual(second["dashboard_cache"]["status"], "HIT")
        self.assertEqual(calls[0][0], 30)
        self.assertEqual(calls[0][11], "")

    def test_concurrent_cache_miss_fails_fast(self):
        with stats_server._DASHBOARD_BUILD_LOCK:
            started = time.monotonic()
            with patch.object(stats_server, "_connect", side_effect=RuntimeError("no database")):
                with self.assertRaisesRegex(TimeoutError, "warming"):
                    stats_server.build_dashboard()
            self.assertLess(time.monotonic() - started, 0.2)


if __name__ == "__main__":
    unittest.main()
