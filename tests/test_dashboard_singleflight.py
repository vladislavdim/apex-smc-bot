import time
import unittest
from unittest.mock import patch

import stats_server


class DashboardSingleFlightTests(unittest.TestCase):
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

    def test_fetch_is_bounded_for_free_tier_memory(self):
        executed = {}

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, query, params):
                executed["query"] = query
                executed["params"] = params

            def fetchall(self):
                return []

        class Connection:
            def cursor(self, **_kwargs):
                return Cursor()

            def close(self):
                pass

        with patch.object(stats_server, "_connect", return_value=Connection()):
            stats_server._fetch(30, "", "")

        self.assertIn("LIMIT %s", executed["query"])
        self.assertEqual(executed["params"][-1], stats_server._DASHBOARD_EVENT_LIMIT)
        self.assertLessEqual(stats_server._DASHBOARD_EVENT_LIMIT, 5_000)

    def test_main_does_not_eagerly_warm_dashboard(self):
        with patch.object(type(stats_server._SETTINGS), "validate_startup"), \
             patch.object(stats_server, "ensure_schema"), \
             patch.object(stats_server, "build_dashboard") as build, \
             patch.object(stats_server, "APEXStatsServer") as server:
            server.return_value.serve_forever.side_effect = RuntimeError("stop")
            with self.assertRaisesRegex(RuntimeError, "stop"):
                stats_server.main()

        build.assert_not_called()


if __name__ == "__main__":
    unittest.main()
