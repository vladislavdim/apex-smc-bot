from __future__ import annotations

import unittest
from datetime import timezone

from apex.config.settings import ConfigParseError
from apex.ui.dashboard.config import DEFAULT_STATS_BASELINE_UTC, DashboardSettings


class DashboardConfigTests(unittest.TestCase):
    def valid(self, **overrides):
        env = {
            "DATABASE_URL": "postgres://internal/apex",
            "DASHBOARD_TOKEN": "dashboard-secret",
            "INGEST_TOKEN": "ingest-secret",
        }
        env.update(overrides)
        return env

    def test_typed_values_and_utc_baseline(self):
        settings = DashboardSettings.from_env(self.valid(
            PORT="12000",
            APEX_STATS_BASELINE_UTC="2026-09-10T09:55:47+02:00",
        ))
        settings.validate_startup()
        self.assertEqual(settings.port, 12000)
        self.assertIs(settings.stats_baseline_utc.tzinfo, timezone.utc)
        self.assertEqual(settings.stats_baseline_utc.isoformat(), "2026-09-10T07:55:47+00:00")

    def test_baseline_is_stable_when_not_explicitly_configured(self):
        settings = DashboardSettings.from_env(self.valid())
        self.assertEqual(settings.stats_baseline_utc.isoformat(), DEFAULT_STATS_BASELINE_UTC)

    def test_naive_or_invalid_baseline_fails_closed(self):
        with self.assertRaisesRegex(ConfigParseError, "TZ_REQUIRED"):
            DashboardSettings.from_env(self.valid(APEX_STATS_BASELINE_UTC="2026-09-10T07:55:47"))
        with self.assertRaisesRegex(ConfigParseError, "BASELINE_UTC_INVALID"):
            DashboardSettings.from_env(self.valid(APEX_STATS_BASELINE_UTC="not-a-date"))

    def test_invalid_port_fails_closed(self):
        with self.assertRaisesRegex(ConfigParseError, "PORT_INVALID"):
            DashboardSettings.from_env(self.valid(PORT="many"))
        with self.assertRaisesRegex(ConfigParseError, "PORT_OUT_OF_RANGE"):
            DashboardSettings.from_env(self.valid(PORT="70000"))

    def test_required_secrets_are_checked_only_at_startup(self):
        settings = DashboardSettings.from_env({})
        with self.assertRaisesRegex(
            ConfigParseError,
            "DATABASE_URL,DASHBOARD_TOKEN,INGEST_TOKEN",
        ):
            settings.validate_startup()
        self.assertNotIn("dashboard-secret", repr(DashboardSettings.from_env(self.valid())))


if __name__ == "__main__":
    unittest.main()
