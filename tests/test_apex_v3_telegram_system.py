import unittest

from apex.ui.telegram.system import format_system_status


class TelegramSystemTests(unittest.TestCase):
    def test_formats_connections_release_and_reasons(self):
        text = format_system_status({
            "status": "DEGRADED", "health": "DEGRADED", "ready": False,
            "new_entries": "OFF", "release_sha": "a" * 40,
            "fencing_generation": 7,
            "reason_codes": ["READINESS_UNHEALTHY:gate:STALE"],
            "components": {
                "state_db": {"state": "READY"},
                "gate": {"state": "STALE"},
                "binance_reconciliation": {"state": "READY"},
                "strategy_activation": {"state": "READY"},
                "scanner_fast": {"state": "READY"},
            },
        })
        self.assertIn("Release: <code>aaaaaaaaaaaa</code>", text)
        self.assertIn("State DB: <b>READY</b>", text)
        self.assertIn("Gate: <b>STALE</b>", text)
        self.assertIn("Binance: <b>READY</b>", text)
        self.assertIn("Strategy activation: <b>READY</b>", text)
        self.assertIn("Scanner FAST: <b>READY</b>", text)
        self.assertIn("READINESS_UNHEALTHY:gate:STALE", text)

    def test_candidate_driven_components_are_not_reported_as_unknown(self):
        text = format_system_status({
            "status": "READY", "health": "HEALTHY", "ready": True,
            "release_sha": "b" * 40,
            "components": {
                "groq": {"state": "UNKNOWN"},
                "risk_engine": {"state": "UNKNOWN"},
                "scanner_wyckoff": {"state": "UNKNOWN"},
            },
        })
        self.assertIn("Groq: <b>ON_DEMAND</b>", text)
        self.assertIn("Risk engine: <b>ON_DEMAND</b>", text)
        self.assertIn("Scanner WYCKOFF: <b>WAITING_FIRST_RUN</b>", text)
        self.assertIn("это не ошибка подключения", text)


if __name__ == "__main__":
    unittest.main()
