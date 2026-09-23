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


if __name__ == "__main__":
    unittest.main()
