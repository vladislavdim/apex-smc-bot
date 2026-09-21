import unittest

from apex.ui.telegram.incidents import format_incidents


class TelegramIncidentTests(unittest.TestCase):
    def test_empty_incident_view(self):
        self.assertIn("Активных инцидентов нет", format_incidents([]))

    def test_active_incident_is_bounded_and_rendered(self):
        text = format_incidents([{
            "code": "GATE_STALE",
            "component": "gate",
            "severity": "ERROR",
            "count": 3,
            "last_seen": "2026-09-12T10:20:30+00:00",
        }])
        self.assertIn("GATE_STALE", text)
        self.assertIn("x3", text)
        self.assertIn("2026-09-12T10:20:30", text)


if __name__ == "__main__":
    unittest.main()
