import unittest
from pathlib import Path

class Post97StatsTests(unittest.TestCase):
    def test_dashboard_is_clamped_to_post97_baseline(self):
        s=Path("stats_server.py").read_text(encoding="utf-8")
        self.assertIn("2026-09-03T14:54:22+00:00", s)
        self.assertIn("GREATEST(NOW() - (%s * INTERVAL '1 day'), %s::timestamptz)", s)
        self.assertIn('"baseline":"post97"', s)
        self.assertIn("Актуальная статистика после #97", s)

if __name__ == "__main__": unittest.main()
