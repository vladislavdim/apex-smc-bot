import unittest
from pathlib import Path

class Post95StatsTests(unittest.TestCase):
    def test_dashboard_is_clamped_to_post95_baseline(self):
        s=Path("stats_server.py").read_text(encoding="utf-8")
        self.assertIn("STATS_BASELINE_UTC", s)
        self.assertIn("GREATEST(NOW() - (%s * INTERVAL '1 day'), %s::timestamptz)", s)
        self.assertIn('"baseline":"post95"', s)
        self.assertIn("Актуальная статистика после #95", s)

if __name__ == "__main__": unittest.main()
