import unittest
from pathlib import Path

class ProductionBaselineStatsTests(unittest.TestCase):
    def test_dashboard_uses_stable_production_baseline(self):
        s=Path("apex/ui/dashboard/server.py").read_text(encoding="utf-8")
        self.assertIn("STATS_BASELINE_UTC = _SETTINGS.stats_baseline_utc", s)
        self.assertIn("GREATEST(NOW() - (%s * INTERVAL '1 day'), %s::timestamptz)", s)
        self.assertIn('"baseline":"production-live-v3"', s)
        self.assertNotIn('"baseline":"post97"', s)

if __name__ == "__main__": unittest.main()
