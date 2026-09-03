import unittest
from core.strategy_catalog import STRATEGY_CATALOG


class StrategyCatalogTests(unittest.TestCase):
    def test_all_live_strategies_have_detailed_catalogs(self):
        self.assertEqual(set(STRATEGY_CATALOG), {"FAST", "MTF", "SWING", "ZONE", "WYCKOFF"})
        for name, data in STRATEGY_CATALOG.items():
            self.assertGreaterEqual(len(data["criteria"]), 15, name)
            self.assertTrue(data["timeframes"])
            self.assertTrue(data["rr"])

    def test_critical_trigger_and_geometry_categories_present(self):
        for name, data in STRATEGY_CATALOG.items():
            cats = {row["category"] for row in data["criteria"]}
            self.assertTrue(any(x in cats for x in {"Trigger", "Structure"}), name)
            self.assertIn("Geometry", cats, name)
            self.assertIn("AI quality gate", cats, name)


if __name__ == "__main__":
    unittest.main()
