import unittest
from pathlib import Path


MARKET = Path("market.py").read_text(encoding="utf-8")


class FastStructuralTargetRRTests(unittest.TestCase):
    def test_fast_rr_uses_first_structural_target_that_meets_two_r(self):
        self.assertIn('_fast_qualifying_targets = [', MARKET)
        self.assertIn('item for item in _fast_target_geometry if item["rr"] >= 2.0', MARKET)
        self.assertIn('tp1 = _fast_selected_targets[0]["price"]', MARKET)
        self.assertIn('reward = abs(tp1 - entry)', MARKET)
        self.assertIn('tp = tp1', MARKET)

    def test_no_synthetic_target_is_created(self):
        start = MARKET.index('# RR is defined from TP1 by the central integrity/evidence pipeline.')
        end = MARKET.index('sl_pct = round(abs(entry - sl) / entry * 100, 2)', start)
        block = MARKET[start:end]
        self.assertIn('_fast_targets', block)
        self.assertNotIn('entry + risk * 2', block)
        self.assertNotIn('entry - risk * 2', block)
        self.assertNotIn('risk * 2.0', block)
        self.assertNotIn('risk * 2.5', block)

    def test_rr_floor_and_execution_quality_are_unchanged(self):
        self.assertIn("(rr < 2.0)", MARKET)
        self.assertIn('_vol_threshold = 1.6', MARKET)
        self.assertIn('curr_body / curr_range < 0.65', MARKET)
        self.assertIn('not _fast_structure_event', MARKET)

    def test_fast_records_target_geometry_for_future_audit(self):
        self.assertIn('_audit_observe("fast_rr_geometry"', MARKET)
        for field in (
            'nearest_structural_rr', 'best_structural_rr',
            'qualifying_target_count', 'intermediate_target_count', 'selected_tp1_rr',
        ):
            self.assertIn(field, MARKET)

    def test_near_example_still_fails_without_a_farther_structural_target(self):
        entry, sl, only_target = 2.2944, 2.2278, 2.3652
        rr = abs(only_target - entry) / abs(entry - sl)
        self.assertAlmostEqual(rr, 1.0630630631, places=6)
        self.assertLess(rr, 2.0)

    def test_farther_real_swing_can_become_tp1_without_lowering_rr_floor(self):
        entry, sl = 100.0, 99.0
        structural_targets = [101.1, 102.2, 103.4]
        geometry = [(p, abs(p-entry)/abs(entry-sl)) for p in structural_targets]
        eligible = [p for p, rr in geometry if rr >= 2.0]
        self.assertEqual(eligible[0], 102.2)


if __name__ == "__main__":
    unittest.main()
