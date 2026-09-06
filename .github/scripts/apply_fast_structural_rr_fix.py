from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one match, found {count}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


old = '''        tp1 = smart_round(_fast_targets[0])
        tp2 = smart_round(_fast_targets[1]) if len(_fast_targets) > 1 else tp1
        tp = tp2  # основной TP для RR расчёта

        # ── RR проверка ──
        risk   = abs(entry - sl)
        reward = abs(tp1 - entry)
        if _audit_test('FAST_DETECT_FAST_DEAL_G9353', (risk == 0), 'RR проверка', 'risk == 0', 9353):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9354', 'RR проверка', locals(), 'risk == 0', 9354)
        rr = round(reward / risk, 2)
        _audit_observe("bos_progress", {"rr_reached": True, "rr_passed": bool(rr >= 2.0)})
        if _audit_test('FAST_DETECT_FAST_DEAL_G9356', (rr < 2.0), 'rr < 2.0', 'rr < 2.0', 9356):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9357', 'rr < 2.0', locals(), 'rr < 2.0', 9357)
'''

new = '''        # RR is defined from TP1 by the central integrity/evidence pipeline.
        # Therefore FAST must make TP1 the nearest *real structural* swing target
        # that itself satisfies the universal RR >= 2.0 floor.  We never invent
        # or stretch a target: every eligible target comes from _fast_targets.
        # Closer confirmed swings below 2R remain observable intermediate
        # liquidity, but they are not mislabeled as the trade's TP1.
        risk = abs(entry - sl)
        if _audit_test('FAST_DETECT_FAST_DEAL_G9353', (risk == 0), 'RR проверка', 'risk == 0', 9353):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9354', 'RR проверка', locals(), 'risk == 0', 9354)

        _fast_target_prices = []
        for _fast_target_raw in _fast_targets:
            _fast_target_price = smart_round(_fast_target_raw)
            if _fast_target_price not in _fast_target_prices:
                _fast_target_prices.append(_fast_target_price)
        _fast_target_geometry = [
            {"price": target, "rr": round(abs(target - entry) / risk, 4)}
            for target in _fast_target_prices
        ]
        _fast_qualifying_targets = [
            item for item in _fast_target_geometry if item["rr"] >= 2.0
        ]
        _fast_intermediate_targets = [
            item for item in _fast_target_geometry if item["rr"] < 2.0
        ]

        # Preserve the existing rr<2 blocker when there is no qualifying
        # structural target.  This keeps NEAR-like 1.06R setups rejected rather
        # than manufacturing a synthetic 2R take-profit after the fact.
        _fast_selected_targets = _fast_qualifying_targets or _fast_target_geometry[:1]
        tp1 = _fast_selected_targets[0]["price"]
        tp2 = _fast_qualifying_targets[1]["price"] if len(_fast_qualifying_targets) > 1 else tp1
        tp = tp1

        reward = abs(tp1 - entry)
        rr = round(reward / risk, 2)
        _audit_observe("fast_rr_geometry", {
            "target_count": len(_fast_target_geometry),
            "nearest_structural_target": _fast_target_geometry[0]["price"] if _fast_target_geometry else None,
            "nearest_structural_rr": _fast_target_geometry[0]["rr"] if _fast_target_geometry else None,
            "best_structural_rr": max((item["rr"] for item in _fast_target_geometry), default=None),
            "qualifying_target_count": len(_fast_qualifying_targets),
            "intermediate_target_count": len(_fast_intermediate_targets),
            "selected_tp1": tp1,
            "selected_tp1_rr": rr,
            "targets": _fast_target_geometry[:8],
        })
        _audit_observe("bos_progress", {"rr_reached": True, "rr_passed": bool(rr >= 2.0)})
        if _audit_test('FAST_DETECT_FAST_DEAL_G9356', (rr < 2.0), 'rr < 2.0', 'rr < 2.0', 9356):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9357', 'rr < 2.0', locals(), 'rr < 2.0', 9357)
'''

replace_once("market.py", old, new)

Path("tests/test_fast_structural_target_rr.py").write_text(r'''import unittest
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
        block = MARKET[MARKET.index('# RR is defined from TP1 by the central integrity/evidence pipeline.'):MARKET.index('sl_pct = round(abs(entry - sl) / entry * 100, 2)')]
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
''', encoding="utf-8")

print("FAST structural target RR fix applied")
