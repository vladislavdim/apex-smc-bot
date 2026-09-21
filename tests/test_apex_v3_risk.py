from __future__ import annotations

import unittest

from apex.domain.enums import Direction, Strategy
from apex.domain.models import Candidate
from apex.execution.plan import build_execution_plan, client_order_ids
from apex.risk.engine import RiskLimits, RiskState, decide_risk


def candidate() -> Candidate:
    return Candidate(
        "cand_" + "1" * 32, "BTCUSDT", Strategy.MTF, Direction.LONG,
        100, 90, 110, 120, None, 2, "snap_" + "1" * 32,
    )


LIMITS = RiskLimits(1.0, 10, 3.0, 2.0, 1.5)
STATE = RiskState(True, False, 10_000, 0, 0, 0)


class ApexV3RiskTests(unittest.TestCase):
    def test_risk_never_increases_above_base(self):
        for multiplier in (-1, 0, 0.25, 1, 2, 100):
            result = decide_risk(candidate(), base_risk_pct=0.5, leverage=5, state=STATE, limits=LIMITS, dependency_multiplier=multiplier)
            self.assertLessEqual(result.final_risk_pct, 0.5)

    def test_dependency_can_reduce_but_not_boost(self):
        reduced = decide_risk(candidate(), base_risk_pct=0.5, leverage=5, state=STATE, limits=LIMITS, dependency_multiplier=0.4)
        self.assertEqual(reduced.decision, "REDUCE")
        self.assertEqual(reduced.final_risk_pct, 0.2)
        self.assertEqual(reduced.quantity, 2)
        kept = decide_risk(candidate(), base_risk_pct=0.5, leverage=5, state=STATE, limits=LIMITS, dependency_multiplier=2)
        self.assertEqual(kept.decision, "KEEP")
        self.assertEqual(kept.final_risk_pct, 0.5)

    def test_readiness_and_exposure_limits_block(self):
        blocked_states = [
            RiskState(False, False, 10_000, 0, 0, 0),
            RiskState(True, True, 10_000, 0, 0, 0),
            RiskState(True, False, 10_000, 3, 0, 0),
            RiskState(True, False, 10_000, 0, 2, 0),
            RiskState(True, False, 10_000, 0, 0, 1.5),
        ]
        for state in blocked_states:
            self.assertEqual(decide_risk(candidate(), base_risk_pct=0.5, leverage=5, state=state, limits=LIMITS).decision, "BLOCK")

    def test_execution_plan_preserves_candidate_geometry(self):
        source = candidate()
        risk = decide_risk(source, base_risk_pct=0.5, leverage=5, state=STATE, limits=LIMITS)
        plan = build_execution_plan(source, risk)
        self.assertEqual((plan.entry, plan.sl, plan.targets), (100, 90, (110, 120)))
        self.assertEqual(plan.candidate_id, source.candidate_id)
        ids = client_order_ids(plan.execution_id)
        self.assertEqual(len(set(ids.values())), 4)
        self.assertTrue(all(value.startswith("APEX-") for value in ids.values()))

    def test_blocked_risk_cannot_build_execution(self):
        blocked = decide_risk(candidate(), base_risk_pct=0.5, leverage=5, state=RiskState(False, False, 10_000, 0, 0, 0), limits=LIMITS)
        with self.assertRaisesRegex(ValueError, "approved_positive_risk_required"):
            build_execution_plan(candidate(), blocked)


if __name__ == "__main__":
    unittest.main()
