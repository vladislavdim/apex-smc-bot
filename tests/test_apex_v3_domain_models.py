from datetime import datetime, timezone
import unittest

from apex.domain.enums import Direction, Strategy
from apex.domain.ids import new_id
from apex.domain.models import (
    Candidate,
    ExecutionPlan,
    ManagerDecision,
    TradeOutcome,
)


class DomainModelTests(unittest.TestCase):
    def candidate(self, **changes):
        values = {
            "candidate_id": new_id("candidate"),
            "snapshot_id": new_id("snapshot"),
            "symbol": "BTCUSDT",
            "strategy": Strategy.MTF,
            "direction": Direction.LONG,
            "entry": 100.0,
            "initial_sl": 95.0,
            "tp1": 110.0,
            "tp2": 115.0,
            "tp3": 120.0,
            "rr": 4.0,
            "created_at": datetime.now(timezone.utc),
        }
        values.update(changes)
        return Candidate(**values)

    def test_canonical_candidate_accepts_ordered_geometry(self):
        self.assertEqual(self.candidate().direction, Direction.LONG)
        self.assertEqual(
            self.candidate(
                direction=Direction.SHORT, initial_sl=105, tp1=90, tp2=85, tp3=80,
            ).direction,
            Direction.SHORT,
        )

    def test_candidate_rejects_invalid_ids_and_non_finite_geometry(self):
        for changes in (
            {"candidate_id": "cand_1"},
            {"rr": float("nan")},
            {"tp3": float("inf")},
            {"created_at": datetime.now()},
        ):
            with self.subTest(changes=changes), self.assertRaises((ValueError, TypeError)):
                self.candidate(**changes)

    def test_downstream_entities_require_typed_chain_ids(self):
        candidate = self.candidate()
        plan = ExecutionPlan(
            execution_id=new_id("execution"), candidate_id=candidate.candidate_id,
            symbol="BTCUSDT", direction=Direction.LONG, entry=100, sl=95,
            targets=(110, 115), quantity=1,
        )
        self.assertTrue(plan.execution_id.startswith("exec_"))
        with self.assertRaisesRegex(ValueError, "execution_id"):
            ExecutionPlan(
                execution_id="17", candidate_id=candidate.candidate_id,
                symbol="BTCUSDT", direction=Direction.LONG, entry=100, sl=95,
                targets=(110,), quantity=1,
            )

    def test_manager_and_outcome_reject_free_form_ids_and_reasons(self):
        position_id = new_id("position")
        decision = ManagerDecision(
            manager_event_id=new_id("manager_event"), position_id=position_id,
            action="HOLD", eligible_actions=("HOLD",), proposed_stop=None,
            reason_codes=("NO_CHANGE",),
        )
        self.assertEqual(decision.reason_codes, ("NO_CHANGE",))
        with self.assertRaisesRegex(ValueError, "invalid_reason_code"):
            ManagerDecision(
                manager_event_id=new_id("manager_event"), position_id=position_id,
                action="HOLD", eligible_actions=("HOLD",), proposed_stop=None,
                reason_codes=("human explanation",),
            )
        with self.assertRaisesRegex(ValueError, "outcome_id"):
            TradeOutcome(
                outcome_id="outcome-1", position_id=position_id,
                weighted_entry=100, weighted_exit=110, net_r=2,
                fees=1, funding=0, closed_at=datetime.now(timezone.utc),
            )


if __name__ == "__main__":
    unittest.main()
