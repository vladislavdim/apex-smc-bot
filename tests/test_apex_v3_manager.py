from __future__ import annotations

import unittest

from apex.domain.enums import Direction
from apex.manager.eligibility import ManagerFacts, eligible_actions, enforce_action
from apex.manager.groq_schema import parse_manager_review
from apex.manager.state_machine import (
    ProtectionState, ProtectionStatus, new_stop_accepted, old_stop_cancelled,
    propose, reconcile_exchange_stop, replacement_uncertain, request,
)


class ApexV3ManagerTests(unittest.TestCase):
    def facts(self, **changes) -> ManagerFacts:
        values = {
            "direction": Direction.LONG,
            "current_price": 105,
            "confirmed_stop": 95,
            "remaining_quantity": 1,
        }
        values.update(changes)
        return ManagerFacts(**values)

    def test_external_conflict_alone_cannot_close(self):
        facts = self.facts(external_conflict=True)
        self.assertNotIn("CLOSE", eligible_actions(facts))
        self.assertEqual(enforce_action("CLOSE", facts), "HOLD")

    def test_close_requires_deterministic_invalidation_reversal_or_emergency(self):
        self.assertIn("CLOSE", eligible_actions(self.facts(invalidation=True)))
        self.assertIn("CLOSE", eligible_actions(self.facts(reversal_confirmed=True)))
        self.assertIn("CLOSE", eligible_actions(self.facts(emergency=True)))

    def test_protect_allows_structural_risk_reduction_below_entry(self):
        facts = self.facts(proposed_stop=98.5, structural_protection=True)
        self.assertIn("PROTECT", eligible_actions(facts))
        self.assertNotIn("PARTIAL_EXIT", eligible_actions(facts))

    def test_stop_never_moves_backwards(self):
        long = self.facts(proposed_stop=94, structural_protection=True)
        short = self.facts(
            direction=Direction.SHORT, current_price=95, confirmed_stop=105,
            proposed_stop=106, structural_protection=True,
        )
        self.assertNotIn("PROTECT", eligible_actions(long))
        self.assertNotIn("PROTECT", eligible_actions(short))

    def test_partial_exit_requires_confirmed_tp1(self):
        self.assertNotIn("PARTIAL_EXIT", eligible_actions(self.facts()))
        self.assertIn("PARTIAL_EXIT", eligible_actions(self.facts(tp1_confirmed=True)))

    def test_groq_is_limited_to_eligible_action(self):
        result = parse_manager_review({
            "action": "CLOSE", "confidence": 0.99,
            "reason_codes": ["EXTERNAL_CONFLICT"], "summary": "close",
        }, self.facts(external_conflict=True))
        self.assertEqual(result.action, "HOLD")
        self.assertIn("ACTION_NOT_ELIGIBLE", result.reason_codes)

    def test_proposed_stop_is_not_confirmed_before_binance(self):
        state = ProtectionState(Direction.LONG, 95, "old")
        proposed = propose(state, 98.5, current_price=105, structural=True)
        requested = request(proposed)
        self.assertEqual(requested.confirmed_stop, 95)
        self.assertEqual(requested.requested_stop, 98.5)
        self.assertEqual(requested.status, ProtectionStatus.REQUESTED)

    def test_stop_replace_persists_new_id_before_old_cancel(self):
        state = request(propose(
            ProtectionState(Direction.LONG, 95, "old"), 98.5,
            current_price=105, structural=True,
        ))
        pending = new_stop_accepted(state, "new")
        self.assertEqual(pending.pending_order_id, "new")
        self.assertEqual(pending.old_order_id, "old")
        self.assertEqual(pending.confirmed_order_id, "old")
        self.assertEqual(pending.confirmed_stop, 95)
        confirmed = old_stop_cancelled(pending)
        self.assertEqual(confirmed.confirmed_order_id, "new")
        self.assertEqual(confirmed.confirmed_stop, 98.5)

    def test_uncertain_cancel_requires_reconciliation_without_third_stop(self):
        state = request(propose(
            ProtectionState(Direction.LONG, 95, "old"), 98.5,
            current_price=105, structural=True,
        ))
        uncertain = replacement_uncertain(new_stop_accepted(state, "new"))
        self.assertEqual(uncertain.status, ProtectionStatus.RECONCILE_REQUIRED)
        self.assertEqual(uncertain.pending_order_id, "new")
        reconciled = reconcile_exchange_stop(uncertain, stop=98.5, order_id="new")
        self.assertEqual(reconciled.status, ProtectionStatus.CONFIRMED)
        self.assertEqual(reconciled.confirmed_order_id, "new")


if __name__ == "__main__":
    unittest.main()
