"""Execution owns exchange protection; Manager keeps only a compatibility surface."""
from apex.execution import protection as execution_protection
from apex.manager import state_machine


def test_manager_compatibility_surface_points_to_execution_owner():
    assert state_machine.ProtectionState is execution_protection.ProtectionState
    assert state_machine.ProtectionStatus is execution_protection.ProtectionStatus
    assert state_machine.propose is execution_protection.propose
    assert state_machine.request is execution_protection.request
    assert state_machine.new_stop_accepted is execution_protection.new_stop_accepted
    assert state_machine.old_stop_cancelled is execution_protection.old_stop_cancelled
    assert state_machine.replacement_uncertain is execution_protection.replacement_uncertain
    assert state_machine.reconcile_exchange_stop is execution_protection.reconcile_exchange_stop


def test_manager_lifecycle_is_separate_from_exchange_protection():
    state=state_machine.ManagerState(position_id="p1")
    assert state.position_id=="p1"
    assert state.status is state_machine.ManagerStatus.ACTIVE
