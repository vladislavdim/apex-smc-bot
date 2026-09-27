"""Manager-facing compatibility surface for protection state.

Protection order mechanics are owned by :mod:`apex.execution.protection`.
Manager keeps this import surface during the V3 cutover so existing callers do
not create a second protection state machine.
"""
from apex.execution.protection import (
    ProtectionRequest, ProtectionState, ProtectionStatus,
    new_stop_accepted, old_stop_cancelled, propose,
    reconcile_exchange_stop, replacement_uncertain, request,
    valid_stop_replacement,
)
__all__=[
    "ProtectionRequest","ProtectionState","ProtectionStatus",
    "new_stop_accepted","old_stop_cancelled","propose",
    "reconcile_exchange_stop","replacement_uncertain","request",
    "valid_stop_replacement",
]
