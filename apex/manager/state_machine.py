"""Manager protection compatibility surface.

Canonical Binance-facing protection state lives in apex.execution.protection.
Manager owns eligibility and lifecycle decisions, not exchange order mutation.
"""
from apex.execution.protection import (
    ProtectionState, ProtectionStatus, StopProtectionRequest,
    new_stop_accepted, old_stop_cancelled, propose,
    reconcile_exchange_stop, replacement_uncertain, request,
)
__all__=["ProtectionState","ProtectionStatus","StopProtectionRequest","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
