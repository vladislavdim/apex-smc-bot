"""Manager-facing compatibility surface for the canonical Execution protection state.

Manager owns decision eligibility; Execution owns exchange protection mechanics.
"""
from apex.execution.protection import (
    ProtectionState,ProtectionStatus,new_stop_accepted,old_stop_cancelled,propose,
    reconcile_exchange_stop,replacement_uncertain,request,valid_stop_replacement,
)
__all__=["ProtectionState","ProtectionStatus","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request","valid_stop_replacement"]
