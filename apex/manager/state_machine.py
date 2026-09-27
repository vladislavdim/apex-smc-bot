"""Compatibility import for the Execution-owned protection state machine."""
from apex.execution.protection import ProtectionState,ProtectionStatus,new_stop_accepted,old_stop_cancelled,propose,reconcile_exchange_stop,replacement_uncertain,request
__all__=["ProtectionState","ProtectionStatus","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
