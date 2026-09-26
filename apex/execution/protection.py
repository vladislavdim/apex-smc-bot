"""Canonical protection state-machine surface for execution."""
from apex.manager.state_machine import (ProtectionState,ProtectionStatus,new_stop_accepted,old_stop_cancelled,propose,reconcile_exchange_stop,replacement_uncertain,request)
__all__=["ProtectionState","ProtectionStatus","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
