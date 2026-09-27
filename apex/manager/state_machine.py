"""Manager decision lifecycle; exchange protection mechanics remain in Execution."""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
class ManagerStatus(str,Enum):
    ACTIVE="ACTIVE"; PROTECTING="PROTECTING"; PARTIAL="PARTIAL"; CLOSED="CLOSED"
@dataclass(frozen=True)
class ManagerState:
    position_id:str
    status:ManagerStatus=ManagerStatus.ACTIVE
    tp1_confirmed:bool=False
    remaining_quantity:float=0.0
__all__=["ManagerState","ManagerStatus"]

# Transitional imports preserve the public Manager API while ownership lives in Execution.
from apex.execution.protection import ProtectionState,ProtectionStatus,new_stop_accepted,old_stop_cancelled,propose,reconcile_exchange_stop,replacement_uncertain,request
__all__ += ["ProtectionState","ProtectionStatus","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
