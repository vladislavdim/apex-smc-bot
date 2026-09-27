"""Manager lifecycle state machine.

Manager owns lifecycle decisions for confirmed Binance positions. Exchange stop
replacement itself is owned by :mod:\`apex.execution.protection\`.
"""
from __future__ import annotations
from dataclasses import dataclass,replace
from enum import Enum
from apex.execution.protection import (
    ProtectionState,ProtectionStatus,StopProtectionRequest,new_stop_accepted,
    old_stop_cancelled,propose,reconcile_exchange_stop,replacement_uncertain,request,
)

class ManagerStatus(str,Enum):
    CONFIRMED="CONFIRMED"
    MANAGING="MANAGING"
    EXIT_PENDING="EXIT_PENDING"
    CLOSED="CLOSED"
    RECONCILE_REQUIRED="RECONCILE_REQUIRED"

@dataclass(frozen=True)
class ManagerState:
    position_id:str
    status:ManagerStatus=ManagerStatus.CONFIRMED
    last_action:str="HOLD"
    tp1_confirmed:bool=False

    def transition(self,status:ManagerStatus,*,action:str|None=None)->"ManagerState":
        if self.status is ManagerStatus.CLOSED and status is not ManagerStatus.CLOSED:
            raise ValueError("closed_manager_state_is_terminal")
        return replace(self,status=status,last_action=action or self.last_action)

__all__=["ManagerState","ManagerStatus","ProtectionState","ProtectionStatus","StopProtectionRequest","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
