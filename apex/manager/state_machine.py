"""Manager lifecycle state machine.

Exchange stop replacement lives in :mod:`apex.execution.protection`; this module
tracks only the Manager lifecycle of a confirmed Binance position.
"""
from __future__ import annotations
from dataclasses import dataclass,replace
from enum import Enum
class ManagerStatus(str,Enum):
    CONFIRMED="CONFIRMED"; ACTIVE="ACTIVE"; PARTIAL="PARTIAL"; CLOSING="CLOSING"; CLOSED="CLOSED"; RECONCILE_REQUIRED="RECONCILE_REQUIRED"
@dataclass(frozen=True)
class ManagerState:
    position_id:str
    status:ManagerStatus=ManagerStatus.CONFIRMED
    tp1_confirmed:bool=False

def activate(state:ManagerState)->ManagerState:
    return replace(state,status=ManagerStatus.ACTIVE) if state.status is ManagerStatus.CONFIRMED else state
def confirm_tp1(state:ManagerState)->ManagerState:
    return replace(state,status=ManagerStatus.PARTIAL,tp1_confirmed=True) if state.status in {ManagerStatus.ACTIVE,ManagerStatus.CONFIRMED} else state
def begin_close(state:ManagerState)->ManagerState:
    return replace(state,status=ManagerStatus.CLOSING) if state.status not in {ManagerStatus.CLOSED,ManagerStatus.CLOSING} else state
def confirm_closed(state:ManagerState)->ManagerState:
    return replace(state,status=ManagerStatus.CLOSED) if state.status is ManagerStatus.CLOSING else state
def require_reconcile(state:ManagerState)->ManagerState:return replace(state,status=ManagerStatus.RECONCILE_REQUIRED)
__all__=["ManagerState","ManagerStatus","activate","begin_close","confirm_closed","confirm_tp1","require_reconcile"]

# Temporary import-compatibility surface. Protection ownership remains in Execution.
from apex.execution.protection import (
    ProtectionState, ProtectionStatus, new_stop_accepted, old_stop_cancelled,
    propose, reconcile_exchange_stop, replacement_uncertain, request,
)
__all__ += ["ProtectionState","ProtectionStatus","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
