"""Execution-owned exchange protection state machine.

Manager may propose an eligible protective action, but Execution owns the
validated stop replacement and Binance-confirmed transition.
"""
from __future__ import annotations
from dataclasses import dataclass, replace
from enum import Enum
from apex.domain.enums import Direction

class ProtectionStatus(str, Enum):
    CONFIRMED="CONFIRMED"
    REQUESTED="REQUESTED"
    STOP_REPLACEMENT_PENDING="STOP_REPLACEMENT_PENDING"
    RECONCILE_REQUIRED="RECONCILE_REQUIRED"

@dataclass(frozen=True)
class ProtectionState:
    direction: Direction
    confirmed_stop: float
    confirmed_order_id: str
    proposed_stop: float|None=None
    requested_stop: float|None=None
    pending_order_id: str|None=None
    old_order_id: str|None=None
    status: ProtectionStatus=ProtectionStatus.CONFIRMED

@dataclass(frozen=True)
class ProtectionTransition:
    current_stop: float
    proposed_stop: float
    accepted: bool
    reason: str

def validate_stop_transition(*,direction:Direction,current_stop:float,proposed_stop:float,current_price:float)->ProtectionTransition:
    ok=(current_stop < proposed_stop < current_price) if direction is Direction.LONG else (current_price < proposed_stop < current_stop)
    return ProtectionTransition(float(current_stop),float(proposed_stop),bool(ok),"OK" if ok else "NON_PROTECTIVE_STOP")

def propose(state:ProtectionState,level:float,*,current_price:float,structural:bool)->ProtectionState:
    if not structural:return state
    t=validate_stop_transition(direction=state.direction,current_stop=state.confirmed_stop,proposed_stop=level,current_price=current_price)
    return replace(state,proposed_stop=float(level)) if t.accepted else state

def request(state:ProtectionState)->ProtectionState:
    if state.proposed_stop is None or state.status is not ProtectionStatus.CONFIRMED:return state
    return replace(state,requested_stop=state.proposed_stop,proposed_stop=None,old_order_id=state.confirmed_order_id,status=ProtectionStatus.REQUESTED)

def new_stop_accepted(state:ProtectionState,new_order_id:str)->ProtectionState:
    if state.status is not ProtectionStatus.REQUESTED or not new_order_id:return state
    return replace(state,pending_order_id=new_order_id,status=ProtectionStatus.STOP_REPLACEMENT_PENDING)

def old_stop_cancelled(state:ProtectionState)->ProtectionState:
    if state.status is not ProtectionStatus.STOP_REPLACEMENT_PENDING:return state
    if state.requested_stop is None or state.pending_order_id is None:return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)
    return ProtectionState(direction=state.direction,confirmed_stop=state.requested_stop,confirmed_order_id=state.pending_order_id)

def replacement_uncertain(state:ProtectionState)->ProtectionState:
    if state.status not in {ProtectionStatus.REQUESTED,ProtectionStatus.STOP_REPLACEMENT_PENDING}:return state
    return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)

def reconcile_exchange_stop(state:ProtectionState,*,stop:float,order_id:str)->ProtectionState:
    return ProtectionState(direction=state.direction,confirmed_stop=float(stop),confirmed_order_id=str(order_id),status=ProtectionStatus.CONFIRMED)

__all__=["ProtectionState","ProtectionStatus","ProtectionTransition","validate_stop_transition","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
