"""Execution-owned exchange protection state machine.

Binance-confirmed stop state is authoritative. Manager may propose a safer
level, but replacement/cancel/reconciliation mechanics belong to Execution.
"""
from __future__ import annotations
from dataclasses import dataclass,replace
from enum import Enum
from apex.domain.enums import Direction
class ProtectionStatus(str,Enum):
    CONFIRMED="CONFIRMED"; REQUESTED="REQUESTED"; STOP_REPLACEMENT_PENDING="STOP_REPLACEMENT_PENDING"; RECONCILE_REQUIRED="RECONCILE_REQUIRED"
@dataclass(frozen=True)
class ProtectionState:
    direction:Direction; confirmed_stop:float; confirmed_order_id:str
    proposed_stop:float|None=None; requested_stop:float|None=None; pending_order_id:str|None=None; old_order_id:str|None=None
    status:ProtectionStatus=ProtectionStatus.CONFIRMED
def propose(state,level,*,current_price,structural):
    if not structural:return state
    improves=state.confirmed_stop<level<current_price if state.direction is Direction.LONG else current_price<level<state.confirmed_stop
    return replace(state,proposed_stop=level) if improves else state
def request(state):
    if state.proposed_stop is None or state.status is not ProtectionStatus.CONFIRMED:return state
    return replace(state,requested_stop=state.proposed_stop,proposed_stop=None,old_order_id=state.confirmed_order_id,status=ProtectionStatus.REQUESTED)
def new_stop_accepted(state,new_order_id):
    return replace(state,pending_order_id=new_order_id,status=ProtectionStatus.STOP_REPLACEMENT_PENDING) if state.status is ProtectionStatus.REQUESTED and new_order_id else state
def old_stop_cancelled(state):
    if state.status is not ProtectionStatus.STOP_REPLACEMENT_PENDING:return state
    if state.requested_stop is None or state.pending_order_id is None:return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)
    return ProtectionState(direction=state.direction,confirmed_stop=state.requested_stop,confirmed_order_id=state.pending_order_id)
def replacement_uncertain(state):
    return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED) if state.status in {ProtectionStatus.REQUESTED,ProtectionStatus.STOP_REPLACEMENT_PENDING} else state
def reconcile_exchange_stop(state,*,stop,order_id):
    return ProtectionState(direction=state.direction,confirmed_stop=float(stop),confirmed_order_id=str(order_id),status=ProtectionStatus.CONFIRMED)
__all__=["ProtectionState","ProtectionStatus","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]


@dataclass(frozen=True)
class ProtectionRequest:
    direction: Direction
    current_stop: float
    proposed_stop: float
    current_price: float

def is_improvement(req:ProtectionRequest)->bool:
    if req.direction is Direction.LONG:
        return req.current_stop < req.proposed_stop < req.current_price
    return req.current_price < req.proposed_stop < req.current_stop

def require_improvement(req:ProtectionRequest)->ProtectionRequest:
    if not is_improvement(req): raise ValueError("PROTECTION_NOT_IMPROVING")
    return req

__all__ += ['ProtectionRequest', 'is_improvement', 'require_improvement']
