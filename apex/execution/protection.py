"""Exchange-confirmed protection mechanics owned by Execution.

Manager may decide that protection is eligible, but only Execution owns the
concrete stop-replacement lifecycle and reconciliation with Binance.
"""
from __future__ import annotations
from dataclasses import dataclass,replace
from enum import Enum
from apex.domain.enums import Direction

class ProtectionStatus(str,Enum):
    CONFIRMED="CONFIRMED"
    REQUESTED="REQUESTED"
    STOP_REPLACEMENT_PENDING="STOP_REPLACEMENT_PENDING"
    RECONCILE_REQUIRED="RECONCILE_REQUIRED"

@dataclass(frozen=True)
class ProtectionState:
    direction:Direction
    confirmed_stop:float
    confirmed_order_id:str
    proposed_stop:float|None=None
    requested_stop:float|None=None
    pending_order_id:str|None=None
    old_order_id:str|None=None
    status:ProtectionStatus=ProtectionStatus.CONFIRMED

@dataclass(frozen=True)
class StopProtectionRequest:
    position_id:str
    symbol:str
    stop_price:float
    quantity:float
    def __post_init__(self):
        if not str(self.position_id).strip(): raise ValueError("position_id_required")
        if not str(self.symbol).strip(): raise ValueError("symbol_required")
        if float(self.stop_price)<=0: raise ValueError("stop_price_invalid")
        if float(self.quantity)<=0: raise ValueError("quantity_invalid")

@dataclass(frozen=True)
class ProtectionRequest:
    direction:Direction
    current_price:float
    confirmed_stop:float
    proposed_stop:float

def valid_stop_replacement(r:ProtectionRequest)->bool:
    return (r.confirmed_stop<r.proposed_stop<r.current_price) if r.direction is Direction.LONG else (r.current_price<r.proposed_stop<r.confirmed_stop)

def propose(state:ProtectionState,level:float,*,current_price:float,structural:bool)->ProtectionState:
    if not structural:return state
    req=ProtectionRequest(state.direction,current_price,state.confirmed_stop,float(level))
    return replace(state,proposed_stop=float(level)) if valid_stop_replacement(req) else state

def request(state:ProtectionState)->ProtectionState:
    if state.proposed_stop is None or state.status is not ProtectionStatus.CONFIRMED:return state
    return replace(state,requested_stop=state.proposed_stop,proposed_stop=None,old_order_id=state.confirmed_order_id,status=ProtectionStatus.REQUESTED)

def new_stop_accepted(state:ProtectionState,new_order_id:str)->ProtectionState:
    if state.status is not ProtectionStatus.REQUESTED or not new_order_id:return state
    return replace(state,pending_order_id=str(new_order_id),status=ProtectionStatus.STOP_REPLACEMENT_PENDING)

def old_stop_cancelled(state:ProtectionState)->ProtectionState:
    if state.status is not ProtectionStatus.STOP_REPLACEMENT_PENDING:return state
    if state.requested_stop is None or state.pending_order_id is None:return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)
    return ProtectionState(direction=state.direction,confirmed_stop=state.requested_stop,confirmed_order_id=state.pending_order_id)

def replacement_uncertain(state:ProtectionState)->ProtectionState:
    if state.status not in {ProtectionStatus.REQUESTED,ProtectionStatus.STOP_REPLACEMENT_PENDING}:return state
    return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)

def reconcile_exchange_stop(state:ProtectionState,*,stop:float,order_id:str)->ProtectionState:
    return ProtectionState(direction=state.direction,confirmed_stop=float(stop),confirmed_order_id=str(order_id),status=ProtectionStatus.CONFIRMED)

__all__=["StopProtectionRequest","ProtectionRequest","ProtectionState","ProtectionStatus","valid_stop_replacement","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
