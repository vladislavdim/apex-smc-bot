"""Execution-owned protective-stop state and validation for APEX V3."""
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

@dataclass(frozen=True)
class ProtectionRequest:
    direction:Direction; current_price:float; current_stop:float; proposed_stop:float

@dataclass(frozen=True)
class StopProtectionRequest:
    position_id:str; symbol:str; stop_price:float; quantity:float
    def __post_init__(self):
        if not self.position_id or not self.symbol: raise ValueError("protection_identity_required")
        if float(self.stop_price)<=0 or float(self.quantity)<=0: raise ValueError("protection_values_must_be_positive")

ProtectionChange=ProtectionRequest

def valid_stop_replacement(x:ProtectionRequest)->bool:
    return (x.current_stop<x.proposed_stop<x.current_price) if x.direction is Direction.LONG else (x.current_price<x.proposed_stop<x.current_stop)
def improves(x:ProtectionRequest)->bool:return valid_stop_replacement(x)
def require_improvement(x:ProtectionRequest)->ProtectionRequest:
    if not valid_stop_replacement(x): raise ValueError("PROTECTION_MUST_IMPROVE")
    return x

def propose(state:ProtectionState,level:float,*,current_price:float,structural:bool)->ProtectionState:
    if not structural:return state
    req=ProtectionRequest(state.direction,current_price,state.confirmed_stop,level)
    return replace(state,proposed_stop=level) if valid_stop_replacement(req) else state

def request(state:ProtectionState)->ProtectionState:
    if state.proposed_stop is None or state.status is not ProtectionStatus.CONFIRMED:return state
    return replace(state,requested_stop=state.proposed_stop,proposed_stop=None,old_order_id=state.confirmed_order_id,status=ProtectionStatus.REQUESTED)
def new_stop_accepted(state:ProtectionState,new_order_id:str)->ProtectionState:
    if state.status is not ProtectionStatus.REQUESTED or not new_order_id:return state
    return replace(state,pending_order_id=new_order_id,status=ProtectionStatus.STOP_REPLACEMENT_PENDING)
def old_stop_cancelled(state:ProtectionState)->ProtectionState:
    if state.status is not ProtectionStatus.STOP_REPLACEMENT_PENDING:return state
    if state.requested_stop is None or state.pending_order_id is None:return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)
    return ProtectionState(state.direction,state.requested_stop,state.pending_order_id)
def replacement_uncertain(state:ProtectionState)->ProtectionState:
    if state.status not in {ProtectionStatus.REQUESTED,ProtectionStatus.STOP_REPLACEMENT_PENDING}:return state
    return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)
def reconcile_exchange_stop(state:ProtectionState,*,stop:float,order_id:str)->ProtectionState:
    return ProtectionState(state.direction,float(stop),str(order_id),status=ProtectionStatus.CONFIRMED)

__all__=["ProtectionChange","ProtectionRequest","StopProtectionRequest","ProtectionState","ProtectionStatus","improves","valid_stop_replacement","require_improvement","propose","request","new_stop_accepted","old_stop_cancelled","replacement_uncertain","reconcile_exchange_stop"]
