"""Execution-owned exchange protection primitives.
Manager decides eligibility; Execution validates and applies stop geometry.
"""
from __future__ import annotations
from dataclasses import dataclass,replace
from enum import Enum
from apex.domain.enums import Direction

class ProtectionStatus(str,Enum):
    CONFIRMED="CONFIRMED"; PROPOSED="PROPOSED"; REQUESTED="REQUESTED"; NEW_ACCEPTED="NEW_ACCEPTED"; RECONCILE_REQUIRED="RECONCILE_REQUIRED"

@dataclass(frozen=True)
class ProtectionState:
    direction:Direction
    confirmed_stop:float
    confirmed_order_id:str
    requested_stop:float|None=None
    pending_order_id:str|None=None
    old_order_id:str|None=None
    status:ProtectionStatus=ProtectionStatus.CONFIRMED

@dataclass(frozen=True)
class ProtectionRequest:
    direction:Direction
    current_price:float
    current_stop:float
    new_stop:float

@dataclass(frozen=True)
class StopProtectionRequest:
    position_id:str
    symbol:str
    stop_price:float
    quantity:float
    def __post_init__(self):
        if not self.position_id or not self.symbol: raise ValueError("protection_identity_required")
        if float(self.stop_price)<=0 or float(self.quantity)<=0: raise ValueError("protection_values_must_be_positive")

def stop_is_protective(direction:Direction,current_stop:float,new_stop:float,current_price:float)->bool:
    if direction is Direction.LONG: return current_stop < new_stop < current_price
    return current_price < new_stop < current_stop

def valid_stop_replacement(req:ProtectionRequest)->bool:
    return stop_is_protective(req.direction,float(req.current_stop),float(req.new_stop),float(req.current_price))

def require_protective_stop(direction:Direction,current_stop:float,new_stop:float,current_price:float)->float:
    value=float(new_stop)
    if not stop_is_protective(direction,float(current_stop),value,float(current_price)): raise ValueError("NON_PROTECTIVE_STOP_FORBIDDEN")
    return value

def propose(state:ProtectionState,new_stop:float,*,current_price:float,structural:bool=False)->ProtectionState:
    if not structural or not stop_is_protective(state.direction,state.confirmed_stop,float(new_stop),float(current_price)): raise ValueError("NON_PROTECTIVE_STOP_FORBIDDEN")
    return replace(state,requested_stop=float(new_stop),status=ProtectionStatus.PROPOSED)

def request(state:ProtectionState)->ProtectionState:
    if state.status is not ProtectionStatus.PROPOSED or state.requested_stop is None: raise ValueError("PROTECTION_NOT_PROPOSED")
    return replace(state,status=ProtectionStatus.REQUESTED)

def new_stop_accepted(state:ProtectionState,order_id:str)->ProtectionState:
    if state.status is not ProtectionStatus.REQUESTED or not order_id: raise ValueError("PROTECTION_NOT_REQUESTED")
    return replace(state,pending_order_id=str(order_id),old_order_id=state.confirmed_order_id,status=ProtectionStatus.NEW_ACCEPTED)

def old_stop_cancelled(state:ProtectionState)->ProtectionState:
    if state.status is not ProtectionStatus.NEW_ACCEPTED or state.pending_order_id is None or state.requested_stop is None: raise ValueError("NEW_STOP_NOT_ACCEPTED")
    return replace(state,confirmed_stop=state.requested_stop,confirmed_order_id=state.pending_order_id,requested_stop=None,pending_order_id=None,old_order_id=None,status=ProtectionStatus.CONFIRMED)

def replacement_uncertain(state:ProtectionState)->ProtectionState:
    if state.status is not ProtectionStatus.NEW_ACCEPTED: raise ValueError("NO_REPLACEMENT_TO_RECONCILE")
    return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)

def reconcile_exchange_stop(state:ProtectionState,*,stop:float,order_id:str)->ProtectionState:
    if state.status is not ProtectionStatus.RECONCILE_REQUIRED or not order_id: raise ValueError("RECONCILIATION_NOT_REQUIRED")
    return replace(state,confirmed_stop=float(stop),confirmed_order_id=str(order_id),requested_stop=None,pending_order_id=None,old_order_id=None,status=ProtectionStatus.CONFIRMED)

__all__=["ProtectionRequest","StopProtectionRequest","ProtectionState","ProtectionStatus","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request","require_protective_stop","stop_is_protective","valid_stop_replacement"]
