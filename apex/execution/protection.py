"""Exchange-confirmed protection state owned by Execution."""
from __future__ import annotations
from dataclasses import dataclass,replace
from enum import Enum
from apex.domain.enums import Direction

class ProtectionStatus(str,Enum):
    CONFIRMED="CONFIRMED"; REQUESTED="REQUESTED"; STOP_REPLACEMENT_PENDING="STOP_REPLACEMENT_PENDING"; RECONCILE_REQUIRED="RECONCILE_REQUIRED"

@dataclass(frozen=True)
class ProtectionRequest:
    direction:Direction
    current_price:float
    confirmed_stop:float
    proposed_stop:float

@dataclass(frozen=True)
class StopProtectionRequest:
    position_id:str
    symbol:str
    stop_price:float
    quantity:float
    def __post_init__(self):
        if not str(self.position_id).strip(): raise ValueError("position_id_required")
        if not str(self.symbol).strip(): raise ValueError("symbol_required")
        if float(self.stop_price)<=0 or float(self.quantity)<=0: raise ValueError("positive_stop_and_quantity_required")

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

def valid_stop_replacement(state_or_request,level=None,*,current_price=None,structural=True)->bool:
    if not structural:return False
    if isinstance(state_or_request,ProtectionRequest):
        direction=state_or_request.direction; confirmed=state_or_request.confirmed_stop; proposed=state_or_request.proposed_stop; market=state_or_request.current_price
    else:
        direction=state_or_request.direction; confirmed=state_or_request.confirmed_stop; proposed=level; market=current_price
    if proposed is None or market is None:return False
    return confirmed<proposed<market if direction is Direction.LONG else market<proposed<confirmed

def propose(state,level,*,current_price,structural):
    return replace(state,proposed_stop=level) if valid_stop_replacement(state,level,current_price=current_price,structural=structural) else state

def request(state):
    if state.proposed_stop is None or state.status is not ProtectionStatus.CONFIRMED:return state
    return replace(state,requested_stop=state.proposed_stop,proposed_stop=None,old_order_id=state.confirmed_order_id,status=ProtectionStatus.REQUESTED)

def new_stop_accepted(state,new_order_id):
    if state.status is not ProtectionStatus.REQUESTED or not new_order_id:return state
    return replace(state,pending_order_id=new_order_id,status=ProtectionStatus.STOP_REPLACEMENT_PENDING)

def old_stop_cancelled(state):
    if state.status is not ProtectionStatus.STOP_REPLACEMENT_PENDING:return state
    if state.requested_stop is None or state.pending_order_id is None:return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)
    return ProtectionState(direction=state.direction,confirmed_stop=state.requested_stop,confirmed_order_id=state.pending_order_id)

def replacement_uncertain(state):
    return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED) if state.status in {ProtectionStatus.REQUESTED,ProtectionStatus.STOP_REPLACEMENT_PENDING} else state

def reconcile_exchange_stop(state,*,stop,order_id):
    return ProtectionState(direction=state.direction,confirmed_stop=float(stop),confirmed_order_id=str(order_id),status=ProtectionStatus.CONFIRMED)

__all__=["ProtectionRequest","StopProtectionRequest","ProtectionState","ProtectionStatus","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request","valid_stop_replacement"]
