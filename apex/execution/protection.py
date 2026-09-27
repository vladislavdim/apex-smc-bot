"""Exchange protection mechanics owned by Execution; Manager only requests eligible actions."""
from __future__ import annotations
from dataclasses import dataclass,replace
from enum import Enum
from apex.domain.enums import Direction

class ProtectionStatus(str,Enum):
    CONFIRMED="CONFIRMED"; REQUESTED="REQUESTED"; STOP_REPLACEMENT_PENDING="STOP_REPLACEMENT_PENDING"; RECONCILE_REQUIRED="RECONCILE_REQUIRED"

@dataclass(frozen=True)
class ProtectionRequest:
    """Pure stop-improvement check used before any exchange mutation."""
    direction: Direction
    current_price: float
    confirmed_stop: float
    proposed_stop: float

def valid_stop_replacement(req:ProtectionRequest)->bool:
    if req.direction is Direction.LONG:
        return float(req.confirmed_stop) < float(req.proposed_stop) < float(req.current_price)
    return float(req.current_price) < float(req.proposed_stop) < float(req.confirmed_stop)

@dataclass(frozen=True)
class StopProtectionRequest:
    """Execution command for an already-eligible confirmed position."""
    position_id:str
    symbol:str
    stop_price:float
    quantity:float
    def __post_init__(self):
        if not self.position_id or not self.symbol: raise ValueError("protection_identity_missing")
        if float(self.stop_price)<=0 or float(self.quantity)<=0: raise ValueError("protection_values_invalid")

@dataclass(frozen=True)
class ProtectionState:
    direction:Direction; confirmed_stop:float; confirmed_order_id:str
    proposed_stop:float|None=None; requested_stop:float|None=None; pending_order_id:str|None=None; old_order_id:str|None=None
    status:ProtectionStatus=ProtectionStatus.CONFIRMED

@dataclass(frozen=True)
class ProtectionResult:
    accepted:bool; confirmed_stop:float|None=None; order_id:str|None=None; detail:str=""

def propose(state,level,*,current_price,structural):
    if not structural:return state
    req=ProtectionRequest(state.direction,float(current_price),float(state.confirmed_stop),float(level))
    return replace(state,proposed_stop=level) if valid_stop_replacement(req) else state

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
    if state.status not in {ProtectionStatus.REQUESTED,ProtectionStatus.STOP_REPLACEMENT_PENDING}:return state
    return replace(state,status=ProtectionStatus.RECONCILE_REQUIRED)

def reconcile_exchange_stop(state,*,stop,order_id):
    return ProtectionState(direction=state.direction,confirmed_stop=float(stop),confirmed_order_id=str(order_id))

__all__=["ProtectionRequest","StopProtectionRequest","ProtectionResult","ProtectionState","ProtectionStatus","valid_stop_replacement","new_stop_accepted","old_stop_cancelled","propose","reconcile_exchange_stop","replacement_uncertain","request"]
