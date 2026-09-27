"""Execution-owned protective-order state machine.

Manager decides *when* protection is eligible; Execution owns the mechanics and
state transitions for confirmed protective orders.
"""
from __future__ import annotations
from dataclasses import dataclass,replace

@dataclass(frozen=True)
class ProtectionState:
    confirmed_stop: float
    pending_stop: float|None=None
    pending_order_id: str|None=None

def request_protection(state:ProtectionState,new_stop:float)->ProtectionState:
    return replace(state,pending_stop=float(new_stop),pending_order_id=None)

def protection_submitted(state:ProtectionState,order_id:str)->ProtectionState:
    if state.pending_stop is None: raise RuntimeError("protection_not_requested")
    return replace(state,pending_order_id=str(order_id))

def protection_confirmed(state:ProtectionState)->ProtectionState:
    if state.pending_stop is None or not state.pending_order_id: raise RuntimeError("protection_not_submitted")
    return ProtectionState(confirmed_stop=state.pending_stop)

def protection_failed(state:ProtectionState)->ProtectionState:
    return replace(state,pending_stop=None,pending_order_id=None)

__all__=["ProtectionState","request_protection","protection_submitted","protection_confirmed","protection_failed"]
