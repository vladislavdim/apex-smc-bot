"""Execution protection primitives.

Exchange protection mechanics live in Execution. Manager decides whether an
eligible protection action should be requested, but does not own order state.
"""
from __future__ import annotations
from dataclasses import dataclass
from apex.domain.enums import Direction

@dataclass(frozen=True)
class ProtectionRequest:
    position_id: str
    direction: Direction
    current_stop: float
    requested_stop: float
    current_price: float

def validate_protection(request: ProtectionRequest) -> None:
    if request.direction is Direction.LONG:
        valid=request.current_stop < request.requested_stop < request.current_price
    else:
        valid=request.current_price < request.requested_stop < request.current_stop
    if not valid: raise ValueError("PROTECTION_NOT_IMPROVING")

__all__=["ProtectionRequest","validate_protection"]
