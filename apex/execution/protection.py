"""Execution protection boundary.

Protection mechanics belong to Execution. Manager decides eligibility/action;
Execution validates the concrete stop replacement before exchange submission.
"""
from __future__ import annotations
from dataclasses import dataclass
from apex.domain.enums import Direction

@dataclass(frozen=True)
class ProtectionRequest:
    direction: Direction
    current_price: float
    confirmed_stop: float
    proposed_stop: float

def valid_stop_replacement(request: ProtectionRequest) -> bool:
    if request.direction is Direction.LONG:
        return request.confirmed_stop < request.proposed_stop < request.current_price
    return request.current_price < request.proposed_stop < request.confirmed_stop

__all__=["ProtectionRequest","valid_stop_replacement"]
