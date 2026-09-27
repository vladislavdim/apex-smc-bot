"""Exchange-protection primitives owned by the Execution layer.
Manager may request an eligible improvement; only Execution applies it.
"""
from __future__ import annotations
from dataclasses import dataclass
from apex.domain.enums import Direction
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
__all__=["ProtectionRequest","is_improvement","require_improvement"]
