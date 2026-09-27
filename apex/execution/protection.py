"""Execution-owned protection primitives.

This layer validates protective-stop changes. Manager may propose an action,
but only Execution owns the exchange protection boundary.
"""
from __future__ import annotations
from dataclasses import dataclass
from apex.domain.enums import Direction

@dataclass(frozen=True)
class ProtectionChange:
    current_stop: float
    proposed_stop: float
    current_price: float
    direction: Direction

def improves(change: ProtectionChange) -> bool:
    if change.direction is Direction.LONG:
        return change.current_stop < change.proposed_stop < change.current_price
    return change.current_price < change.proposed_stop < change.current_stop

def require_improvement(change: ProtectionChange) -> ProtectionChange:
    if not improves(change):
        raise ValueError("PROTECTION_MUST_IMPROVE")
    return change

__all__=["ProtectionChange","improves","require_improvement"]
