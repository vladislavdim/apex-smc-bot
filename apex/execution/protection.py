"""Execution protection primitives.

This module owns exchange-protection state. Manager may request an eligible
protection action, but execution validates the stop transition before submission.
"""
from __future__ import annotations
from dataclasses import dataclass
from apex.domain.enums import Direction

@dataclass(frozen=True)
class ProtectionTransition:
    current_stop: float
    proposed_stop: float
    accepted: bool
    reason: str

def validate_stop_transition(*,direction:Direction,current_stop:float,proposed_stop:float,current_price:float)->ProtectionTransition:
    if direction is Direction.LONG:
        ok=current_stop < proposed_stop < current_price
    else:
        ok=current_price < proposed_stop < current_stop
    return ProtectionTransition(float(current_stop),float(proposed_stop),bool(ok),"OK" if ok else "NON_PROTECTIVE_STOP")

__all__=["ProtectionTransition","validate_stop_transition"]
