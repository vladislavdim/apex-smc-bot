"""Execution-owned protection primitives for confirmed Binance positions.

Manager decides when protection is eligible; Execution owns exchange-facing
stop geometry. Candidate Entry/SL/TP/RR remain immutable.
"""
from __future__ import annotations
from dataclasses import dataclass
from apex.domain.enums import Direction
@dataclass(frozen=True)
class ProtectionChange:
    current_stop: float
    proposed_stop: float
    accepted: bool
    reason: str

def validate_stop_improvement(*,direction:Direction,current_price:float,current_stop:float,proposed_stop:float)->ProtectionChange:
    ok=(current_stop < proposed_stop < current_price) if direction is Direction.LONG else (current_price < proposed_stop < current_stop)
    return ProtectionChange(float(current_stop),float(proposed_stop),bool(ok),"OK" if ok else "NON_IMPROVING_STOP")
__all__=["ProtectionChange","validate_stop_improvement"]
