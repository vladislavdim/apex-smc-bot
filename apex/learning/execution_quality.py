"""Learning projection of confirmed execution quality; advisory only."""
from __future__ import annotations
from dataclasses import dataclass
from apex.execution.execution_quality import slippage_bps
@dataclass(frozen=True)
class ExecutionQualityObservation:
    expected_price: float
    filled_price: float
    slippage_bps: float

def observe(expected_price:float,filled_price:float)->ExecutionQualityObservation:
    return ExecutionQualityObservation(float(expected_price),float(filled_price),slippage_bps(expected_price,filled_price))
__all__=["ExecutionQualityObservation","observe","slippage_bps"]
