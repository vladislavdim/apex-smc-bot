"""Learning-side execution quality observations from real fills only."""
from __future__ import annotations
from apex.execution.execution_quality import slippage_bps

def observed_slippage(expected,filled,*,confirmed:bool):
    if not confirmed: raise ValueError("execution_quality_requires_confirmed_fill")
    return slippage_bps(expected,filled)
__all__=["observed_slippage","slippage_bps"]
