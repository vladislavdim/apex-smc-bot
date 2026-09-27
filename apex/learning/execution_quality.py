"""Learning projection of execution quality from confirmed live fills only."""
from __future__ import annotations
from apex.execution.execution_quality import slippage_bps

def live_slippage_bps(*,expected:float,filled:float,confirmed_fill:bool)->float|None:
    if not confirmed_fill:return None
    return slippage_bps(expected=expected,filled=filled)
__all__=["live_slippage_bps","slippage_bps"]
