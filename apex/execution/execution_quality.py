"""Execution-quality metrics derived only from confirmed fills."""
from __future__ import annotations

def slippage_bps(*,planned:float,filled:float)->float|None:
    if planned<=0 or filled<=0: return None
    return (float(filled)-float(planned))/float(planned)*10000.0
__all__=["slippage_bps"]
