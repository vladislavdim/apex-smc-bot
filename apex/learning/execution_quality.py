"""Execution-quality learning metrics from confirmed live fills only."""
from apex.execution.execution_quality import slippage_bps

def confirmed_slippage_bps(*,planned_price,fill_price,confirmed:bool):
    if not confirmed: raise ValueError("unconfirmed_fill_forbidden")
    return slippage_bps(planned_price,fill_price)
__all__=["confirmed_slippage_bps","slippage_bps"]
