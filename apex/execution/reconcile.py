"""Execution reconciliation contracts; Binance is execution truth."""
from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class ReconciliationResult:
    ok:bool
    positions_seen:int=0
    orders_seen:int=0
    detail:str=""

def require_success(result:ReconciliationResult)->None:
    if not result.ok: raise RuntimeError(result.detail or "BINANCE_RECONCILIATION_FAILED")
__all__=["ReconciliationResult","require_success"]
