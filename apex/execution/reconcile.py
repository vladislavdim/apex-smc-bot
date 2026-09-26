"""Execution reconciliation contracts; Binance remains authoritative."""
from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class ReconciliationResult:
    ok: bool
    positions_seen: int=0
    orders_seen: int=0
    detail: str=""
__all__=["ReconciliationResult"]
