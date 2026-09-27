"""Execution-side protection contracts.
Exchange order mechanics live here; Manager decides *when* an eligible protection action is requested.
"""
from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class ProtectionRequest:
    position_id:str
    symbol:str
    proposed_stop:float
    reason:str
    def __post_init__(self):
        if not self.position_id or not self.symbol: raise ValueError("protection_identity_missing")
@dataclass(frozen=True)
class ProtectionResult:
    accepted:bool
    confirmed_stop:float|None=None
    order_id:str|None=None
    detail:str=""
__all__=["ProtectionRequest","ProtectionResult"]
