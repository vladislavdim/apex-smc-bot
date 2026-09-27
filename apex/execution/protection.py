"""Execution protection boundary.

The Manager decides whether protection is eligible; this module owns only the
exchange-facing stop replacement request shape. It deliberately does not
import Manager internals, preserving dependency direction.
"""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class StopProtectionRequest:
    position_id:str
    symbol:str
    stop_price:float
    quantity:float
    def __post_init__(self)->None:
        if not self.position_id or not self.symbol: raise ValueError("protection_identity_missing")
        if self.stop_price<=0 or self.quantity<=0: raise ValueError("invalid_protection_geometry")

__all__=["StopProtectionRequest"]
