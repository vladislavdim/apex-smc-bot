"""Execution protection boundary.

Protection state belongs to Execution. Manager may request an eligible
protection action, but confirmed exchange protection is represented here.
"""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class ProtectionState:
    position_id: str
    confirmed_stop: float
    order_id: str|None=None

def improves_stop(*,direction:str,current_stop:float,proposed_stop:float,current_price:float)->bool:
    side=str(direction).upper()
    if side in {"LONG","BULLISH","BUY"}:
        return current_stop < proposed_stop < current_price
    if side in {"SHORT","BEARISH","SELL"}:
        return current_price < proposed_stop < current_stop
    return False

__all__=["ProtectionState","improves_stop"]
