"""Exchange-protection primitives owned by Execution.
Manager may request protection, but only Execution may translate it into exchange orders.
"""
from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class ProtectionRequest:
    position_id:str
    stop_price:float
    reason:str
    def __post_init__(self):
        if not self.position_id: raise ValueError("position_id_required")
        if self.stop_price<=0: raise ValueError("stop_price_must_be_positive")
__all__=["ProtectionRequest"]
