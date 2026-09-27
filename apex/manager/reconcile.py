"""Manager reconciliation state derived only from exchange-confirmed positions."""
from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class ManagerReconciliation:
    position_id:str
    confirmed:bool
    reason:str=""

def require_confirmed_position(result:ManagerReconciliation)->None:
    if not result.confirmed: raise RuntimeError("MANAGER_REQUIRES_CONFIRMED_POSITION")
__all__=["ManagerReconciliation","require_confirmed_position"]
