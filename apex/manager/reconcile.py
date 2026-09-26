"""Manager reconciliation state projected from confirmed exchange positions."""
from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class ManagerReconciliation:
    position_id: str
    confirmed: bool
    reason: str=""
__all__=["ManagerReconciliation"]
