"""Manager decision lifecycle; exchange protection mechanics remain in Execution."""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
class ManagerStatus(str,Enum):
    ACTIVE="ACTIVE"; PROTECTING="PROTECTING"; PARTIAL="PARTIAL"; CLOSED="CLOSED"
@dataclass(frozen=True)
class ManagerState:
    position_id:str
    status:ManagerStatus=ManagerStatus.ACTIVE
    tp1_confirmed:bool=False
    remaining_quantity:float=0.0
__all__=["ManagerState","ManagerStatus"]
