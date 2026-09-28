"""Canonical Manager event vocabulary."""
from enum import Enum
class ManagerEvent(str,Enum):
    POSITION_CONFIRMED="POSITION_CONFIRMED"
    TP1_CONFIRMED="TP1_CONFIRMED"
    STRUCTURE_CHANGED="STRUCTURE_CHANGED"
    INVALIDATED="INVALIDATED"
    POSITION_CLOSED="POSITION_CLOSED"
__all__=["ManagerEvent"]
