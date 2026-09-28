"""Fail-closed V3 entry kill switch."""
from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class KillSwitch:
    enabled: bool=False
    reason: str=""
    @property
    def entries_allowed(self)->bool: return not self.enabled
__all__=["KillSwitch"]
