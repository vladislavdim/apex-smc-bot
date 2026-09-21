"""Fail-closed entry kill switch."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class KillSwitch:
    active: bool = False
    reason: str = ""

    def allow_new_entries(self) -> bool:
        return not self.active


__all__ = ["KillSwitch"]
