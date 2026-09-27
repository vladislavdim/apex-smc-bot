"""System Telegram presentation helpers kept separate from runtime ownership."""
from __future__ import annotations

def readiness_line(status:str,entries_allowed:bool)->str:
    return f"System {status} · new entries {'ON' if entries_allowed else 'OFF'}"
__all__=["readiness_line"]
