"""Incident Telegram presentation boundary."""
from __future__ import annotations

def incident_line(code:str,status:str,severity:str|None=None)->str:
    parts=[str(code),str(status)]
    if severity: parts.append(str(severity))
    return " · ".join(parts)
__all__=["incident_line"]
