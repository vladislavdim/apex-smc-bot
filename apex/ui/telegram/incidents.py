"""Read-only Telegram projection of persistent production incidents."""
from __future__ import annotations
from collections.abc import Iterable
from typing import Any,Mapping

def incident_line(code:str,status:str,severity:str|None=None)->str:
    parts=[str(code),str(status)]
    if severity: parts.append(str(severity))
    return " · ".join(parts)

def format_incidents(rows:Iterable[Mapping[str,Any]])->str:
    incidents=list(rows); lines=["⚠️ <b>INCIDENTS</b>","━━━━━━━━━━━━━━━━━━━━"]
    if not incidents: lines.append("Активных инцидентов нет.")
    for row in incidents[:20]:
        severity=str(row.get("severity") or "WARNING").upper(); icon="🔴" if severity=="CRITICAL" else "🟠" if severity=="ERROR" else "🟡"
        lines.append(f"{icon} <b>{row.get('code') or 'UNKNOWN'}</b> · {row.get('component') or 'system'} · {severity} · x{int(row.get('count') or 1)}")
        if row.get("last_seen"): lines.append(f"  last_seen: {str(row.get('last_seen'))[:19]} UTC")
    lines.extend(["","Инциденты дедуплицируются и не изменяют торговые решения."])
    return "\n".join(lines)
__all__=["format_incidents","incident_line"]
