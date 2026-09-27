"""Incident Telegram presentation helpers."""
from __future__ import annotations
from typing import Iterable,Mapping,Any

def incident_line(code,severity,status): return f"{severity} · {code} · {status}"

def format_incidents(items:Iterable[Mapping[str,Any]])->str:
    rows=list(items)
    if not rows:return "Активных инцидентов нет"
    lines=["<b>Активные инциденты</b>"]
    for item in rows[:20]:
        seen=str(item.get("last_seen") or "")[:19]
        lines.append(f"{item.get('severity','UNKNOWN')} · {item.get('code','UNKNOWN')} · {item.get('component','unknown')} · x{int(item.get('count') or 1)} · {seen}")
    return "\n".join(lines)
__all__=["format_incidents","incident_line"]
