"""Read-only Telegram projection of persistent production incidents."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Mapping


def format_incidents(rows: Iterable[Mapping[str, Any]]) -> str:
    incidents = list(rows)
    lines = ["⚠️ <b>INCIDENTS</b>", "━━━━━━━━━━━━━━━━━━━━"]
    if not incidents:
        lines.append("Активных инцидентов нет.")
    for row in incidents[:20]:
        severity = str(row.get("severity") or "WARNING").upper()
        icon = "🔴" if severity == "CRITICAL" else "🟠" if severity == "ERROR" else "🟡"
        lines.append(
            f"{icon} <b>{row.get('code') or 'UNKNOWN'}</b> · "
            f"{row.get('component') or 'system'} · {severity} · x{int(row.get('count') or 1)}"
        )
        last_seen = row.get("last_seen")
        if last_seen:
            lines.append(f"  last_seen: {str(last_seen)[:19]} UTC")
    lines.extend(["", "Инциденты дедуплицируются и не изменяют торговые решения."])
    return "\n".join(lines)


__all__ = ["format_incidents"]
