"""Secret-free Telegram projection of the canonical production runtime."""

from __future__ import annotations

import html
from typing import Any, Mapping


_ORDER = (
    "config", "state_db", "memory_db", "gate", "market_data", "groq",
    "binance_reconciliation", "manager", "manager_reconciliation",
    "scheduler", "telegram", "dashboard_telemetry", "backup",
    "instance_fencing", "restart_guard", "cpu", "memory",
)

_LABELS = {
    "config": "Config", "state_db": "State DB", "memory_db": "Memory DB",
    "gate": "Gate", "market_data": "Market data", "groq": "Groq",
    "binance_reconciliation": "Binance", "manager": "Manager",
    "manager_reconciliation": "Manager reconcile", "scheduler": "Scheduler",
    "telegram": "Telegram", "dashboard_telemetry": "Dashboard",
    "backup": "Backup", "instance_fencing": "Fencing",
    "restart_guard": "Restart guard", "cpu": "CPU", "memory": "Memory",
}


def _icon(state: str) -> str:
    value = state.upper()
    if value in {"READY", "FRESH", "HEALTHY"}:
        return "✅"
    if value in {"STARTING", "UNKNOWN"}:
        return "⚪"
    if value in {"DEGRADED", "STALE"}:
        return "🟡"
    return "🔴"


def format_system_status(snapshot: Mapping[str, Any]) -> str:
    """Render one canonical runtime snapshot without probing services again."""
    status = str(snapshot.get("status") or "UNKNOWN").upper()
    health = str(snapshot.get("health") or "UNKNOWN").upper()
    ready = snapshot.get("ready") is True
    entries = str(snapshot.get("new_entries") or ("ON" if ready else "OFF"))
    release = html.escape(str(snapshot.get("release_sha") or "unknown")[:12])
    components = snapshot.get("components")
    components = components if isinstance(components, Mapping) else {}

    lines = [
        "🛡 <b>Система APEX</b>", "━━━━━━━━━━━━━━━━━━━━",
        f"{_icon(health)} Runtime: <b>{html.escape(status)}</b> · health {html.escape(health)}",
        f"{'✅' if ready else '⛔'} Новые входы: <b>{html.escape(entries)}</b>",
        f"📦 Release: <code>{release}</code>",
        f"🔐 Fencing generation: <b>{snapshot.get('fencing_generation') if snapshot.get('fencing_generation') is not None else '—'}</b>",
        "", "<b>Подключения и компоненты</b>",
    ]
    for name in _ORDER:
        item = components.get(name)
        item = item if isinstance(item, Mapping) else {}
        state = str(item.get("state") or "UNKNOWN").upper()
        lines.append(f"{_icon(state)} {html.escape(_LABELS.get(name, name))}: <b>{html.escape(state)}</b>")

    reasons = snapshot.get("reason_codes")
    reasons = list(reasons) if isinstance(reasons, (list, tuple, set)) else []
    lines.extend(["", "<b>Причины блокировки</b>"])
    lines.extend(
        [f"• <code>{html.escape(str(reason))}</code>" for reason in reasons[:8]]
        if reasons else ["Нет."]
    )
    return "\n".join(lines)[:4000]


__all__ = ["format_system_status"]
