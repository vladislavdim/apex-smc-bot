"""Read-only Telegram projection of persistent production incidents."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any, Mapping


def notification_batches(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Summarize a complete queued episode without suppressing active faults."""
    groups: dict[str, list] = {}
    for row in rows:
        groups.setdefault(str(row["incident_id"]), []).append(row)
    batches = []
    rank = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}
    for notifications in groups.values():
        last = notifications[-1]
        payload = dict(last.get("payload") or {})
        peak = max(notifications, key=lambda row: rank.get(
            str((row.get("payload") or {}).get("severity")), -1,
        ))
        payload["severity"] = (peak.get("payload") or {}).get("severity", "UNKNOWN")
        payload["details"] = (peak.get("payload") or {}).get("details", {})
        events = {row.get("event_type") for row in notifications}
        recovered = "OPENED" in events and ("RESOLVED" in events or bool(last.get("resolved_at")))
        batches.append({
            **last, "payload": payload,
            "event_type": "RECOVERED" if recovered else last.get("event_type"),
            "notification_ids": [int(row["notification_id"]) for row in notifications],
        })
    return batches


def format_notification(notification: Mapping[str, Any]) -> str:
    payload = notification.get("payload") or {}
    event = str(notification.get("event_type") or "INCIDENT")
    recovered = event in {"RESOLVED", "RECOVERED"}
    title = "Сбой уже устранён" if event == "RECOVERED" else event
    lines = [
        f"{'✅' if recovered else '🚨'} APEX INCIDENT · {title}",
        f"{payload.get('severity', 'UNKNOWN')} · {payload.get('component', 'system')}",
        str(payload.get("code") or "UNKNOWN"),
    ]
    started = notification.get("started_at")
    resolved = notification.get("resolved_at")
    if started:
        lines.append(f"Начало: {started} (UTC)")
    if recovered and resolved:
        lines.append(f"Восстановление компонента: {resolved} (UTC)")
        if started:
            try:
                duration = datetime.fromisoformat(str(resolved)) - datetime.fromisoformat(str(started))
                lines.append(f"Длительность: {max(0, int(duration.total_seconds()))} с")
            except (TypeError, ValueError):
                pass
    details = payload.get("details")
    if isinstance(details, dict) and details:
        lines.append(", ".join(f"{key}={value}" for key, value in sorted(details.items()))[:500])
    return "\n".join(lines)


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


__all__ = ["format_incidents", "format_notification", "notification_batches"]
