"""Small, decision-neutral telemetry projections for the public Dashboard."""
from __future__ import annotations

from typing import Any


_SEVERITIES = {"INFO", "WARNING", "ERROR", "CRITICAL"}


def normalize_incident_snapshot(raw: Any, *, limit: int = 50) -> list[dict[str, Any]]:
    """Bound and normalize worker-authored incident state for presentation.

    This projection has no trading authority. Unknown fields are intentionally
    discarded so a malformed detail payload cannot inflate Dashboard responses.
    """
    if not isinstance(raw, list):
        return []
    result: list[dict[str, Any]] = []
    for value in raw[: max(1, min(int(limit), 100))]:
        if not isinstance(value, dict):
            continue
        severity = str(value.get("severity") or "WARNING").upper()
        if severity not in _SEVERITIES:
            severity = "WARNING"
        details = value.get("details") if isinstance(value.get("details"), dict) else {}
        safe_details = {
            str(key)[:60]: str(item)[:240]
            for key, item in list(details.items())[:10]
        }
        result.append({
            "incident_id": str(value.get("incident_id") or "")[:80],
            "code": str(value.get("code") or "UNKNOWN")[:80],
            "severity": severity,
            "component": str(value.get("component") or "system")[:80],
            "started_at": str(value.get("started_at") or "")[:64],
            "last_seen": str(value.get("last_seen") or "")[:64],
            "count": max(1, int(value.get("count") or 1)),
            "details": safe_details,
        })
    return result


__all__ = ["normalize_incident_snapshot"]
