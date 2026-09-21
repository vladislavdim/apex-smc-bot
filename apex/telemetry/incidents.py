"""Deduplicated persistent incident lifecycle."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from apex.domain.enums import IncidentSeverity
from apex.domain.ids import new_id


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _queue_notification(
    conn: sqlite3.Connection,
    incident_id: str,
    event_type: str,
    payload: Mapping[str, Any],
) -> None:
    event_key = (
        f"{incident_id}:{event_type}:{str(payload.get('severity') or '')}:"
        f"{int(payload.get('occurrence') or 1)}"
    )
    conn.execute(
        """INSERT OR IGNORE INTO incident_notifications(
               event_key,incident_id,event_type,payload_json
           ) VALUES(?,?,?,?)""",
        (event_key, incident_id, event_type, json.dumps(dict(payload), sort_keys=True, default=str)),
    )


def open_incident(
    conn: sqlite3.Connection,
    code: str,
    component: str,
    severity: IncidentSeverity | str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    level = severity if isinstance(severity, IncidentSeverity) else IncidentSeverity(str(severity).upper())
    row = conn.execute(
        "SELECT incident_id,count,severity FROM incidents WHERE code=? AND component=? AND resolved_at IS NULL",
        (str(code), str(component)),
    ).fetchone()
    now = _now()
    payload = json.dumps(dict(details or {}), sort_keys=True, default=str)
    if row:
        incident_id = str(row[0])
        conn.execute(
            "UPDATE incidents SET severity=?,last_seen=?,count=count+1,details_json=? WHERE incident_id=?",
            (level.value, now, payload, incident_id),
        )
        opened = False
        severity_changed = str(row[2]) != level.value
    else:
        incident_id = new_id("incident")
        conn.execute(
            """INSERT INTO incidents(incident_id,code,severity,component,started_at,last_seen,details_json)
               VALUES(?,?,?,?,?,?,?)""",
            (incident_id, str(code), level.value, str(component), now, now, payload),
        )
        opened = True
        severity_changed = False
    if opened or severity_changed:
        _queue_notification(
            conn,
            incident_id,
            "OPENED" if opened else "SEVERITY_CHANGED",
            {
                "code": str(code), "component": str(component),
                "severity": level.value, "details": dict(details or {}),
                "occurrence": int(row[1]) + 1 if row else 1,
            },
        )
    conn.commit()
    return {
        "incident_id": incident_id,
        "opened": opened,
        "severity": level.value,
        "severity_changed": severity_changed,
        "notify": opened or severity_changed,
    }


def resolve_incident(conn: sqlite3.Connection, code: str, component: str) -> bool:
    row = conn.execute(
        "SELECT incident_id,severity,count FROM incidents WHERE code=? AND component=? AND resolved_at IS NULL",
        (str(code), str(component)),
    ).fetchone()
    cursor = conn.execute(
        "UPDATE incidents SET resolved_at=?,last_seen=? WHERE code=? AND component=? AND resolved_at IS NULL",
        (_now(), _now(), str(code), str(component)),
    )
    if row is not None:
        _queue_notification(
            conn, str(row[0]), "RESOLVED",
            {
                "code": str(code), "component": str(component),
                "severity": str(row[1]), "occurrence": int(row[2]),
            },
        )
    conn.commit()
    return bool(cursor.rowcount)


def active_incidents(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM incidents WHERE resolved_at IS NULL ORDER BY started_at"
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        try:
            item["details"] = json.loads(item.pop("details_json"))
        except (TypeError, ValueError):
            item["details"] = {}
        result.append(item)
    return result


_CONN_FACTORY: Callable[[], sqlite3.Connection] | None = None


def configure_incidents(conn_factory: Callable[[], sqlite3.Connection] | None) -> None:
    """Configure the state-DB boundary without creating import side effects."""
    global _CONN_FACTORY
    _CONN_FACTORY = conn_factory


def report_incident(
    code: str,
    component: str,
    severity: IncidentSeverity | str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if _CONN_FACTORY is None:
        return {"configured": False, "notify": False}
    conn = _CONN_FACTORY()
    try:
        return {"configured": True, **open_incident(conn, code, component, severity, details)}
    except Exception as exc:
        logging.warning("[Incidents] persist failed safely: %s", type(exc).__name__)
        return {"configured": True, "notify": False, "error": type(exc).__name__}
    finally:
        conn.close()


def recover_incident(code: str, component: str) -> bool:
    if _CONN_FACTORY is None:
        return False
    conn = _CONN_FACTORY()
    try:
        return resolve_incident(conn, code, component)
    except Exception as exc:
        logging.warning("[Incidents] resolution failed safely: %s", type(exc).__name__)
        return False
    finally:
        conn.close()


def current_incidents() -> list[dict[str, Any]]:
    """Return active incidents through the configured boundary, fail safely."""
    if _CONN_FACTORY is None:
        return []
    conn = _CONN_FACTORY()
    try:
        conn.row_factory = sqlite3.Row
        return active_incidents(conn)
    except Exception as exc:
        logging.warning("[Incidents] active read failed safely: %s", type(exc).__name__)
        return []
    finally:
        conn.close()


def pending_notifications(limit: int = 20) -> list[dict[str, Any]]:
    if _CONN_FACTORY is None:
        return []
    conn = _CONN_FACTORY()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT * FROM incident_notifications WHERE delivered_at IS NULL
                 ORDER BY notification_id LIMIT ?""",
            (max(1, min(int(limit), 100)),),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json"))
            result.append(item)
        return result
    except Exception as exc:
        logging.warning("[Incidents] notification read failed safely: %s", type(exc).__name__)
        return []
    finally:
        conn.close()


def mark_notification_delivered(notification_id: int) -> bool:
    if _CONN_FACTORY is None:
        return False
    conn = _CONN_FACTORY()
    try:
        cursor = conn.execute(
            """UPDATE incident_notifications SET delivered_at=?
                 WHERE notification_id=? AND delivered_at IS NULL""",
            (_now(), int(notification_id)),
        )
        conn.commit()
        return bool(cursor.rowcount)
    except Exception as exc:
        logging.warning("[Incidents] notification ack failed safely: %s", type(exc).__name__)
        return False
    finally:
        conn.close()


__all__ = [
    "active_incidents", "configure_incidents", "current_incidents",
    "mark_notification_delivered",
    "open_incident", "pending_notifications", "recover_incident",
    "report_incident", "resolve_incident",
]
