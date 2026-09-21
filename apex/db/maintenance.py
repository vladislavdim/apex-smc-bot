"""Bounded V3 database retention and low-impact WAL health checks."""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


def _cutoff(now: datetime, days: int) -> str:
    return (now.astimezone(timezone.utc) - timedelta(days=int(days))).isoformat(timespec="seconds")


def database_health(conn: sqlite3.Connection, path: str) -> dict[str, Any]:
    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    free_pages = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    quick_check = str(conn.execute("PRAGMA quick_check(1)").fetchone()[0])
    return {
        "integrity": quick_check,
        "size_bytes": page_count * page_size,
        "free_bytes": free_pages * page_size,
        "wal_bytes": os.path.getsize(path + "-wal") if os.path.exists(path + "-wal") else 0,
        "page_count": page_count,
        "free_pages": free_pages,
    }


def _checkpoint(conn: sqlite3.Connection) -> dict[str, int]:
    busy, log_frames, checkpointed = conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
    return {
        "busy": int(busy), "log_frames": int(log_frames),
        "checkpointed_frames": int(checkpointed),
    }


def maintain_state(
    conn: sqlite3.Connection,
    path: str,
    *,
    telemetry_days: int = 30,
    resolved_incident_days: int = 365,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    telemetry_cutoff = _cutoff(current, telemetry_days)
    incident_cutoff = _cutoff(current, resolved_incident_days)
    stale_running = _cutoff(current, 1)
    abandoned = conn.execute(
        """UPDATE job_runs SET status='ABANDONED',finished_at=?,error_code='PROCESS_RESTART'
             WHERE status='RUNNING' AND started_at<?""",
        (current.isoformat(timespec="seconds"), stale_running),
    ).rowcount
    job_runs = conn.execute(
        "DELETE FROM job_runs WHERE finished_at IS NOT NULL AND finished_at<?",
        (telemetry_cutoff,),
    ).rowcount
    notifications = conn.execute(
        "DELETE FROM incident_notifications WHERE delivered_at IS NOT NULL AND delivered_at<?",
        (telemetry_cutoff,),
    ).rowcount
    delivery_claims = conn.execute(
        "DELETE FROM delivery_claims WHERE delivered_at IS NOT NULL AND updated_at<?",
        (telemetry_cutoff,),
    ).rowcount
    setup_audit = conn.execute(
        "DELETE FROM setup_audit_events WHERE synced=1 AND occurred_at<?",
        (telemetry_cutoff,),
    ).rowcount
    strategy_decisions = conn.execute(
        "DELETE FROM strategy_decisions WHERE created_at<?", (telemetry_cutoff,),
    ).rowcount
    incidents = conn.execute(
        "DELETE FROM incidents WHERE resolved_at IS NOT NULL AND resolved_at<?",
        (incident_cutoff,),
    ).rowcount
    conn.commit()
    checkpoint = _checkpoint(conn)
    return {
        "deleted": {
            "job_runs": int(job_runs), "incident_notifications": int(notifications),
            "delivery_claims": int(delivery_claims),
            "setup_audit_events": int(setup_audit),
            "strategy_decisions": int(strategy_decisions),
            "resolved_incidents": int(incidents),
        },
        "abandoned_job_runs": int(abandoned),
        "checkpoint": checkpoint,
        "health": database_health(conn, path),
    }


def maintain_memory(
    conn: sqlite3.Connection,
    path: str,
    *,
    context_days: int = 365,
    now: datetime | None = None,
) -> dict[str, Any]:
    cutoff = _cutoff(now or datetime.now(timezone.utc), context_days)
    events = conn.execute(
        "DELETE FROM live_market_events WHERE event_time<?", (cutoff,),
    ).rowcount
    context = conn.execute(
        "DELETE FROM live_context_observations WHERE event_time<?", (cutoff,),
    ).rowcount
    conn.commit()
    checkpoint = _checkpoint(conn)
    return {
        "deleted": {"market_events": int(events), "context_observations": int(context)},
        "checkpoint": checkpoint,
        "health": database_health(conn, path),
    }


__all__ = ["database_health", "maintain_memory", "maintain_state"]
