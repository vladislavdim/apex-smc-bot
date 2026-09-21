"""Typed ownership of production runtime history and heartbeats."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Callable


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RuntimeRepository:
    def __init__(self, connection_factory: Callable[[], sqlite3.Connection]) -> None:
        self.connection_factory = connection_factory

    def record_start(self, instance_id: str, release_sha: str) -> dict[str, Any]:
        iid = str(instance_id).strip()
        if not iid:
            raise ValueError("instance_id_required")
        sha = str(release_sha or "unknown").strip()
        now = _now()
        conn = self.connection_factory()
        try:
            previous = conn.execute(
                "SELECT * FROM runtime_instances WHERE instance_id<>? "
                "ORDER BY started_at DESC LIMIT 1",
                (iid,),
            ).fetchone()
            conn.execute(
                """INSERT INTO runtime_instances(instance_id,release_sha,started_at,previous_instance)
                   VALUES(?,?,?,?)
                   ON CONFLICT(instance_id) DO UPDATE SET release_sha=excluded.release_sha""",
                (iid, sha, now.isoformat(timespec="seconds"), str(previous["instance_id"] if previous else "")),
            )
            one_hour = (now - timedelta(hours=1)).isoformat(timespec="seconds")
            one_day = (now - timedelta(days=1)).isoformat(timespec="seconds")
            count_1h = int(conn.execute(
                "SELECT COUNT(*) FROM runtime_instances WHERE started_at>=?", (one_hour,),
            ).fetchone()[0])
            count_24h = int(conn.execute(
                "SELECT COUNT(*) FROM runtime_instances WHERE started_at>=?", (one_day,),
            ).fetchone()[0])
            conn.commit()
            return {
                "instance_id": iid,
                "release_sha": sha,
                "started_at": now.isoformat(timespec="seconds"),
                "previous_instance": str(previous["instance_id"] if previous else ""),
                "previous_started_at": str(previous["started_at"] if previous else ""),
                "previous_shutdown_reason": str(previous["shutdown_reason"] if previous else ""),
                "restart_count_1h": count_1h,
                "restart_count_24h": count_24h,
            }
        finally:
            conn.close()

    def record_shutdown(self, instance_id: str, reason: str) -> bool:
        iid = str(instance_id).strip()
        if not iid:
            raise ValueError("instance_id_required")
        conn = self.connection_factory()
        try:
            cursor = conn.execute(
                "UPDATE runtime_instances SET stopped_at=?,shutdown_reason=? WHERE instance_id=?",
                (_now().isoformat(timespec="seconds"), str(reason)[:200], iid),
            )
            conn.commit()
            return bool(cursor.rowcount)
        finally:
            conn.close()

    def heartbeat(self, instance_id: str, release_sha: str) -> None:
        iid = str(instance_id).strip()
        if not iid:
            raise ValueError("instance_id_required")
        conn = self.connection_factory()
        try:
            conn.execute(
                """INSERT INTO runtime_heartbeats(instance_id,release_sha,observed_at)
                   VALUES(?,?,?)""",
                (iid, str(release_sha or "unknown").strip(), _now().isoformat(timespec="seconds")),
            )
            conn.execute(
                """DELETE FROM runtime_heartbeats WHERE heartbeat_id NOT IN
                   (SELECT heartbeat_id FROM runtime_heartbeats
                    ORDER BY heartbeat_id DESC LIMIT 100)"""
            )
            conn.commit()
        finally:
            conn.close()


__all__ = ["RuntimeRepository"]
