"""Isolated diagnostic outbox with durable, bounded legacy State archival.

Trading state is never deleted to save space. Old diagnostic rows are removed
only after an exact copy has been committed to the audit backup on GitHub.
"""
from __future__ import annotations

import hashlib
import json
import os
from contextlib import closing

from .connection import connect_compatibility

_COLUMNS = "event_key,kind,strategy,symbol,occurred_at,payload_json,synced,sync_attempts,last_sync_error,created_at"


def audit_path(state_path: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(state_path)), "apex_audit.db")


def migrate_audit(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS setup_audit_events (
        event_key TEXT PRIMARY KEY, kind TEXT NOT NULL, strategy TEXT, symbol TEXT,
        occurred_at TEXT NOT NULL, payload_json TEXT NOT NULL,
        synced INTEGER NOT NULL DEFAULT 0, sync_attempts INTEGER NOT NULL DEFAULT 0,
        last_sync_error TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_unsynced ON setup_audit_events(occurred_at) WHERE synced=0")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_recent ON setup_audit_events(occurred_at)")
    conn.commit()


def _identity(row) -> str:
    # Sync bookkeeping may change after the copy; payload/provenance may not.
    return hashlib.sha256(json.dumps(list(row[:6]) + [row[9]], ensure_ascii=False).encode()).hexdigest()


def stage_legacy_events(state_path: str, target_path: str, limit: int = 10000) -> dict[str, str]:
    """Copy at most one bounded batch; do not remove anything from State."""
    if os.path.abspath(state_path) == os.path.abspath(target_path):
        raise ValueError("audit database must be separate from State")
    with closing(connect_compatibility(state_path, read_only=True)) as source:
        rows = source.execute(f"SELECT {_COLUMNS} FROM setup_audit_events ORDER BY rowid LIMIT ?", (int(limit),)).fetchall()
    receipts = {}
    with closing(connect_compatibility(target_path)) as target:
        migrate_audit(target)
        with target:
            for row in rows:
                target.execute(f"INSERT OR IGNORE INTO setup_audit_events ({_COLUMNS}) VALUES ({','.join('?' for _ in range(10))})", tuple(row))
                archived = target.execute(f"SELECT {_COLUMNS} FROM setup_audit_events WHERE event_key=?", (row[0],)).fetchone()
                if _identity(archived) != _identity(row):
                    raise RuntimeError("audit archive event collision")
                receipts[str(row[0])] = _identity(row)
    return receipts


def retire_legacy_events(state_path: str, receipts: dict[str, str], backup_result: dict) -> int:
    """Only call with receipts staged before this successful durable backup."""
    if backup_result.get("status") not in {"saved", "unchanged"}:
        return 0
    removed = 0
    # Short transactions avoid locking trading writers for a whole archive batch.
    keys = list(receipts)
    with closing(connect_compatibility(state_path)) as source:
        for offset in range(0, len(keys), 100):
            with source:
                for key in keys[offset:offset + 100]:
                    row = source.execute(f"SELECT {_COLUMNS} FROM setup_audit_events WHERE event_key=?", (key,)).fetchone()
                    if row is not None and _identity(row) == receipts[key]:
                        removed += source.execute(
                            "DELETE FROM setup_audit_events WHERE event_key=? AND kind=? "
                            "AND strategy IS ? AND symbol IS ? AND occurred_at=? "
                            "AND payload_json=? AND created_at=?",
                            tuple(row[:6]) + (row[9],),
                        ).rowcount
    return removed


def merge_audit_snapshot(local_path: str, remote_path: str) -> int:
    """Union diagnostics only; conflicting immutable evidence is never replaced."""
    merged = 0
    with closing(connect_compatibility(remote_path, read_only=True)) as source, \
            closing(connect_compatibility(local_path)) as target:
        tables = {row[0] for row in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables - {"setup_audit_events", "brain_persistence_meta", "sqlite_sequence"}:
            raise RuntimeError("audit merge contains non-audit tables")
        migrate_audit(target)
        cursor = source.execute(f"SELECT {_COLUMNS} FROM setup_audit_events")
        with target:
            for row in cursor:
                local = target.execute(f"SELECT {_COLUMNS} FROM setup_audit_events WHERE event_key=?", (row[0],)).fetchone()
                if local is not None and tuple(local[:6]) != tuple(row[:6]):
                    if str(row[1]) not in {"runtime_status", "incident_snapshot", "apex_v2_snapshot", "apex_v3_snapshot", "worker_heartbeat"}:
                        raise RuntimeError("audit immutable event conflict")
                    if str(row[4]) == str(local[4]):
                        raise RuntimeError("audit mutable event same-time conflict")
                    if str(row[4]) < str(local[4]):
                        continue
                elif local is not None:
                    target.execute("UPDATE setup_audit_events SET synced=MIN(synced,?) WHERE event_key=?", (row[6],row[0]))
                    continue
                target.execute(f"INSERT OR REPLACE INTO setup_audit_events ({_COLUMNS}) VALUES ({','.join('?' for _ in range(10))})", tuple(row))
                merged += 1
    return merged
