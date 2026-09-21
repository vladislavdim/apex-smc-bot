"""Persistent restart detector backed by the production state database."""

from __future__ import annotations

import sqlite3
from typing import Any
from apex.db.connection import connect_compatibility
from apex.db.repositories.runtime import RuntimeRepository
from apex.db.state_db import migrate_state


def _connect(path: str) -> sqlite3.Connection:
    conn = connect_compatibility(path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_restart_schema(db_path: str) -> None:
    conn = _connect(db_path)
    try:
        migrate_state(conn)
    finally:
        conn.close()


def record_start(db_path: str, *, instance_id: str = "", release_sha: str = "") -> dict[str, Any]:
    ensure_restart_schema(db_path)
    return RuntimeRepository(lambda: _connect(db_path)).record_start(instance_id, release_sha)


def record_shutdown(db_path: str, reason: str, *, instance_id: str = "") -> None:
    ensure_restart_schema(db_path)
    RuntimeRepository(lambda: _connect(db_path)).record_shutdown(instance_id, reason)


__all__ = ["ensure_restart_schema", "record_shutdown", "record_start"]
