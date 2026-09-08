"""Safe SQLite backup and restore verification helpers.

The functions operate on explicit file paths supplied by the caller.  They do
not touch production automatically; a deploy/runbook can invoke them before a
restart and inspect the returned integrity report.
"""
from __future__ import annotations

import os
import sqlite3
from typing import Any, Iterable


def backup_sqlite(source_path: str, destination_path: str) -> dict[str, Any]:
    if not source_path or not destination_path or os.path.abspath(source_path) == os.path.abspath(destination_path):
        raise ValueError("source_and_destination_must_differ")
    os.makedirs(os.path.dirname(os.path.abspath(destination_path)), exist_ok=True)
    source = sqlite3.connect(source_path, timeout=20)
    destination = sqlite3.connect(destination_path, timeout=20)
    try:
        source.backup(destination)
        destination.commit()
    finally:
        destination.close(); source.close()
    return verify_sqlite_backup(destination_path)


def verify_sqlite_backup(path: str, required_tables: Iterable[str] = ()) -> dict[str, Any]:
    conn = sqlite3.connect(path, timeout=20)
    try:
        integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        tables = {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing = sorted(set(required_tables) - tables)
        return {"path": os.path.abspath(path), "integrity": integrity, "ok": integrity.lower() == "ok" and not missing,
                "tables": sorted(tables), "missing_tables": missing}
    finally:
        conn.close()


__all__ = ["backup_sqlite", "verify_sqlite_backup"]
