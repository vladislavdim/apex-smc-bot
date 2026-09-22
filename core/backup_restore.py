"""Safe SQLite backup and restore verification helpers.

The functions operate on explicit file paths supplied by the caller.  They do
not touch production automatically; a deploy/runbook can invoke them before a
restart and inspect the returned integrity report.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import Any, Iterable


def backup_sqlite(source_path: str, destination_path: str) -> dict[str, Any]:
    if not source_path or not destination_path or os.path.abspath(source_path) == os.path.abspath(destination_path):
        raise ValueError("source_and_destination_must_differ")
    source_path = os.path.abspath(source_path)
    destination_path = os.path.abspath(destination_path)
    if not os.path.isfile(source_path):
        raise FileNotFoundError(source_path)
    destination_dir = os.path.dirname(destination_path)
    os.makedirs(destination_dir, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(prefix=".apex-backup-", suffix=".db", dir=destination_dir)
    os.close(fd)
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True, timeout=20)
    destination = sqlite3.connect(temporary_path, timeout=20)
    try:
        source.backup(destination)
        destination.commit()
        destination.close()
        source.close()
        report = verify_sqlite_backup(temporary_path)
        if not report["ok"]:
            raise sqlite3.DatabaseError(f"backup_integrity_failed:{report['integrity']}")
        os.replace(temporary_path, destination_path)
        temporary_path = ""
    finally:
        try:
            destination.close()
        finally:
            source.close()
        if temporary_path:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass
    return verify_sqlite_backup(destination_path)


def verify_sqlite_backup(path: str, required_tables: Iterable[str] = ()) -> dict[str, Any]:
    absolute = os.path.abspath(path)
    if not os.path.isfile(absolute):
        return {"path": absolute, "integrity": "missing", "ok": False, "tables": [],
                "missing_tables": sorted(set(required_tables))}
    conn = sqlite3.connect(f"file:{absolute}?mode=ro", uri=True, timeout=20)
    try:
        integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        tables = {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing = sorted(set(required_tables) - tables)
        return {"path": absolute, "integrity": integrity, "ok": integrity.lower() == "ok" and not missing,
                "tables": sorted(tables), "missing_tables": missing}
    finally:
        conn.close()


__all__ = ["backup_sqlite", "verify_sqlite_backup"]
