"""Fail-closed integrity checks for canonical V3 SQLite stores."""
from __future__ import annotations
import sqlite3

def check_integrity(conn:sqlite3.Connection)->None:
    row=conn.execute("PRAGMA quick_check").fetchone()
    if not row or str(row[0]).lower()!="ok": raise RuntimeError("database_integrity_failed")
__all__=["check_integrity"]
