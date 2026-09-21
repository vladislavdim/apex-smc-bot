"""SQLite integrity checks for canonical V3 stores."""
from __future__ import annotations

import sqlite3


def check_integrity(conn: sqlite3.Connection, *, quick: bool = True) -> tuple[bool, tuple[str, ...]]:
    pragma = "quick_check" if quick else "integrity_check"
    rows = tuple(str(row[0]) for row in conn.execute(f"PRAGMA {pragma}"))
    return rows == ("ok",), rows


__all__ = ["check_integrity"]
