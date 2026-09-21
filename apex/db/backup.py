"""SQLite backup primitive; persistence policy stays outside this module."""
from __future__ import annotations

import sqlite3
from pathlib import Path


def backup_database(source: sqlite3.Connection, destination_path: str) -> str:
    destination = Path(destination_path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    target = sqlite3.connect(str(destination))
    try:
        source.backup(target)
        target.commit()
    finally:
        target.close()
    return str(destination)


__all__ = ["backup_database"]
