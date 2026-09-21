"""SQLite backup primitive; persistence policy stays outside this module."""
from __future__ import annotations

from pathlib import Path
from sqlite3 import Connection

from apex.db.connection import connect_path


def backup_database(source: Connection, destination_path: str) -> str:
    destination = Path(destination_path).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    target = connect_path(str(destination))
    try:
        source.backup(target)
        target.commit()
    finally:
        target.close()
    return str(destination)


__all__ = ["backup_database"]
