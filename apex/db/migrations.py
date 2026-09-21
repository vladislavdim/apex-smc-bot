"""Atomic, ordered V3 schema migrations."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, Iterable


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    apply: Callable[[sqlite3.Connection], None]


class MigrationRunner:
    def __init__(self, migrations: Iterable[Migration]) -> None:
        self.migrations = tuple(sorted(migrations, key=lambda item: item.version))
        versions = [item.version for item in self.migrations]
        if versions != list(range(1, len(versions) + 1)):
            raise ValueError("migration versions must be contiguous from 1")

    @staticmethod
    def _ensure_table(conn: sqlite3.Connection) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS apex_schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def run(self, conn: sqlite3.Connection) -> tuple[int, ...]:
        self._ensure_table(conn)
        conn.commit()
        history = {
            int(row[0]): str(row[1])
            for row in conn.execute(
                "SELECT version,name FROM apex_schema_migrations ORDER BY version"
            )
        }
        known = {migration.version: migration.name for migration in self.migrations}
        unknown = sorted(set(history) - set(known))
        if unknown:
            raise MigrationError(
                "database_schema_newer_than_runtime:" + ",".join(map(str, unknown))
            )
        drift = [
            version for version, name in history.items()
            if known.get(version) != name
        ]
        if drift:
            version = drift[0]
            raise MigrationError(
                f"migration_history_drift:{version}:{history[version]}!={known[version]}"
            )
        applied = set(history)
        completed: list[int] = []
        for migration in self.migrations:
            if migration.version in applied:
                continue
            try:
                conn.execute("BEGIN IMMEDIATE")
                migration.apply(conn)
                conn.execute(
                    "INSERT INTO apex_schema_migrations(version,name) VALUES(?,?)",
                    (migration.version, migration.name),
                )
                conn.commit()
                completed.append(migration.version)
            except Exception as exc:
                conn.rollback()
                raise MigrationError(
                    f"migration_{migration.version:03d}_{migration.name} failed: {exc}"
                ) from exc
        return tuple(completed)


__all__ = ["Migration", "MigrationError", "MigrationRunner"]
