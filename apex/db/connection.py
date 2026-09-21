"""The only V3 SQLite connection policy."""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from apex.config.settings import ApexConfig


_CONNECT = sqlite3.dbapi2.connect
_COMPATIBILITY_LOCK = threading.RLock()


def _connect(
    path: str, *, read_only: bool = False, timeout: float = 30,
) -> sqlite3.Connection:
    resolved = os.path.abspath(path)
    if not read_only:
        Path(resolved).parent.mkdir(parents=True, exist_ok=True)
    target = f"file:{resolved}?mode=ro" if read_only else resolved
    conn = _CONNECT(
        target, uri=read_only, timeout=max(0.1, float(timeout)),
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    if not read_only:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
    return conn


def connect_state(config: ApexConfig | None = None, *, read_only: bool = False) -> sqlite3.Connection:
    settings = config or ApexConfig.from_env()
    return _connect(settings.database.state_db_path, read_only=read_only)


def connect_memory(config: ApexConfig | None = None, *, read_only: bool = False) -> sqlite3.Connection:
    settings = config or ApexConfig.from_env()
    return _connect(settings.database.memory_db_path, read_only=read_only)


def connect_compatibility(
    path: str | None = None,
    timeout: float = 30,
    check_same_thread: bool = False,
    *,
    config: ApexConfig | None = None,
    read_only: bool = False,
) -> sqlite3.Connection:
    """Open the legacy compatibility DB without mutating ``sqlite3.connect``.

    Callers that cannot yet use repositories may use this explicit bridge.
    Transaction ownership remains with the caller; new V3 code should prefer
    ``connect_state`` or ``connect_memory``.
    """
    del check_same_thread  # Canonical policy is always safe across worker threads.
    settings = config or ApexConfig.from_env()
    return _connect(
        path or settings.database.compatibility_db_path,
        read_only=read_only,
        timeout=timeout,
    )


@contextmanager
def compatibility_connection(
    config: ApexConfig | None = None,
    *,
    path: str | None = None,
) -> Iterator[sqlite3.Connection]:
    """Temporary serialized access to the legacy DB during V3 migration."""
    settings = config or ApexConfig.from_env()
    target = path or settings.database.compatibility_db_path
    with _COMPATIBILITY_LOCK:
        conn = connect_compatibility(target, config=settings)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


__all__ = [
    "compatibility_connection", "connect_compatibility", "connect_memory",
    "connect_state",
]
