"""Persistence helpers for the analytics signal execution lifecycle.

Signals are Telegram analytics, not exchange orders.  Nevertheless, their
learning record must not start before the advertised entry is touched.  The
isolated table avoids altering the legacy ``signals`` schema and is safe for
existing databases: old pending rows are treated as already active.
"""

from __future__ import annotations

import sqlite3
from typing import Any


WAITING_ENTRY = "waiting_entry"
ACTIVE = "active"
CLOSED = "closed"
CANCELLED = "cancelled"


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_execution_state (
        signal_id INTEGER PRIMARY KEY,
        status TEXT NOT NULL,
        activated_at TEXT,
        last_checked_at TEXT,
        closed_at TEXT,
        cancel_reason TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")


def register_waiting(conn: sqlite3.Connection, signal_id: int) -> None:
    ensure_schema(conn)
    conn.execute(
        """INSERT OR REPLACE INTO signal_execution_state
           (signal_id, status, activated_at, last_checked_at, closed_at, cancel_reason, created_at)
           VALUES (?, ?, NULL, CURRENT_TIMESTAMP, NULL, NULL, CURRENT_TIMESTAMP)""",
        (signal_id, WAITING_ENTRY),
    )


def state_for(conn: sqlite3.Connection, signal_id: int) -> str:
    """Return state; legacy pending rows without a state are already active."""
    ensure_schema(conn)
    row = conn.execute(
        "SELECT status FROM signal_execution_state WHERE signal_id=?", (signal_id,)
    ).fetchone()
    if row:
        return str(row[0])
    conn.execute(
        """INSERT OR IGNORE INTO signal_execution_state
           (signal_id, status, activated_at, last_checked_at)
           VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
        (signal_id, ACTIVE),
    )
    return ACTIVE


def activated_at_for(conn: sqlite3.Connection, signal_id: int) -> str | None:
    """Return the persisted activation boundary for interval-barrier checks."""
    ensure_schema(conn)
    row = conn.execute(
        "SELECT activated_at FROM signal_execution_state WHERE signal_id=?",
        (signal_id,),
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def mark_active(conn: sqlite3.Connection, signal_id: int) -> None:
    ensure_schema(conn)
    conn.execute(
        """UPDATE signal_execution_state
           SET status=?, activated_at=COALESCE(activated_at, CURRENT_TIMESTAMP),
               last_checked_at=CURRENT_TIMESTAMP, cancel_reason=NULL
           WHERE signal_id=?""",
        (ACTIVE, signal_id),
    )


def mark_finished(conn: sqlite3.Connection, signal_id: int, status: str = CLOSED, reason: str | None = None) -> None:
    ensure_schema(conn)
    conn.execute(
        """UPDATE signal_execution_state
           SET status=?, closed_at=CURRENT_TIMESTAMP, last_checked_at=CURRENT_TIMESTAMP,
               cancel_reason=? WHERE signal_id=?""",
        (status, reason, signal_id),
    )


def touch(conn: sqlite3.Connection, signal_id: int) -> None:
    ensure_schema(conn)
    conn.execute(
        "UPDATE signal_execution_state SET last_checked_at=CURRENT_TIMESTAMP WHERE signal_id=?",
        (signal_id,),
    )


def entry_touched(
    direction: str,
    entry: float,
    current: float | None = None,
    low: float | None = None,
    high: float | None = None,
) -> bool:
    """Best-effort limit-entry touch using the latest observation interval."""
    if low is not None and high is not None and low <= entry <= high:
        return True
    direction = str(direction).upper()
    if current is None:
        return False
    return current <= entry if direction == "BULLISH" else current >= entry


def barrier_hits(
    direction: str,
    sl: float,
    tp1: float,
    tp2: float,
    low: float,
    high: float,
) -> dict[str, bool]:
    """Describe barrier touches without guessing their intrabar order."""
    if str(direction).upper() == "BULLISH":
        return {"sl": low <= sl, "tp1": high >= tp1, "tp2": high >= tp2}
    return {"sl": high >= sl, "tp1": low <= tp1, "tp2": low <= tp2}
