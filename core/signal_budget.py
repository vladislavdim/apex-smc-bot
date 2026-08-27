"""Portfolio-level delivery budget for the scarce, high-quality strategies."""

from __future__ import annotations

import os
import sqlite3
from typing import Any


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db")
EXEMPT_STRATEGIES = {"FAST", "WYCKOFF"}


def _strategy(candidate: dict[str, Any]) -> str:
    value = candidate.get("scan_type") or candidate.get("grade") or candidate.get("signal_type") or "MTF"
    value = str(value).upper()
    return {"SWING": "SWING", "ZONE": "ZONE", "FAST": "FAST", "WYCKOFF": "WYCKOFF"}.get(value, value)


def _limit() -> int:
    try:
        return max(1, int(os.environ.get("NON_FAST_WEEKLY_SIGNAL_LIMIT", "5")))
    except ValueError:
        return 5


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_delivery_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, strategy TEXT, direction TEXT, timeframe TEXT,
        decision TEXT, confidence REAL,
        delivered_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    return conn


def weekly_budget_status(candidate: dict[str, Any]) -> dict[str, Any]:
    strategy = _strategy(candidate)
    limit = _limit()
    if strategy in EXEMPT_STRATEGIES:
        return {"allowed": True, "strategy": strategy, "used": 0, "limit": limit, "exempt": True}
    try:
        conn = _connect()
        used = conn.execute(
            """SELECT COUNT(*) FROM signal_delivery_log
               WHERE strategy NOT IN ('FAST','WYCKOFF')
                 AND delivered_at >= datetime('now', '-7 days')"""
        ).fetchone()[0]
        conn.close()
        return {"allowed": used < limit, "strategy": strategy, "used": used, "limit": limit, "exempt": False}
    except Exception:
        # A bookkeeping failure must not stop the scanner or Telegram.
        return {"allowed": True, "strategy": strategy, "used": 0, "limit": limit, "exempt": False, "degraded": True}


def record_signal_delivery(candidate: dict[str, Any], review: dict[str, Any] | None = None) -> None:
    strategy = _strategy(candidate)
    try:
        conn = _connect()
        conn.execute(
            """INSERT INTO signal_delivery_log
               (symbol, strategy, direction, timeframe, decision, confidence)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                candidate.get("symbol"), strategy, candidate.get("direction"), candidate.get("timeframe"),
                (review or {}).get("decision", "APPROVE"), (review or {}).get("confidence", 0.0),
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass
