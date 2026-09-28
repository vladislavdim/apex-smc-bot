"""Read-only legacy signal views for pre-V3 callers only.

Production Telegram uses the State-owned projection in apex.ui.telegram.trades.
"""
from __future__ import annotations

import sqlite3
from typing import Any
from apex.db.connection import connect_compatibility as _connect_compatibility_db

def _connect(db_path: str) -> sqlite3.Connection:
    conn = _connect_compatibility_db(db_path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def fetch_trades(db_path: str, category: str, limit: int = 12) -> list[dict[str, Any]]:
    """Return existing signal rows; never creates or migrates a table."""
    category = str(category).lower()
    where = {
        "active": "s.result='pending'",
        "take": "s.result IN ('tp1','tp2','tp3')",
        "stop": "s.result='sl'",
    }.get(category)
    if not where:
        raise ValueError(f"unsupported trade category: {category}")
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            f"""SELECT s.id, s.symbol, s.direction, s.signal_type, s.entry, s.sl,
                       s.tp1, s.tp2, s.tp3, s.timeframe, s.grade, s.result,
                       s.created_at, s.closed_at, s.tp1_hit, s.trailing_sl,
                       COALESCE(es.status, CASE WHEN s.result='pending' THEN 'active' ELSE 'closed' END) AS lifecycle_status,
                       te.status AS execution_status
                FROM signals s
                LEFT JOIN signal_execution_state es ON es.signal_id=s.id
                LEFT JOIN trade_executions te ON te.signal_id=s.id
                WHERE {where}
                ORDER BY COALESCE(s.closed_at, s.created_at) DESC LIMIT ?""",
            (max(1, min(int(limit), 30)),),
        ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.OperationalError as exc:
        # Older databases can predate either isolated status table.  Preserve
        # the view with only the stable legacy signals schema.
        if "no such table" not in str(exc).lower() and "no such column" not in str(exc).lower():
            raise
        rows = conn.execute(
            f"""SELECT s.id, s.symbol, s.direction, s.signal_type, s.entry, s.sl,
                       s.tp1, s.tp2, s.tp3, s.timeframe, s.grade, s.result,
                       s.created_at, s.closed_at, 0 AS tp1_hit, 0 AS trailing_sl,
                       CASE WHEN s.result='pending' THEN 'active' ELSE 'closed' END AS lifecycle_status,
                       NULL AS execution_status
                FROM signals s WHERE {where}
                ORDER BY COALESCE(s.closed_at, s.created_at) DESC LIMIT ?""",
            (max(1, min(int(limit), 30)),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


__all__ = ["fetch_trades"]
