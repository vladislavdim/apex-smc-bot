"""Idempotent State-to-compatibility projection for analytical signal readers."""

from __future__ import annotations

import sqlite3
from typing import Callable


class SignalMonitorProjectionError(RuntimeError):
    pass


def sync_state_monitor_projection(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
) -> int:
    """Replay State-owned monitor progress before any legacy-to-State refresh.

    A missing compatibility identity blocks cutover; this projection cannot
    invent a delivered signal or an exchange position.
    """
    state = state_factory()
    try:
        state.row_factory = sqlite3.Row
        rows = [dict(row) for row in state.execute(
            "SELECT * FROM signal_lifecycle WHERE ownership='state' ORDER BY signal_id"
        ).fetchall()]
    finally:
        state.close()
    if not rows:
        return 0
    legacy = legacy_factory()
    try:
        legacy.execute("BEGIN IMMEDIATE")
        for row in rows:
            signal_id = int(row["signal_id"])
            exists = legacy.execute("SELECT 1 FROM signals WHERE id=?", (signal_id,)).fetchone()
            if not exists:
                raise SignalMonitorProjectionError(f"signal_monitor_legacy_identity_missing:{signal_id}")
            legacy.execute(
                """UPDATE signals SET result=?,closed_at=?,tp1_hit=?,trailing_sl=?,best_price=?
                   WHERE id=?""",
                (row["result"], row["closed_at"], row["tp1_hit"],
                 row["trailing_sl"], row["best_price"], signal_id),
            )
            legacy.execute(
                """INSERT INTO signal_execution_state(
                       signal_id,status,activated_at,last_checked_at,closed_at,cancel_reason
                   ) VALUES(?,?,?,?,?,?)
                   ON CONFLICT(signal_id) DO UPDATE SET
                       status=excluded.status,activated_at=excluded.activated_at,
                       last_checked_at=excluded.last_checked_at,
                       closed_at=excluded.closed_at,cancel_reason=excluded.cancel_reason""",
                (signal_id, row["status"], row["activated_at"], row["last_checked_at"],
                 row["closed_at"], row["cancel_reason"]),
            )
        legacy.commit()
        return len(rows)
    except Exception:
        legacy.rollback()
        raise
    finally:
        legacy.close()


__all__ = ["SignalMonitorProjectionError", "sync_state_monitor_projection"]
