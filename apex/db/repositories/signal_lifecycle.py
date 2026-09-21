"""State-owned delivered-signal lifecycle projection."""

from __future__ import annotations

import sqlite3
from typing import Any, Callable, Mapping


class SignalLifecycleStateError(RuntimeError):
    pass


class SignalLifecycleRepository:
    _STATUSES = frozenset({"waiting_entry", "active", "closed", "cancelled"})

    def __init__(self, conn_factory: Callable[[], sqlite3.Connection]) -> None:
        self._conn_factory = conn_factory

    def import_row(self, values: Mapping[str, Any]) -> bool:
        signal_id = int(values.get("signal_id") or 0)
        status = str(values.get("status") or "").lower()
        if signal_id <= 0 or status not in self._STATUSES:
            raise SignalLifecycleStateError("signal_lifecycle_invalid")
        result = str(values.get("result") or "pending").lower()
        conn = self._conn_factory()
        try:
            cursor = conn.execute(
                """INSERT INTO signal_lifecycle(
                    signal_id,status,result,activated_at,last_checked_at,closed_at,
                    cancel_reason,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP),COALESCE(?,CURRENT_TIMESTAMP))
                ON CONFLICT(signal_id) DO UPDATE SET
                    status=excluded.status,result=excluded.result,
                    activated_at=excluded.activated_at,
                    last_checked_at=excluded.last_checked_at,
                    closed_at=excluded.closed_at,cancel_reason=excluded.cancel_reason,
                    updated_at=excluded.updated_at""",
                (
                    signal_id, status, result, values.get("activated_at"),
                    values.get("last_checked_at"), values.get("closed_at"),
                    values.get("cancel_reason"),
                    values.get("created_at") or values.get("signal_created_at"),
                    values.get("updated_at") or values.get("last_checked_at")
                    or values.get("closed_at"),
                ),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def get(self, signal_id: int) -> dict[str, Any] | None:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM signal_lifecycle WHERE signal_id=?", (int(signal_id),),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_many(self, signal_ids: list[int] | tuple[int, ...]) -> dict[int, dict[str, Any]]:
        ids = tuple(sorted({int(value) for value in signal_ids if int(value) > 0}))
        if not ids:
            return {}
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM signal_lifecycle WHERE signal_id IN (%s)" % ",".join("?" for _ in ids),
                ids,
            ).fetchall()
            return {int(row["signal_id"]): dict(row) for row in rows}
        finally:
            conn.close()


__all__ = ["SignalLifecycleRepository", "SignalLifecycleStateError"]
