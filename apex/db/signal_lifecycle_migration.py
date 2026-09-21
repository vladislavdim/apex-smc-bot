"""Restart-safe signal lifecycle import and parity verification."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from .repositories.signal_lifecycle import SignalLifecycleRepository, SignalLifecycleStateError


_MARKER = "signal_lifecycle_legacy_import_v1"
_FIELDS = (
    "status", "result", "activated_at", "last_checked_at", "closed_at", "cancel_reason",
)


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}


def _projection(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    available = _tables(conn)
    if "signals" not in available:
        return {}
    conn.row_factory = sqlite3.Row
    signal_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(signals)")}
    lifecycle = "signal_execution_state" in available
    lifecycle_columns = (
        {str(row[1]) for row in conn.execute("PRAGMA table_info(signal_execution_state)")}
        if lifecycle else set()
    )
    signal_expr = lambda name: f"s.{name}" if name in signal_columns else "NULL"
    life_expr = lambda name: f"x.{name}" if name in lifecycle_columns else "NULL"
    result_expr = signal_expr("result")
    created_expr = signal_expr("created_at")
    closed_expr = life_expr("closed_at") if lifecycle else signal_expr("closed_at")
    status_expr = life_expr("status") if lifecycle else "NULL"
    activated_expr = life_expr("activated_at") if lifecycle else "NULL"
    checked_expr = life_expr("last_checked_at") if lifecycle else "NULL"
    cancel_expr = life_expr("cancel_reason") if lifecycle else "NULL"
    join = " LEFT JOIN signal_execution_state x ON x.signal_id=s.id" if lifecycle else ""
    query = f"""SELECT s.id AS signal_id,{result_expr} AS result,
                       {created_expr} AS signal_created_at,{status_expr} AS status,
                       {activated_expr} AS activated_at,{checked_expr} AS last_checked_at,
                       {closed_expr} AS closed_at,{cancel_expr} AS cancel_reason,
                       COALESCE({checked_expr},{closed_expr},{created_expr}) AS updated_at
                  FROM signals s{join} ORDER BY s.id"""
    rows = conn.execute(query).fetchall()
    result: dict[int, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        signal_result = str(row.get("result") or "pending").lower()
        status = str(row.get("status") or "").lower()
        if not status:
            status = "active" if signal_result == "pending" else (
                "cancelled" if signal_result == "cancelled" else "closed"
            )
        result[int(row["signal_id"])] = {
            **row, "status": status, "result": signal_result,
        }
    return result


def import_legacy_signal_lifecycle(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
    *, refresh: bool = False,
) -> dict[str, int | bool]:
    state = state_factory()
    try:
        marker = state.execute("SELECT 1 FROM runtime_state WHERE key=?", (_MARKER,)).fetchone()
        if marker is not None and not refresh:
            return {"already_complete": True, "signals": 0}
    finally:
        state.close()
    legacy = legacy_factory()
    try:
        source = _projection(legacy)
    finally:
        legacy.close()
    repository = SignalLifecycleRepository(state_factory)
    imported = 0
    for row in source.values():
        imported += int(repository.import_row(row))
    state = state_factory()
    try:
        target_ids = {int(row[0]) for row in state.execute("SELECT signal_id FROM signal_lifecycle")}
        if target_ids != set(source):
            raise SignalLifecycleStateError("signal_lifecycle_identity_set")
        state.execute(
            """INSERT INTO runtime_state(key,value_json) VALUES(?,?)
               ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                 updated_at=CURRENT_TIMESTAMP""",
            (_MARKER, json.dumps({"signals": len(source)})),
        )
        state.commit()
    finally:
        state.close()
    return {"already_complete": False, "signals": imported}


def signal_lifecycle_parity_report(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
) -> dict[str, Any]:
    legacy, state = legacy_factory(), state_factory()
    mismatches: list[str] = []
    try:
        source = _projection(legacy)
        state.row_factory = sqlite3.Row
        target = {
            int(row["signal_id"]): dict(row)
            for row in state.execute("SELECT * FROM signal_lifecycle")
        }
        if set(source) != set(target):
            mismatches.append("signal_identity_set")
        for signal_id in sorted(set(source) & set(target)):
            for field in _FIELDS:
                if source[signal_id].get(field) != target[signal_id].get(field):
                    mismatches.append(f"signal:{signal_id}:{field}")
    finally:
        legacy.close(); state.close()
    return {"ok": not mismatches, "signals": len(source), "mismatches": mismatches[:100]}


__all__ = ["import_legacy_signal_lifecycle", "signal_lifecycle_parity_report"]
