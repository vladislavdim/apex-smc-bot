"""Restart-safe execution state import and parity verification."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from .repositories.executions import ExecutionRepository, ExecutionStateError


_MARKER = "execution_state_legacy_import_v1"
_EXECUTION_FIELDS = (
    "mode", "exchange", "symbol", "direction", "status", "entry", "sl", "tp1", "tp2",
    "quantity", "risk_usdt", "balance_usdt", "leverage", "entry_order_id", "stop_order_id",
    "tp1_order_id", "tp2_order_id", "active_stop_price", "pending_stop_order_id",
    "previous_stop_order_id", "last_error",
)
_ACTION_FIELDS = (
    "signal_id", "action", "status", "requested_level", "exchange_order_id", "error",
)
_IMMUTABLE_EXECUTION_FIELDS = (
    "mode", "exchange", "symbol", "direction", "entry", "sl", "tp1", "tp2",
    "risk_usdt", "balance_usdt", "leverage",
)
_IMMUTABLE_ACTION_FIELDS = ("signal_id", "action", "requested_level")


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}


def _rows(conn: sqlite3.Connection, table: str, order: str) -> list[dict[str, Any]]:
    cursor = conn.execute(f"SELECT * FROM {table} ORDER BY {order}")
    columns = tuple(item[0] for item in cursor.description)
    return [dict(row) if isinstance(row, sqlite3.Row) else dict(zip(columns, row))
            for row in cursor.fetchall()]


def import_legacy_executions(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
    *, refresh: bool = False,
) -> dict[str, int | bool]:
    state = state_factory()
    try:
        marker = state.execute("SELECT 1 FROM runtime_state WHERE key=?", (_MARKER,)).fetchone()
        if marker is not None and not refresh:
            return {"already_complete": True, "executions": 0, "actions": 0}
    finally:
        state.close()
    legacy = legacy_factory()
    try:
        legacy.row_factory = sqlite3.Row
        tables = _tables(legacy)
        executions = _rows(legacy, "trade_executions", "signal_id") if "trade_executions" in tables else []
        actions = _rows(legacy, "manager_execution_actions", "action_key") if "manager_execution_actions" in tables else []
    finally:
        legacy.close()

    repository = ExecutionRepository(state_factory)
    imported_executions = 0
    for row in executions:
        values = {key: row.get(key) for key in _EXECUTION_FIELDS}
        values.update(signal_id=row.get("signal_id"), tp3=None)
        if repository.get(int(row["signal_id"])) is None:
            imported_executions += int(repository.register(values))
    known = {int(row["signal_id"]) for row in executions}
    imported_actions = 0
    state = state_factory()
    try:
        known_action_keys = {
            str(item[0]) for item in state.execute(
                "SELECT action_key FROM execution_actions"
            ).fetchall()
        }
    finally:
        state.close()
    for row in actions:
        if int(row.get("signal_id") or 0) not in known:
            raise ExecutionStateError(f"execution_action_orphan:{row.get('action_key')}")
        action_key = str(row["action_key"])
        if action_key not in known_action_keys and repository.import_action(row):
            imported_actions += 1
            known_action_keys.add(action_key)

    state = state_factory()
    try:
        target_executions = int(state.execute(
            "SELECT COUNT(*) FROM executions WHERE signal_id IN (%s)" % (
                ",".join("?" for _ in known) or "NULL"
            ), tuple(sorted(known)),
        ).fetchone()[0])
        keys = tuple(str(row["action_key"]) for row in actions)
        target_actions = int(state.execute(
            "SELECT COUNT(*) FROM execution_actions WHERE action_key IN (%s)" % (
                ",".join("?" for _ in keys) or "NULL"
            ), keys,
        ).fetchone()[0])
        if target_executions != len(executions) or target_actions != len(actions):
            raise ExecutionStateError("execution_import_verification_failed")
        state.execute(
            """INSERT INTO runtime_state(key,value_json) VALUES(?,?)
               ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                 updated_at=CURRENT_TIMESTAMP""",
            (_MARKER, json.dumps({"executions": len(executions), "actions": len(actions)})),
        )
        state.commit()
    finally:
        state.close()
    return {
        "already_complete": False, "executions": imported_executions,
        "actions": imported_actions,
    }


def execution_parity_report(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
) -> dict[str, Any]:
    legacy, state = legacy_factory(), state_factory()
    mismatches: list[str] = []
    try:
        legacy.row_factory = state.row_factory = sqlite3.Row
        tables = _tables(legacy)
        source_rows = _rows(legacy, "trade_executions", "signal_id") if "trade_executions" in tables else []
        action_rows = _rows(legacy, "manager_execution_actions", "action_key") if "manager_execution_actions" in tables else []
        source = {int(row["signal_id"]): row for row in source_rows}
        target = {int(row["signal_id"]): dict(row) for row in state.execute("SELECT * FROM executions")}
        if set(source) - set(target): mismatches.append("execution_identity_set")
        for signal_id in sorted(set(source) & set(target)):
            for field in _IMMUTABLE_EXECUTION_FIELDS:
                if source[signal_id].get(field) != target[signal_id].get(field):
                    mismatches.append(f"execution:{signal_id}:{field}")
        source_actions = {str(row["action_key"]): row for row in action_rows}
        target_actions = {str(row["action_key"]): dict(row) for row in state.execute("SELECT * FROM execution_actions")}
        if set(source_actions) - set(target_actions): mismatches.append("action_identity_set")
        for key in sorted(set(source_actions) & set(target_actions)):
            for field in _IMMUTABLE_ACTION_FIELDS:
                if source_actions[key].get(field) != target_actions[key].get(field):
                    mismatches.append(f"action:{key}:{field}")
    finally:
        legacy.close(); state.close()
    return {
        "ok": not mismatches, "executions": len(source), "actions": len(source_actions),
        "mismatches": mismatches[:100],
    }


__all__ = ["execution_parity_report", "import_legacy_executions"]
