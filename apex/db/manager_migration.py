"""Restart-safe legacy Manager import into the V3 State DB."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from apex.domain.ids import derived_id, is_id
from .repositories.manager import ManagerRepository, ManagerStateError


_MARKER = "manager_state_legacy_import_v1"

_POSITION_FIELD_MAP = {
    "symbol": "symbol", "strategy": "strategy", "direction": "direction",
    "management_tf": "management_tf", "initial_entry": "initial_entry",
    "initial_sl": "initial_sl", "initial_tp1": "initial_tp1",
    "initial_tp2": "initial_tp2", "initial_tp3": "initial_tp3",
    "initial_rr": "initial_rr", "manager_version": "manager_version",
    "status": "status", "manager_state": "manager_state",
    "position_fraction": "position_fraction", "partial_exit_done": "partial_exit_done",
    "last_price": "last_price", "best_price": "best_price", "current_r": "current_r",
    "tp1_seen": "tp1_seen", "tp2_seen": "tp2_seen", "tp3_seen": "tp3_seen",
    "manager_target": "manager_target",
    "manager_protect_level": "confirmed_protect_level",
    "proposed_protect_level": "proposed_protect_level", "last_event": "last_event",
    "last_action": "last_action", "last_confidence": "last_confidence",
    "last_reviewed_candle": "last_reviewed_candle",
    "no_progress_bars": "no_progress_bars", "progress_anchor_r": "progress_anchor_r",
    "last_progress_candle": "last_progress_candle",
    "data_failure_count": "data_failure_count",
    "data_failure_notified": "data_failure_notified",
    "last_data_error": "last_data_error", "pre_degraded_state": "pre_degraded_state",
    "reconciliation_reason": "reconciliation_reason", "closed_at": "closed_at",
    "close_result": "close_result", "exit_price": "exit_price",
    "realized_pct": "realized_pct", "realized_r": "realized_r",
}

_EVENT_FIELD_MAP = {
    "event_type": "event_type", "action": "action", "confidence": "confidence",
    "price": "price", "r_multiple": "r_multiple", "manager_target": "manager_target",
    "manager_protect_level": "confirmed_protect_level", "reason": "summary",
}

_LEGACY_POSITION_DEFAULTS = {
    "manager_version": 2, "status": "ACTIVE", "manager_state": "PROTECTED",
    "position_fraction": 1.0, "partial_exit_done": 0, "current_r": 0,
    "tp1_seen": 0, "tp2_seen": 0, "tp3_seen": 0,
    "no_progress_bars": 0, "progress_anchor_r": 0,
    "data_failure_count": 0, "data_failure_notified": 0,
}

_IMMUTABLE_POSITION_FIELDS = frozenset({
    "symbol", "strategy", "direction", "management_tf", "initial_entry",
    "initial_sl", "initial_tp1", "initial_tp2", "initial_tp3", "initial_rr",
    "manager_version",
})


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]) for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _as_dict(row: sqlite3.Row, columns: tuple[str, ...]) -> dict[str, Any]:
    return dict(row) if isinstance(row, sqlite3.Row) else dict(zip(columns, row))


def _event_id(row: dict[str, Any]) -> str:
    candidate = str(row.get("manager_event_id") or "")
    return candidate if is_id(candidate, "manager_event") else derived_id(
        "manager_event", "legacy", row.get("id")
    )


def _position_projection(
    row: dict[str, Any], live_execution_ids: set[int], closed_signal_ids: set[int],
) -> dict[str, Any]:
    """Prevent a candle-derived legacy close from becoming a State outcome."""
    projected = dict(row)
    signal_id = int(projected.get("signal_id") or 0)
    closed = str(projected.get("status") or "ACTIVE").upper() == "CLOSED"
    last_event = str(projected.get("last_event") or "").upper()
    not_opened = str(projected.get("close_result") or "").upper().startswith("NOT_OPENED:")
    exchange_close = last_event in {
        "CONFIRMED_BINANCE_FILLS", "CONFIRMED_BINANCE_FILLS_ACCOUNTING_PENDING",
    }
    awaiting_live_close = signal_id in live_execution_ids and (
        closed or signal_id in closed_signal_ids
    )
    if awaiting_live_close and not not_opened and not exchange_close:
        projected.update({
            "status": "CLOSING",
            "manager_state": "RECONCILIATION_REQUIRED",
            "close_result": None,
            "exit_price": None,
            "realized_pct": None,
            "realized_r": None,
            "closed_at": None,
            "last_event": "AWAITING_CONFIRMED_BINANCE_CLOSE",
        })
    return projected


def import_legacy_manager(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
    *,
    refresh: bool = False,
) -> dict[str, int | bool]:
    """Copy Manager history once without deleting or mutating its source.

    The completion marker is written only after positions and events are both
    durable. A crash before the marker is safe because position geometry and
    imported event IDs are idempotent.
    """
    state = state_factory()
    try:
        marker = state.execute(
            "SELECT value_json FROM runtime_state WHERE key=?", (_MARKER,),
        ).fetchone()
        if marker is not None and not refresh:
            return {"already_complete": True, "positions": 0, "events": 0}
    finally:
        state.close()

    legacy = legacy_factory()
    try:
        legacy.row_factory = sqlite3.Row
        available = _tables(legacy)
        if "trade_manager_state" not in available:
            positions: list[dict[str, Any]] = []
        else:
            cursor = legacy.execute("SELECT * FROM trade_manager_state ORDER BY signal_id")
            columns = tuple(item[0] for item in cursor.description)
            positions = [_as_dict(row, columns) for row in cursor.fetchall()]
        if "trade_manager_events" not in available:
            events: list[dict[str, Any]] = []
        else:
            cursor = legacy.execute("SELECT * FROM trade_manager_events ORDER BY id")
            columns = tuple(item[0] for item in cursor.description)
            events = [_as_dict(row, columns) for row in cursor.fetchall()]
        runtime_rows = (
            legacy.execute("SELECT key,value FROM trade_manager_runtime").fetchall()
            if "trade_manager_runtime" in available else []
        )
        live_execution_ids = {
            int(item[0]) for item in legacy.execute(
                "SELECT signal_id FROM trade_executions WHERE mode='live'"
            ).fetchall()
        } if "trade_executions" in available else set()
        closed_signal_ids = {
            int(item[0]) for item in legacy.execute(
                "SELECT id FROM signals WHERE LOWER(COALESCE(result,'pending'))!='pending'"
            ).fetchall()
        } if "signals" in available else set()
    finally:
        legacy.close()

    repository = ManagerRepository(state_factory)
    imported_positions = 0
    for row in positions:
        row = _position_projection(row, live_execution_ids, closed_signal_ids)
        payload = {
            "signal_id": row.get("signal_id"), "symbol": row.get("symbol"),
            "strategy": row.get("strategy"), "direction": row.get("direction"),
            "management_tf": row.get("management_tf"),
            "initial_entry": row.get("initial_entry"), "initial_sl": row.get("initial_sl"),
            "initial_tp1": row.get("initial_tp1"), "initial_tp2": row.get("initial_tp2"),
            "initial_tp3": row.get("initial_tp3"), "initial_rr": row.get("initial_rr"),
            "manager_version": row.get("manager_version") or 2,
        }
        try:
            payload["thesis"] = json.loads(str(row.get("thesis_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload["thesis"] = {"legacy_thesis_unreadable": True}
        created = repository.register(payload)
        if created:
            imported_positions += 1
            signal_entity_id = str(repository.get(int(row["signal_id"]))["signal_entity_id"])
            target = state_factory()
            try:
                target.execute(
                """UPDATE manager_positions SET
                    status=?,manager_state=?,position_fraction=?,partial_exit_done=?,
                    last_price=?,best_price=?,current_r=?,tp1_seen=?,tp2_seen=?,tp3_seen=?,
                    manager_target=?,confirmed_protect_level=?,proposed_protect_level=?,
                    last_event=?,last_action=?,last_confidence=?,last_reviewed_candle=?,
                    no_progress_bars=?,progress_anchor_r=?,last_progress_candle=?,
                    data_failure_count=?,data_failure_notified=?,last_data_error=?,
                    pre_degraded_state=?,reconciliation_reason=?,created_at=COALESCE(?,created_at),
                    updated_at=COALESCE(?,updated_at),closed_at=?,close_result=?,
                    exit_price=?,realized_pct=?,realized_r=? WHERE signal_entity_id=?""",
                    (
                        row.get("status") or "ACTIVE", row.get("manager_state") or "PROTECTED",
                        row.get("position_fraction") if row.get("position_fraction") is not None else 1.0,
                        int(row.get("partial_exit_done") or 0), row.get("last_price"), row.get("best_price"),
                        row.get("current_r") or 0, int(row.get("tp1_seen") or 0),
                        int(row.get("tp2_seen") or 0), int(row.get("tp3_seen") or 0),
                        row.get("manager_target"), row.get("manager_protect_level"),
                        row.get("proposed_protect_level"), row.get("last_event"), row.get("last_action"),
                        row.get("last_confidence"), row.get("last_reviewed_candle"),
                        int(row.get("no_progress_bars") or 0), row.get("progress_anchor_r") or 0,
                        row.get("last_progress_candle"), int(row.get("data_failure_count") or 0),
                        int(row.get("data_failure_notified") or 0), row.get("last_data_error"),
                        row.get("pre_degraded_state"), row.get("reconciliation_reason"),
                        row.get("created_at"), row.get("updated_at"), row.get("closed_at"),
                        row.get("close_result"), row.get("exit_price"), row.get("realized_pct"),
                        row.get("realized_r"), signal_entity_id,
                    ),
                )
                target.commit()
            finally:
                target.close()

    imported_events = 0
    known = {int(row["signal_id"]) for row in positions}
    for row in events:
        signal_id = int(row.get("signal_id") or 0)
        if signal_id not in known:
            raise ManagerStateError(f"manager_event_orphan:{row.get('id')}")
        try:
            facts = json.loads(str(row.get("facts_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            facts = {"legacy_facts_unreadable": True}
        if repository.append_event({
            "manager_event_id": _event_id(row),
            "signal_id": signal_id, "event_type": row.get("event_type") or "UNKNOWN",
            "action": row.get("action"), "confidence": row.get("confidence"),
            "price": row.get("price"), "r_multiple": row.get("r_multiple"),
            "manager_target": row.get("manager_target"),
            "confirmed_protect_level": row.get("manager_protect_level"),
            "facts": facts, "summary": row.get("reason") or "",
            "reason_codes": ("LEGACY_MANAGER_EVENT",),
        }):
            imported_events += 1

    state = state_factory()
    try:
        for key, value in runtime_rows:
            state.execute(
                """INSERT INTO runtime_state(key,value_json) VALUES(?,?)
                   ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                     updated_at=CURRENT_TIMESTAMP""",
                (f"manager_runtime:{str(key)}", json.dumps(str(value))),
            )
        target_positions = int(state.execute(
            "SELECT COUNT(*) FROM manager_positions WHERE signal_id IN (%s)" % (
                ",".join("?" for _ in known) or "NULL"
            ),
            tuple(sorted(known)),
        ).fetchone()[0])
        imported_ids = tuple(
            _event_id(row) for row in events
        )
        target_events = int(state.execute(
            "SELECT COUNT(*) FROM manager_events WHERE manager_event_id IN (%s)" % (
                ",".join("?" for _ in imported_ids) or "NULL"
            ),
            imported_ids,
        ).fetchone()[0])
        if target_positions != len(positions) or target_events != len(events):
            raise ManagerStateError(
                "manager_import_verification_failed:"
                f"positions={target_positions}/{len(positions)};"
                f"events={target_events}/{len(events)}"
            )
        state.execute(
            """INSERT INTO runtime_state(key,value_json) VALUES(?,?)
               ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                 updated_at=CURRENT_TIMESTAMP""",
            (_MARKER, json.dumps({"positions": len(positions), "events": len(events)})),
        )
        state.commit()
    finally:
        state.close()
    return {
        "already_complete": False, "positions": imported_positions,
        "events": imported_events,
    }


def manager_parity_report(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
) -> dict[str, Any]:
    """Verify that the immutable legacy import is a subset of canonical State.

    Mutable Manager state intentionally diverges after cutover because normal
    production writes no longer flow back to the compatibility database.
    """
    legacy = legacy_factory()
    state = state_factory()
    mismatches: list[str] = []
    try:
        legacy.row_factory = sqlite3.Row
        state.row_factory = sqlite3.Row
        available = _tables(legacy)
        legacy_positions = {
            int(row["signal_id"]): dict(row) for row in (
                legacy.execute("SELECT * FROM trade_manager_state").fetchall()
                if "trade_manager_state" in available else ()
            )
        }
        live_execution_ids = {
            int(item[0]) for item in legacy.execute(
                "SELECT signal_id FROM trade_executions WHERE mode='live'"
            ).fetchall()
        } if "trade_executions" in available else set()
        closed_signal_ids = {
            int(item[0]) for item in legacy.execute(
                "SELECT id FROM signals WHERE LOWER(COALESCE(result,'pending'))!='pending'"
            ).fetchall()
        } if "signals" in available else set()
        target_positions = {
            int(row["signal_id"]): dict(row)
            for row in state.execute("SELECT * FROM manager_positions").fetchall()
        }
        if not set(legacy_positions).issubset(target_positions):
            mismatches.append("position_identity_set")
        for signal_id in sorted(set(legacy_positions) & set(target_positions)):
            source = _position_projection(
                legacy_positions[signal_id], live_execution_ids, closed_signal_ids,
            )
            target = target_positions[signal_id]
            for source_name, target_name in _POSITION_FIELD_MAP.items():
                if source_name not in _IMMUTABLE_POSITION_FIELDS:
                    continue
                source_value = source.get(source_name)
                if source_value is None and source_name in _LEGACY_POSITION_DEFAULTS:
                    source_value = _LEGACY_POSITION_DEFAULTS[source_name]
                if source_value is None and source_name in {"initial_tp2", "initial_tp3"}:
                    source_value = source.get("initial_tp2") or source.get("initial_tp1")
                if source_value != target.get(target_name):
                    mismatches.append(f"position:{signal_id}:{source_name}")
        legacy_events = {
            _event_id(dict(row)): dict(row)
            for row in (
                legacy.execute("SELECT * FROM trade_manager_events").fetchall()
                if "trade_manager_events" in available else ()
            )
        }
        target_events = {
            str(row["manager_event_id"]): dict(row)
            for row in state.execute(
                "SELECT * FROM manager_events WHERE reason_codes_json=?",
                (json.dumps(("LEGACY_MANAGER_EVENT",), separators=(",", ":")),),
            ).fetchall()
        }
        if not set(legacy_events).issubset(target_events):
            mismatches.append("event_identity_set")
        for event_id in sorted(set(legacy_events) & set(target_events)):
            source, target = legacy_events[event_id], target_events[event_id]
            if int(source.get("signal_id") or 0) != int(target.get("signal_id") or 0):
                mismatches.append(f"event:{event_id}:signal_id")
            for source_name, target_name in _EVENT_FIELD_MAP.items():
                if source.get(source_name) != target.get(target_name):
                    mismatches.append(f"event:{event_id}:{source_name}")
            try:
                source_facts = json.loads(str(source.get("facts_json") or "{}"))
                target_facts = json.loads(str(target.get("facts_json") or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                mismatches.append(f"event:{event_id}:facts_json")
            else:
                if source_facts != target_facts:
                    mismatches.append(f"event:{event_id}:facts_json")
    finally:
        legacy.close()
        state.close()
    return {
        "ok": not mismatches,
        "positions": len(legacy_positions),
        "events": len(legacy_events),
        "mismatches": mismatches[:100],
    }


__all__ = ["import_legacy_manager", "manager_parity_report"]
