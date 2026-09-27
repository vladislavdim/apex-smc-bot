"""State-owned delivered-signal lifecycle projection."""

from __future__ import annotations

import sqlite3
from typing import Any, Callable, Mapping

from apex.domain.ids import derived_id, is_id


class SignalLifecycleStateError(RuntimeError):
    pass


class SignalLifecycleRepository:
    _STATUSES = frozenset({"waiting_entry", "active", "closed", "cancelled"})
    _IMMUTABLE_FACTS = (
        "symbol", "direction", "signal_type", "timeframe", "entry", "sl",
        "tp1", "tp2", "tp3", "estimated_hours", "grade",
    )

    def __init__(self, conn_factory: Callable[[], sqlite3.Connection]) -> None:
        self._conn_factory = conn_factory

    def import_row(self, values: Mapping[str, Any]) -> bool:
        signal_id = int(values.get("signal_id") or 0)
        status = str(values.get("status") or "").lower()
        if signal_id <= 0 or status not in self._STATUSES:
            raise SignalLifecycleStateError("signal_lifecycle_invalid")
        result = str(values.get("result") or "pending").lower()
        conn = self._conn_factory()
        original_row_factory = conn.row_factory
        try:
            conn.row_factory = sqlite3.Row
            supplied = str(values.get("signal_entity_id") or "")
            if supplied and not is_id(supplied, "signal"):
                raise SignalLifecycleStateError("signal_lifecycle_invalid_identity")
            existing = conn.execute(
                "SELECT * FROM signal_lifecycle WHERE signal_id=?",
                (signal_id,),
            ).fetchone()
            execution = conn.execute(
                "SELECT signal_entity_id FROM executions WHERE signal_id=?",
                (signal_id,),
            ).fetchone()
            canonical = str(
                (existing["signal_entity_id"] if existing else None)
                or (execution[0] if execution else None)
                or supplied
                or derived_id("signal", "legacy-signal", signal_id)
            )
            if not is_id(canonical, "signal") or (supplied and supplied != canonical):
                raise SignalLifecycleStateError("signal_lifecycle_identity_conflict")
            facts = {
                field: (str(values.get(field) or "").upper() or None)
                if field in {"symbol", "direction", "signal_type"}
                else values.get(field)
                for field in self._IMMUTABLE_FACTS
            }
            if existing:
                for field, value in facts.items():
                    if existing[field] is not None and value is not None and existing[field] != value:
                        raise SignalLifecycleStateError("signal_lifecycle_geometry_conflict:" + field)
                if existing["ownership"] == "state":
                    # Historical import must never roll a State outcome back.
                    return False
            cursor = conn.execute(
                """INSERT INTO signal_lifecycle(
                    signal_entity_id,signal_id,status,result,activated_at,last_checked_at,closed_at,
                    cancel_reason,created_at,updated_at,symbol,direction,signal_type,timeframe,
                    entry,sl,tp1,tp2,tp3,estimated_hours,grade,tp1_hit,trailing_sl,
                    best_price,confluence,regime
                ) VALUES(?,?,?,?,?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP),COALESCE(?,CURRENT_TIMESTAMP),
                         ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(signal_id) DO UPDATE SET
                    status=excluded.status,result=excluded.result,
                    activated_at=excluded.activated_at,
                    last_checked_at=excluded.last_checked_at,
                    closed_at=excluded.closed_at,cancel_reason=excluded.cancel_reason,
                    updated_at=excluded.updated_at,
                    symbol=COALESCE(signal_lifecycle.symbol,excluded.symbol),
                    direction=COALESCE(signal_lifecycle.direction,excluded.direction),
                    signal_type=COALESCE(signal_lifecycle.signal_type,excluded.signal_type),
                    timeframe=COALESCE(signal_lifecycle.timeframe,excluded.timeframe),
                    entry=COALESCE(signal_lifecycle.entry,excluded.entry),
                    sl=COALESCE(signal_lifecycle.sl,excluded.sl),
                    tp1=COALESCE(signal_lifecycle.tp1,excluded.tp1),
                    tp2=COALESCE(signal_lifecycle.tp2,excluded.tp2),
                    tp3=COALESCE(signal_lifecycle.tp3,excluded.tp3),
                    estimated_hours=COALESCE(signal_lifecycle.estimated_hours,excluded.estimated_hours),
                    grade=COALESCE(signal_lifecycle.grade,excluded.grade),
                    tp1_hit=excluded.tp1_hit,trailing_sl=excluded.trailing_sl,
                    best_price=excluded.best_price,confluence=excluded.confluence,
                    regime=excluded.regime""",
                (
                    canonical, signal_id, status, result, values.get("activated_at"),
                    values.get("last_checked_at"), values.get("closed_at"),
                    values.get("cancel_reason"),
                    values.get("created_at") or values.get("signal_created_at"),
                    values.get("updated_at") or values.get("last_checked_at")
                    or values.get("closed_at"),
                    *(facts[field] for field in self._IMMUTABLE_FACTS),
                    values.get("tp1_hit"), values.get("trailing_sl"),
                    values.get("best_price"), values.get("confluence"), values.get("regime"),
                ),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.row_factory = original_row_factory
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

    def require_pending_for_execution(self, signal_id: int, symbol: str) -> dict[str, Any]:
        """A delivered signal must be mirrored to State before Binance admission."""
        row = self.get(signal_id)
        if (
            row is None
            or str(row.get("symbol") or "").upper() != str(symbol or "").upper()
            or str(row.get("status") or "").lower() != "waiting_entry"
            or str(row.get("result") or "").lower() != "pending"
        ):
            raise SignalLifecycleStateError("signal_lifecycle_not_ready_for_execution")
        return row

    def pending_for_monitor(self) -> list[dict[str, Any]]:
        """Read complete State-owned analytics inputs without legacy joins."""
        conn = self._conn_factory()
        original_row_factory = conn.row_factory
        try:
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(
                """SELECT * FROM signal_lifecycle WHERE result='pending'
                   ORDER BY signal_id"""
            ).fetchall()]
            for row in rows:
                required = (
                    "symbol", "direction", "signal_type", "timeframe", "entry", "sl",
                    "tp1", "tp2", "tp3", "created_at",
                )
                missing = [field for field in required if row.get(field) is None]
                if missing or row.get("status") not in {"waiting_entry", "active"}:
                    raise SignalLifecycleStateError(
                        "signal_monitor_incomplete:" + str(row["signal_id"]) + ":" + ",".join(missing)
                    )
            return rows
        finally:
            conn.row_factory = original_row_factory
            conn.close()

    def advance_monitor(
        self, signal_id: int, *, expected_status: str, transition: str,
        result: str | None = None, reason: str | None = None,
        tp1_hit: bool | None = None, trailing_sl: float | None = None,
        best_price: float | None = None,
    ) -> bool:
        """Commit one analytical transition only while the expected State row is current."""
        if transition not in {"touch", "activate", "progress", "close", "cancel"}:
            raise SignalLifecycleStateError("signal_monitor_transition_invalid")
        allowed = {
            "touch": {"waiting_entry", "active"},
            "activate": {"waiting_entry"},
            "progress": {"active"},
            "close": {"active"},
            "cancel": {"waiting_entry"},
        }
        if expected_status not in allowed[transition]:
            raise SignalLifecycleStateError("signal_monitor_status_invalid")
        if transition in {"close", "cancel"} and (
            not result or result == "pending" or (transition == "cancel" and result != "cancelled")
        ):
            raise SignalLifecycleStateError("signal_monitor_result_invalid")
        if transition not in {"close", "cancel"} and result is not None:
            raise SignalLifecycleStateError("signal_monitor_result_invalid")
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            assignments = ["last_checked_at=CURRENT_TIMESTAMP", "updated_at=CURRENT_TIMESTAMP",
                           "ownership='state'"]
            parameters: list[Any] = []
            if transition == "activate":
                assignments += ["status='active'", "activated_at=COALESCE(activated_at,CURRENT_TIMESTAMP)"]
            elif transition in {"close", "cancel"}:
                assignments += ["status=?", "result=?", "closed_at=CURRENT_TIMESTAMP", "cancel_reason=?"]
                parameters += ["cancelled" if transition == "cancel" else "closed", result, reason]
            elif transition == "progress":
                if tp1_hit is not None:
                    assignments.append("tp1_hit=?")
                    parameters.append(int(tp1_hit))
                if trailing_sl is not None:
                    assignments.append("trailing_sl=?")
                    parameters.append(float(trailing_sl))
                if best_price is not None:
                    assignments.append("best_price=?")
                    parameters.append(float(best_price))
            updated = conn.execute(
                f"UPDATE signal_lifecycle SET {','.join(assignments)} "
                "WHERE signal_id=? AND status=? AND result='pending'",
                (*parameters, int(signal_id), expected_status),
            ).rowcount
            conn.commit()
            return updated == 1
        except Exception:
            conn.rollback()
            raise
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
