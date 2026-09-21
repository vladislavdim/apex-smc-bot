"""Production-owned execution intents, exchange state and action claims."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Callable, Mapping

from apex.domain.ids import derived_id, is_id


class ExecutionStateError(RuntimeError):
    pass


class ExecutionRepository:
    def __init__(self, conn_factory: Callable[[], sqlite3.Connection]) -> None:
        self._conn_factory = conn_factory

    @staticmethod
    def _plan(values: Mapping[str, Any]) -> tuple[str, str]:
        plan = {
            key: values.get(key) for key in (
                "signal_id", "signal_entity_id", "mode", "exchange",
                "symbol", "direction", "entry", "sl", "tp1", "tp2", "tp3",
                "quantity", "risk_usdt", "balance_usdt", "leverage",
            )
        }
        encoded = json.dumps(plan, sort_keys=True, separators=(",", ":"), default=str)
        return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def bind_identity(
        self, signal_id: int, *, execution_id: str | None = None,
        candidate_id: str | None = None, position_id: str | None = None,
    ) -> bool:
        """Bind correlation IDs once; a different later value is rejected."""
        supplied = {
            key: value for key, value in {
                "execution_id": execution_id, "candidate_id": candidate_id,
                "position_id": position_id,
            }.items() if value
        }
        if not supplied:
            return False
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT execution_id,candidate_id,position_id,signal_entity_id "
                "FROM executions WHERE signal_id=?",
                (int(signal_id),),
            ).fetchone()
            if row is None:
                raise ExecutionStateError("execution_not_found")
            for key, value in supplied.items():
                if row[key] not in (None, "") and str(row[key]) != str(value):
                    raise ExecutionStateError(f"execution_identity_conflict:{key}")
            assignments = ",".join(f"{key}=COALESCE({key},?)" for key in supplied)
            conn.execute(
                f"UPDATE executions SET {assignments},updated_at=CURRENT_TIMESTAMP WHERE signal_id=?",
                (*supplied.values(), int(signal_id)),
            )
            bound_candidate = candidate_id or row["candidate_id"]
            if bound_candidate and not row["signal_entity_id"]:
                correlated = conn.execute(
                    "SELECT signal_id FROM trade_correlation WHERE candidate_id=?",
                    (str(bound_candidate),),
                ).fetchone()
                if correlated and is_id(correlated[0], "signal"):
                    conn.execute(
                        "UPDATE executions SET signal_entity_id=? WHERE signal_id=?",
                        (str(correlated[0]), int(signal_id)),
                    )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def register(self, values: Mapping[str, Any]) -> bool:
        required = ("signal_id", "mode", "symbol", "direction", "status")
        missing = [key for key in required if values.get(key) in (None, "")]
        if missing:
            raise ExecutionStateError("execution_missing:" + ",".join(missing))
        signal_id = int(values["signal_id"])
        if signal_id <= 0:
            raise ExecutionStateError("execution_invalid:signal_id")
        for field, entity in (
            ("signal_entity_id", "signal"),
            ("execution_id", "execution"), ("candidate_id", "candidate"),
            ("position_id", "position"),
        ):
            value = values.get(field)
            if value and not is_id(value, entity):
                raise ExecutionStateError(f"execution_invalid:{field}")
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            signal_entity_id = str(values.get("signal_entity_id") or "")
            if not signal_entity_id and values.get("candidate_id"):
                correlated = conn.execute(
                    "SELECT signal_id FROM trade_correlation WHERE candidate_id=?",
                    (str(values["candidate_id"]),),
                ).fetchone()
                if correlated and is_id(correlated[0], "signal"):
                    signal_entity_id = str(correlated[0])
            if not signal_entity_id:
                signal_entity_id = derived_id("signal", "legacy-execution", signal_id)
            if not is_id(signal_entity_id, "signal"):
                raise ExecutionStateError("execution_invalid:signal_entity_id")
            normalized = dict(values)
            normalized["signal_entity_id"] = signal_entity_id
            plan_json, plan_hash = self._plan(normalized)
            existing = conn.execute(
                "SELECT plan_hash FROM executions WHERE signal_id=?", (signal_id,),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != plan_hash:
                    raise ExecutionStateError("execution_plan_conflict")
                conn.commit()
                return False
            conn.execute(
                """INSERT INTO executions(
                    signal_id,signal_entity_id,execution_id,candidate_id,mode,exchange,symbol,direction,status,
                    entry,sl,tp1,tp2,tp3,quantity,risk_usdt,balance_usdt,leverage,
                    entry_order_id,stop_order_id,tp1_order_id,tp2_order_id,active_stop_price,
                    pending_stop_order_id,previous_stop_order_id,last_error,plan_json,plan_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    signal_id, signal_entity_id, values.get("execution_id"), values.get("candidate_id"),
                    str(values["mode"]), str(values.get("exchange") or "binance_futures"),
                    str(values["symbol"]).upper(), str(values["direction"]).upper(),
                    str(values["status"]), values.get("entry"), values.get("sl"),
                    values.get("tp1"), values.get("tp2"), values.get("tp3"),
                    values.get("quantity"), values.get("risk_usdt"), values.get("balance_usdt"),
                    values.get("leverage"), values.get("entry_order_id"), values.get("stop_order_id"),
                    values.get("tp1_order_id"), values.get("tp2_order_id"),
                    values.get("active_stop_price"), values.get("pending_stop_order_id"),
                    values.get("previous_stop_order_id"), values.get("last_error"),
                    plan_json, plan_hash,
                ),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get(self, signal_id: int) -> dict[str, Any] | None:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM executions WHERE signal_id=?", (int(signal_id),),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_by_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        if not is_id(candidate_id, "candidate"):
            raise ExecutionStateError("execution_invalid:candidate_id")
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM executions WHERE candidate_id=?", (candidate_id,),
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
                "SELECT * FROM executions WHERE signal_id IN (%s)" % ",".join("?" for _ in ids),
                ids,
            ).fetchall()
            return {int(row["signal_id"]): dict(row) for row in rows}
        finally:
            conn.close()

    def requiring_reconciliation(self, statuses: tuple[str, ...]) -> list[dict[str, Any]]:
        """Return live exchange rows selected by canonical State status."""
        normalized = tuple(sorted({str(value).upper() for value in statuses if value}))
        if not normalized:
            return []
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM executions
                   WHERE mode='live' AND status IN (%s)
                   ORDER BY signal_id""" % ",".join("?" for _ in normalized),
                normalized,
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def status_summary(self, active_statuses: tuple[str, ...]) -> dict[str, Any]:
        """Return Dashboard counts from State without consulting compatibility DB."""
        normalized = tuple(sorted({str(value).upper() for value in active_statuses if value}))
        conn = self._conn_factory()
        try:
            counts = {
                str(status): int(count)
                for status, count in conn.execute(
                    "SELECT status,COUNT(*) FROM executions GROUP BY status"
                ).fetchall()
            }
            live_active = 0
            if normalized:
                live_active = int(conn.execute(
                    """SELECT COUNT(*) FROM executions
                       WHERE mode='live' AND status IN (%s)"""
                    % ",".join("?" for _ in normalized),
                    normalized,
                ).fetchone()[0])
            return {"counts": counts, "live_active_count": live_active}
        finally:
            conn.close()

    def update_exchange_state(self, signal_id: int, **changes: Any) -> bool:
        allowed = {
            "position_id", "status", "entry_order_id", "stop_order_id", "tp1_order_id",
            "tp2_order_id", "active_stop_price", "pending_stop_order_id",
            "previous_stop_order_id", "last_error", "quantity",
        }
        invalid = set(changes) - allowed
        if invalid:
            raise ExecutionStateError("execution_immutable_field:" + ",".join(sorted(invalid)))
        if not changes:
            return False
        assignments = ",".join(f"{key}=?" for key in changes)
        conn = self._conn_factory()
        try:
            cursor = conn.execute(
                f"UPDATE executions SET {assignments},updated_at=CURRENT_TIMESTAMP WHERE signal_id=?",
                (*changes.values(), int(signal_id)),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def claim_action(
        self, action_key: str, signal_id: int, action: str, requested_level: float | None,
    ) -> bool:
        conn = self._conn_factory()
        try:
            owner = conn.execute(
                "SELECT signal_entity_id FROM executions WHERE signal_id=?",
                (int(signal_id),),
            ).fetchone()
            if owner is None or not is_id(owner[0], "signal"):
                raise ExecutionStateError("execution_action_owner_missing")
            cursor = conn.execute(
                """INSERT INTO execution_actions(
                    action_key,signal_id,signal_entity_id,action,status,requested_level
                ) VALUES(?,?,?,?,'PROCESSING',?)
                ON CONFLICT(action_key) DO UPDATE SET status='PROCESSING',error=NULL,
                  updated_at=CURRENT_TIMESTAMP
                WHERE execution_actions.status='ERROR'
                  AND execution_actions.updated_at<=datetime('now','-1 minute')""",
                (
                    str(action_key), int(signal_id), str(owner[0]),
                    str(action), requested_level,
                ),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def finish_action(
        self, action_key: str, status: str, *, order_id: str = "", error: str = "",
    ) -> bool:
        conn = self._conn_factory()
        try:
            cursor = conn.execute(
                """UPDATE execution_actions SET status=?,exchange_order_id=?,error=?,
                   updated_at=CURRENT_TIMESTAMP WHERE action_key=?""",
                (str(status), str(order_id or ""), str(error or "")[:1000], str(action_key)),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def latest_action(
        self, signal_id: int, actions: tuple[str, ...],
    ) -> dict[str, Any] | None:
        """Return the latest canonical action for restart reconciliation."""
        normalized = tuple(sorted({str(value).upper() for value in actions if value}))
        if not normalized:
            return None
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT * FROM execution_actions
                   WHERE signal_id=? AND action IN (%s)
                   ORDER BY created_at DESC,action_key DESC LIMIT 1"""
                % ",".join("?" for _ in normalized),
                (int(signal_id), *normalized),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def import_action(self, values: Mapping[str, Any]) -> bool:
        """Import an existing action exactly; retries must match its identity."""
        required = ("action_key", "signal_id", "action", "status")
        missing = [key for key in required if values.get(key) in (None, "")]
        if missing:
            raise ExecutionStateError("execution_action_missing:" + ",".join(missing))
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT signal_id,action,requested_level FROM execution_actions
                   WHERE action_key=?""", (str(values["action_key"]),),
            ).fetchone()
            identity = (
                int(values["signal_id"]), str(values["action"]), values.get("requested_level"),
            )
            if existing is not None:
                if tuple(existing) != identity:
                    raise ExecutionStateError("execution_action_identity_conflict")
                conn.execute(
                    """UPDATE execution_actions SET status=?,exchange_order_id=?,error=?,
                       updated_at=COALESCE(?,updated_at) WHERE action_key=?""",
                    (
                        str(values["status"]), values.get("exchange_order_id"),
                        values.get("error"), values.get("updated_at"),
                        str(values["action_key"]),
                    ),
                )
                conn.commit()
                return False
            conn.execute(
                """INSERT INTO execution_actions(
                    action_key,signal_id,action,status,requested_level,exchange_order_id,
                    error,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,COALESCE(?,CURRENT_TIMESTAMP),COALESCE(?,CURRENT_TIMESTAMP))""",
                (
                    str(values["action_key"]), int(values["signal_id"]), str(values["action"]),
                    str(values["status"]), values.get("requested_level"),
                    values.get("exchange_order_id"), values.get("error"),
                    values.get("created_at"), values.get("updated_at"),
                ),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


__all__ = ["ExecutionRepository", "ExecutionStateError"]
