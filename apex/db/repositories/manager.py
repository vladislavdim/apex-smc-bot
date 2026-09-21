"""Production Manager state repository.

The repository deliberately contains no joins to legacy ``signals`` or
``trade_executions``.  Reconciliation supplies exchange facts explicitly,
which allows Manager state to live in the small durable State DB.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Callable, Mapping

from apex.domain.ids import derived_id, is_id


class ManagerStateError(RuntimeError):
    pass


class ManagerRepository:
    def __init__(self, conn_factory: Callable[[], sqlite3.Connection]) -> None:
        self._conn_factory = conn_factory

    @staticmethod
    def _runtime_key(key: object) -> str:
        normalized = str(key or "").strip()
        if not normalized:
            raise ManagerStateError("manager_runtime_key_required")
        return f"manager_runtime:{normalized}"

    def runtime(self) -> dict[str, str]:
        conn = self._conn_factory()
        try:
            rows = conn.execute(
                "SELECT key,value_json FROM runtime_state WHERE key LIKE 'manager_runtime:%'"
            ).fetchall()
            result: dict[str, str] = {}
            for key, payload in rows:
                decoded = json.loads(str(payload))
                result[str(key).split(":", 1)[1]] = str(decoded)
            return result
        finally:
            conn.close()

    def set_runtime(self, key: str, value: Any) -> None:
        conn = self._conn_factory()
        try:
            conn.execute(
                """INSERT INTO runtime_state(key,value_json) VALUES(?,?)
                   ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                     updated_at=CURRENT_TIMESTAMP""",
                (self._runtime_key(key), json.dumps(str(value))),
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _encoded(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _signal_entity(
        conn: sqlite3.Connection, signal_id: int, supplied: object = None,
    ) -> str:
        value = str(supplied or "")
        if value and not is_id(value, "signal"):
            raise ManagerStateError("manager_position_invalid:signal_entity_id")
        execution = conn.execute(
            "SELECT signal_entity_id FROM executions WHERE signal_id=?",
            (int(signal_id),),
        ).fetchone()
        canonical = str(execution[0] or "") if execution else ""
        if canonical and value and canonical != value:
            raise ManagerStateError("manager_position_signal_identity_conflict")
        value = canonical or value or derived_id(
            "signal", "legacy-execution", int(signal_id)
        )
        if not is_id(value, "signal"):
            raise ManagerStateError("manager_position_invalid:signal_entity_id")
        return value

    def register(self, position: Mapping[str, Any]) -> bool:
        """Register immutable original geometry; retries are idempotent."""
        required = (
            "signal_id", "symbol", "strategy", "direction", "management_tf",
            "initial_entry", "initial_sl", "initial_tp1", "manager_version",
        )
        missing = [key for key in required if position.get(key) in (None, "")]
        if missing:
            raise ManagerStateError("manager_position_missing:" + ",".join(missing))
        signal_id = int(position["signal_id"])
        if signal_id <= 0:
            raise ManagerStateError("manager_position_invalid:signal_id")
        snapshot = {
            "signal_id": signal_id,
            "symbol": str(position["symbol"]).upper(),
            "strategy": str(position["strategy"]).upper(),
            "direction": str(position["direction"]).upper(),
            "management_tf": str(position["management_tf"]),
            "initial_entry": float(position["initial_entry"]),
            "initial_sl": float(position["initial_sl"]),
            "initial_tp1": float(position["initial_tp1"]),
            "initial_tp2": float(position.get("initial_tp2") or position["initial_tp1"]),
            "initial_tp3": float(position.get("initial_tp3") or position.get("initial_tp2") or position["initial_tp1"]),
            "initial_rr": float(position.get("initial_rr") or 0),
            "manager_version": int(position["manager_version"]),
        }
        snapshot_json = self._encoded(snapshot)
        snapshot_hash = hashlib.sha256(snapshot_json.encode("utf-8")).hexdigest()
        thesis_json = self._encoded(position.get("thesis") or {})[:20000]
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            signal_entity_id = self._signal_entity(
                conn, signal_id, position.get("signal_entity_id")
            )
            existing = conn.execute(
                """SELECT snapshot_hash,signal_entity_id FROM manager_positions
                    WHERE signal_entity_id=?""",
                (signal_entity_id,),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != snapshot_hash:
                    raise ManagerStateError("manager_position_geometry_conflict")
                if str(existing[1] or "") != signal_entity_id:
                    raise ManagerStateError("manager_position_signal_identity_conflict")
                conn.commit()
                return False
            conn.execute(
                """INSERT INTO manager_positions(
                    signal_id,signal_entity_id,symbol,strategy,direction,management_tf,
                    initial_entry,initial_sl,initial_tp1,initial_tp2,initial_tp3,
                    initial_rr,manager_version,thesis_json,snapshot_json,snapshot_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    signal_id, signal_entity_id, snapshot["symbol"], snapshot["strategy"], snapshot["direction"],
                    snapshot["management_tf"], snapshot["initial_entry"], snapshot["initial_sl"],
                    snapshot["initial_tp1"], snapshot["initial_tp2"], snapshot["initial_tp3"],
                    snapshot["initial_rr"], snapshot["manager_version"], thesis_json,
                    snapshot_json, snapshot_hash,
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
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            row = conn.execute(
                "SELECT * FROM manager_positions WHERE signal_entity_id=?",
                (signal_entity_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def update_thesis(self, signal_id: int, thesis: Mapping[str, Any]) -> bool:
        conn = self._conn_factory()
        try:
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            cursor = conn.execute(
                """UPDATE manager_positions SET thesis_json=?,updated_at=CURRENT_TIMESTAMP
                   WHERE signal_entity_id=?""",
                (self._encoded(thesis)[:20000], signal_entity_id),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def active(self, *, limit: int = 500) -> list[dict[str, Any]]:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM manager_positions WHERE status='ACTIVE'
                   ORDER BY updated_at,signal_id LIMIT ?""",
                (max(1, min(int(limit), 5000)),),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def reconciliation_required(self, *, limit: int = 5000) -> set[int]:
        """Return every exchange-reconciliation owner, including CLOSING rows."""
        conn = self._conn_factory()
        try:
            rows = conn.execute(
                """SELECT signal_id FROM manager_positions
                   WHERE manager_state='RECONCILIATION_REQUIRED'
                   ORDER BY signal_id LIMIT ?""",
                (max(1, min(int(limit), 5000)),),
            ).fetchall()
            return {int(row[0]) for row in rows}
        finally:
            conn.close()

    def confirm_reconciliation(self, signal_id: int, target_state: str) -> bool:
        """Resolve one exchange-verified cutover state idempotently."""
        target = str(target_state or "").upper()
        if target not in {"PROTECTED", "CLOSED"}:
            raise ManagerStateError("manager_reconciliation_target_invalid")
        conn = self._conn_factory()
        try:
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            cursor = conn.execute(
                """UPDATE manager_positions SET manager_state=?,reconciliation_reason=NULL,
                          updated_at=CURRENT_TIMESTAMP
                   WHERE signal_entity_id=? AND manager_state='RECONCILIATION_REQUIRED'""",
                (target, signal_entity_id),
            )
            if cursor.rowcount == 0:
                row = conn.execute(
                    "SELECT manager_state FROM manager_positions WHERE signal_entity_id=?",
                    (signal_entity_id,),
                ).fetchone()
                conn.commit()
                return bool(row and str(row[0]).upper() == target)
            conn.commit()
            return True
        finally:
            conn.close()

    def await_exchange_close(self, signal_id: int) -> bool:
        """Fence Manager after an analytical close until Binance proves closure."""
        conn = self._conn_factory()
        try:
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            cursor = conn.execute(
                """UPDATE manager_positions SET status='CLOSING',
                          manager_state='RECONCILIATION_REQUIRED',close_result=NULL,
                          exit_price=NULL,realized_pct=NULL,realized_r=NULL,closed_at=NULL,
                          last_event='AWAITING_CONFIRMED_BINANCE_CLOSE',
                          reconciliation_reason='SIGNAL_CLOSED_AWAITING_BINANCE',
                          updated_at=CURRENT_TIMESTAMP
                   WHERE signal_entity_id=? AND status!='CLOSED'""",
                (signal_entity_id,),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def close_not_opened(self, signal_id: int, execution_status: str) -> bool:
        """Close a historical phantom Manager row without fabricating PnL."""
        reason = f"NOT_OPENED:{str(execution_status or 'NO_CONFIRMED_LIVE_EXECUTION')}"
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            event_id = derived_id("manager_event", "not_opened", signal_entity_id, reason)
            cursor = conn.execute(
                """UPDATE manager_positions
                      SET status='CLOSED',manager_state='CLOSED',close_result=?,
                          exit_price=NULL,realized_pct=NULL,realized_r=NULL,
                          last_event='EXECUTION_NOT_OPENED',last_action='HOLD',
                          closed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                    WHERE signal_entity_id=? AND status!='CLOSED'""",
                (reason, signal_entity_id),
            )
            if cursor.rowcount:
                conn.execute(
                    """INSERT OR IGNORE INTO manager_events(
                        manager_event_id,signal_id,signal_entity_id,event_type,action,confidence,
                        facts_json,reason_codes_json,summary,execution_status
                    ) VALUES(?,?,?,'EXECUTION_NOT_OPENED','HOLD',1.0,?,?,?,?)""",
                    (
                        event_id, int(signal_id), signal_entity_id,
                        self._encoded({"execution_status": str(execution_status)}),
                        self._encoded(("BINANCE_ENTRY_NOT_CONFIRMED",)),
                        "Binance execution did not confirm a protected position",
                        str(execution_status),
                    ),
                )
            conn.commit()
            return cursor.rowcount == 1
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def recent(self, *, limit: int = 100) -> list[dict[str, Any]]:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM manager_positions
                   ORDER BY CASE WHEN status='ACTIVE' THEN 0 ELSE 1 END,
                            COALESCE(closed_at,updated_at) DESC,signal_id DESC
                   LIMIT ?""",
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def record_data_availability(
        self, signal_id: int, *, available: bool, error: str = "",
    ) -> tuple[int, bool]:
        """Track consecutive Gate failures without losing pre-degraded state."""
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.row_factory = sqlite3.Row
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            row = conn.execute(
                """SELECT data_failure_count,data_failure_notified,manager_state,
                          pre_degraded_state FROM manager_positions
                    WHERE signal_entity_id=?""",
                (signal_entity_id,),
            ).fetchone()
            if row is None:
                raise ManagerStateError("manager_position_not_found")
            if available:
                restored = (
                    str(row["pre_degraded_state"] or "PROTECTED")
                    if str(row["manager_state"]).upper() == "DEGRADED"
                    else str(row["manager_state"])
                )
                conn.execute(
                    """UPDATE manager_positions SET data_failure_count=0,
                              data_failure_notified=0,last_data_error=NULL,
                              manager_state=?,pre_degraded_state=NULL,
                              updated_at=CURRENT_TIMESTAMP WHERE signal_entity_id=?""",
                    (restored, signal_entity_id),
                )
                conn.commit()
                return 0, False
            count = int(row["data_failure_count"] or 0) + 1
            notified = bool(int(row["data_failure_notified"] or 0))
            alert_now = count >= 3 and not notified
            next_state = "DEGRADED" if count >= 3 else str(row["manager_state"])
            pre_degraded = row["pre_degraded_state"]
            if count >= 3 and str(row["manager_state"]).upper() != "DEGRADED":
                pre_degraded = str(row["manager_state"])
            conn.execute(
                """UPDATE manager_positions SET data_failure_count=?,
                          data_failure_notified=?,last_data_error=?,manager_state=?,
                          pre_degraded_state=?,updated_at=CURRENT_TIMESTAMP
                   WHERE signal_entity_id=?""",
                (
                    count, int(notified or alert_now), str(error or "")[:500],
                    next_state, pre_degraded, signal_entity_id,
                ),
            )
            conn.commit()
            return count, alert_now
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def append_event(self, event: Mapping[str, Any]) -> bool:
        event_id = str(event.get("manager_event_id") or "")
        if not is_id(event_id, "manager_event"):
            raise ManagerStateError("manager_event_invalid:id")
        signal_id = int(event.get("signal_id") or 0)
        if signal_id <= 0:
            raise ManagerStateError("manager_event_invalid:signal_id")
        conn = self._conn_factory()
        try:
            signal_entity_id = self._signal_entity(conn, signal_id)
            owner = conn.execute(
                "SELECT signal_entity_id FROM manager_positions WHERE signal_entity_id=?",
                (signal_entity_id,),
            ).fetchone()
            if owner is None or not is_id(owner[0], "signal"):
                raise ManagerStateError("manager_position_not_found")
            supplied = str(event.get("signal_entity_id") or "")
            if supplied and supplied != str(owner[0]):
                raise ManagerStateError("manager_event_signal_identity_mismatch")
            cursor = conn.execute(
                """INSERT OR IGNORE INTO manager_events(
                    manager_event_id,signal_id,signal_entity_id,event_type,action,confidence,price,
                    r_multiple,manager_target,confirmed_protect_level,facts_json,
                    reason_codes_json,summary,execution_status
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    event_id, signal_id, str(owner[0]), str(event.get("event_type") or "UNKNOWN"),
                    event.get("action"), event.get("confidence"), event.get("price"),
                    event.get("r_multiple"), event.get("manager_target"),
                    event.get("confirmed_protect_level"), self._encoded(event.get("facts") or {})[:20000],
                    self._encoded(tuple(event.get("reason_codes") or ())),
                    str(event.get("summary") or "")[:1500], event.get("execution_status"),
                ),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def events(self, signal_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            rows = conn.execute(
                """SELECT * FROM manager_events WHERE signal_entity_id=?
                   ORDER BY created_at DESC,manager_event_id DESC LIMIT ?""",
                (signal_entity_id, max(1, min(int(limit), 1000))),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def _event_values(
        event: Mapping[str, Any], encode: Callable[[Any], str], signal_entity_id: str,
    ) -> tuple[Any, ...]:
        event_id = str(event.get("manager_event_id") or "")
        if not is_id(event_id, "manager_event"):
            raise ManagerStateError("manager_event_invalid:id")
        return (
            event_id, int(event.get("signal_id") or 0), signal_entity_id,
            str(event.get("event_type") or "UNKNOWN"), event.get("action"),
            event.get("confidence"), event.get("price"), event.get("r_multiple"),
            event.get("manager_target"), event.get("confirmed_protect_level"),
            encode(event.get("facts") or {})[:20000],
            encode(tuple(event.get("reason_codes") or ())),
            str(event.get("summary") or "")[:1500], event.get("execution_status"),
        )

    def record_review(
        self,
        signal_id: int,
        observation: Mapping[str, Any],
        event: Mapping[str, Any],
    ) -> bool:
        """Atomically store one observation and its idempotent audit event."""
        if int(event.get("signal_id") or 0) != int(signal_id):
            raise ManagerStateError("manager_event_signal_mismatch")
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            row = conn.execute(
                """SELECT status,signal_entity_id FROM manager_positions
                    WHERE signal_entity_id=?""",
                (signal_entity_id,),
            ).fetchone()
            if row is None:
                raise ManagerStateError("manager_position_not_found")
            if str(row[0]).upper() == "CLOSED":
                raise ManagerStateError("manager_position_closed")
            stored_signal_entity_id = str(row[1] or "")
            if stored_signal_entity_id != signal_entity_id:
                raise ManagerStateError("manager_position_invalid:signal_entity_id")
            supplied = str(event.get("signal_entity_id") or "")
            if supplied and supplied != signal_entity_id:
                raise ManagerStateError("manager_event_signal_identity_mismatch")
            values = self._event_values(event, self._encoded, signal_entity_id)
            cursor = conn.execute(
                """INSERT OR IGNORE INTO manager_events(
                    manager_event_id,signal_id,signal_entity_id,event_type,action,confidence,price,
                    r_multiple,manager_target,confirmed_protect_level,facts_json,
                    reason_codes_json,summary,execution_status
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                values,
            )
            if cursor.rowcount != 1:
                conn.commit()
                return False
            # A review can propose protection but cannot confirm it. Binance
            # confirmation is accepted only by ``confirm_action`` below.
            conn.execute(
                """UPDATE manager_positions SET
                    last_price=?,best_price=?,current_r=?,tp1_seen=?,tp2_seen=?,tp3_seen=?,
                    manager_target=?,proposed_protect_level=?,last_event=?,last_action=?,
                    last_confidence=?,last_reviewed_candle=?,no_progress_bars=?,
                    progress_anchor_r=?,last_progress_candle=?,updated_at=CURRENT_TIMESTAMP
                   WHERE signal_entity_id=?""",
                (
                    observation.get("last_price"), observation.get("best_price"),
                    float(observation.get("current_r") or 0),
                    int(bool(observation.get("tp1_seen"))), int(bool(observation.get("tp2_seen"))),
                    int(bool(observation.get("tp3_seen"))), observation.get("manager_target"),
                    observation.get("proposed_protect_level"), observation.get("last_event"),
                    observation.get("last_action"), observation.get("last_confidence"),
                    observation.get("last_reviewed_candle"),
                    int(observation.get("no_progress_bars") or 0),
                    float(observation.get("progress_anchor_r") or 0),
                    observation.get("last_progress_candle"), signal_entity_id,
                ),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def confirm_action(
        self,
        signal_id: int,
        *,
        action: str,
        next_state: str,
        execution_status: str,
        confirmed_stop: float | None = None,
        remaining_fraction: float | None = None,
    ) -> bool:
        """Commit an already-eligible transition only after required confirmation."""
        canonical = str(action or "HOLD").upper()
        if canonical not in {
            "HOLD", "LET_RUN", "PROTECT", "MOVE_STOP_TO_BREAKEVEN", "PARTIAL_EXIT",
        }:
            raise ManagerStateError("manager_action_invalid")
        exchange_required = canonical in {
            "PROTECT", "MOVE_STOP_TO_BREAKEVEN", "PARTIAL_EXIT",
        }
        if exchange_required and str(execution_status or "").upper() != "EXECUTED":
            return False
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.row_factory = sqlite3.Row
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            row = conn.execute(
                """SELECT direction,initial_sl,confirmed_protect_level,last_price,
                          position_fraction,status FROM manager_positions
                    WHERE signal_entity_id=?""",
                (signal_entity_id,),
            ).fetchone()
            if row is None or str(row["status"]).upper() == "CLOSED":
                conn.rollback()
                return False
            if canonical in {"PROTECT", "MOVE_STOP_TO_BREAKEVEN"}:
                if confirmed_stop is None:
                    conn.rollback()
                    return False
                previous = float(row["confirmed_protect_level"] or row["initial_sl"])
                price = float(row["last_price"] or 0)
                level = float(confirmed_stop)
                bullish = str(row["direction"]).upper() in {"BULLISH", "LONG"}
                improves = previous < level < price if bullish else price < level < previous
                if not improves:
                    conn.rollback()
                    return False
            if canonical == "PARTIAL_EXIT":
                if remaining_fraction is None:
                    conn.rollback()
                    return False
                fraction = float(remaining_fraction)
                if not 0 <= fraction < float(row["position_fraction"]):
                    conn.rollback()
                    return False
            else:
                fraction = None
            conn.execute(
                """UPDATE manager_positions SET manager_state=?,
                    partial_exit_done=CASE WHEN ?='PARTIAL_EXIT' THEN 1 ELSE partial_exit_done END,
                    position_fraction=CASE WHEN ?='PARTIAL_EXIT' THEN ? ELSE position_fraction END,
                    confirmed_protect_level=CASE WHEN ? IN ('PROTECT','MOVE_STOP_TO_BREAKEVEN')
                      THEN ? ELSE confirmed_protect_level END,
                    proposed_protect_level=CASE WHEN ? IN ('PROTECT','MOVE_STOP_TO_BREAKEVEN')
                      THEN NULL ELSE proposed_protect_level END,
                    updated_at=CURRENT_TIMESTAMP WHERE signal_entity_id=?""",
                (
                    str(next_state), canonical, canonical, fraction, canonical,
                    confirmed_stop, canonical, signal_entity_id,
                ),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def close_from_accounting(
        self, signal_id: int, accounting: Mapping[str, Any], *, result: str,
    ) -> bool:
        """Close only from complete, confirmed Binance fill accounting."""
        basis = str(accounting.get("accounting_basis") or "")
        if not basis.startswith("confirmed_fills_after_commissions"):
            raise ManagerStateError("manager_close_unconfirmed_accounting")
        required = ("exit_price", "realized_pct", "net_r")
        if any(accounting.get(key) is None for key in required):
            raise ManagerStateError("manager_close_incomplete_accounting")
        conn = self._conn_factory()
        try:
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            cursor = conn.execute(
                """UPDATE manager_positions SET status='CLOSED',manager_state='CLOSED',
                    close_result=?,exit_price=?,realized_pct=?,realized_r=?,
                    last_price=?,current_r=?,last_event='CONFIRMED_BINANCE_FILLS',
                    closed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                   WHERE signal_entity_id=? AND (
                     status!='CLOSED'
                     OR last_event='CONFIRMED_BINANCE_FILLS_ACCOUNTING_PENDING'
                   )""",
                (
                    str(result), float(accounting["exit_price"]),
                    float(accounting["realized_pct"]), float(accounting["net_r"]),
                    float(accounting["exit_price"]), float(accounting["net_r"]),
                    signal_entity_id,
                ),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()

    def mark_exchange_closed(
        self, signal_id: int, accounting: Mapping[str, Any], *, result: str,
    ) -> bool:
        """Stop management after complete exit fills while final costs resolve."""
        if str(accounting.get("status") or "").upper() not in {"CLOSED", "FEES_UNRESOLVED"}:
            return False
        if accounting.get("exit_price") is None or accounting.get("exit_time") is None:
            raise ManagerStateError("manager_exchange_close_incomplete")
        conn = self._conn_factory()
        try:
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            cursor = conn.execute(
                """UPDATE manager_positions SET status='CLOSED',manager_state='CLOSED',
                    close_result=?,exit_price=?,realized_pct=NULL,realized_r=NULL,
                    last_price=?,current_r=0,last_event='CONFIRMED_BINANCE_FILLS_ACCOUNTING_PENDING',
                    closed_at=datetime(?/1000,'unixepoch'),updated_at=CURRENT_TIMESTAMP
                   WHERE signal_entity_id=? AND status!='CLOSED'""",
                (
                    str(result), float(accounting["exit_price"]),
                    float(accounting["exit_price"]), int(accounting["exit_time"]),
                    signal_entity_id,
                ),
            )
            conn.commit()
            return cursor.rowcount == 1
        finally:
            conn.close()


__all__ = ["ManagerRepository", "ManagerStateError"]
