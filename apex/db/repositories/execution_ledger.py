"""Canonical State repository for confirmed bot-owned Binance evidence."""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence

from apex.domain.ids import is_id


class ExecutionLedgerStateError(RuntimeError):
    pass


_KINDS = frozenset({"ENTRY", "SL", "TP1", "TP2", "PARTIAL_EXIT", "CLOSE"})
_SIDES = frozenset({"BUY", "SELL"})


def _number(value: object) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ExecutionLedgerStateError("ledger_invalid_number") from exc
    if not number.is_finite():
        raise ExecutionLedgerStateError("ledger_invalid_number")
    return str(number)


class ExecutionLedgerRepository:
    def __init__(self, conn_factory: Callable[[], sqlite3.Connection]) -> None:
        self._conn_factory = conn_factory

    @staticmethod
    def _signal_entity(
        conn: sqlite3.Connection, signal_id: int, supplied: object = None,
    ) -> str:
        row = conn.execute(
            "SELECT signal_entity_id FROM executions WHERE signal_id=?",
            (int(signal_id),),
        ).fetchone()
        if row is None or not is_id(row[0], "signal"):
            raise ExecutionLedgerStateError("ledger_execution_not_found")
        canonical = str(row[0])
        value = str(supplied or "")
        if value and (not is_id(value, "signal") or value != canonical):
            raise ExecutionLedgerStateError("ledger_signal_identity_mismatch")
        return canonical

    def register_order(self, values: Mapping[str, Any]) -> bool:
        signal_id = int(values.get("signal_id") or 0)
        symbol = str(values.get("symbol") or "").upper()
        kind = str(values.get("kind") or "").upper()
        remote_id = str(values.get("remote_id") or "")
        side = str(values.get("expected_side") or "").upper()
        if signal_id <= 0 or not symbol or not remote_id:
            raise ExecutionLedgerStateError("ledger_order_identity_missing")
        if kind not in _KINDS or side not in _SIDES:
            raise ExecutionLedgerStateError("ledger_order_contract_invalid")
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            signal_entity_id = self._signal_entity(
                conn, signal_id, values.get("signal_entity_id")
            )
            identity = (
                signal_id, signal_entity_id, symbol, kind, remote_id,
                int(bool(values.get("is_algo"))), side, values.get("standard_id"),
            )
            existing = conn.execute(
                """SELECT signal_id,signal_entity_id,symbol,kind,remote_id,is_algo,
                          expected_side,standard_id
                     FROM execution_orders
                    WHERE signal_entity_id=? AND kind=? AND remote_id=?""",
                (signal_entity_id, kind, remote_id),
            ).fetchone()
            if existing is not None and tuple(existing) != identity:
                raise ExecutionLedgerStateError("ledger_order_identity_conflict")
            cursor = conn.execute(
                """INSERT OR IGNORE INTO execution_orders(
                       signal_id,signal_entity_id,symbol,kind,remote_id,is_algo,
                       expected_side,standard_id
                   ) VALUES(?,?,?,?,?,?,?,?)""",
                identity,
            )
            conn.commit()
            return cursor.rowcount == 1
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record_fills(
        self, signal_id: int, order_id: str, kind: str,
        fills: Sequence[Mapping[str, Any]],
    ) -> int:
        normalized_kind = str(kind).upper()
        if normalized_kind not in _KINDS:
            raise ExecutionLedgerStateError("ledger_fill_kind_invalid")
        conn = self._conn_factory()
        inserted = 0
        try:
            conn.execute("BEGIN IMMEDIATE")
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            order = conn.execute(
                """SELECT symbol,expected_side,signal_entity_id FROM execution_orders
                    WHERE signal_entity_id=? AND kind=?
                      AND (remote_id=? OR standard_id=?)""",
                (signal_entity_id, normalized_kind, str(order_id), str(order_id)),
            ).fetchone()
            if order is None:
                raise ExecutionLedgerStateError("ledger_unregistered_order")
            symbol, expected_side = str(order[0]), str(order[1])
            order_signal_entity_id = str(order[2] or "")
            if order_signal_entity_id != signal_entity_id:
                raise ExecutionLedgerStateError("ledger_signal_identity_missing")
            for fill in fills:
                if str(fill.get("symbol") or "").upper() != symbol:
                    raise ExecutionLedgerStateError("ledger_fill_symbol_mismatch")
                if str(fill.get("side") or "").upper() != expected_side:
                    raise ExecutionLedgerStateError("ledger_fill_side_mismatch")
                if str(fill.get("orderId") or "") != str(order_id):
                    raise ExecutionLedgerStateError("ledger_fill_order_mismatch")
                trade_id = str(fill.get("id") or "")
                if not trade_id:
                    raise ExecutionLedgerStateError("ledger_fill_trade_id_missing")
                identity = (
                    symbol, trade_id, int(signal_id), signal_entity_id,
                    str(order_id), normalized_kind,
                    _number(fill.get("qty")), _number(fill.get("price")),
                    _number(fill.get("commission")),
                    str(fill.get("commissionAsset") or "").upper(),
                    int(fill.get("time") or 0),
                    json.dumps(dict(fill), sort_keys=True, separators=(",", ":"), default=str),
                )
                owner = conn.execute(
                    """SELECT symbol,trade_id,signal_id,signal_entity_id,order_id,kind,qty,price,commission,
                              commission_asset,time_ms,payload_json
                         FROM execution_fills WHERE symbol=? AND trade_id=?""",
                    (symbol, trade_id),
                ).fetchone()
                if owner is not None and tuple(owner) != identity:
                    raise ExecutionLedgerStateError("ledger_fill_identity_conflict")
                cursor = conn.execute(
                    """INSERT OR IGNORE INTO execution_fills(
                           symbol,trade_id,signal_id,signal_entity_id,order_id,kind,qty,price,commission,
                           commission_asset,time_ms,payload_json
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    identity,
                )
                inserted += int(cursor.rowcount == 1)
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_order_state(
        self, signal_id: int, kind: str, remote_id: str, **changes: Any,
    ) -> bool:
        allowed = {"standard_id", "complete", "checked_at", "error"}
        invalid = set(changes) - allowed
        if invalid:
            raise ExecutionLedgerStateError(
                "ledger_order_immutable_field:" + ",".join(sorted(invalid))
            )
        if not changes:
            return False
        assignments = ",".join(f"{field}=?" for field in changes)
        conn = self._conn_factory()
        try:
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            cursor = conn.execute(
                f"""UPDATE execution_orders SET {assignments},updated_at=CURRENT_TIMESTAMP
                     WHERE signal_entity_id=? AND kind=? AND remote_id=?""",
                (*changes.values(), signal_entity_id, str(kind).upper(), str(remote_id)),
            )
            conn.commit()
            if cursor.rowcount != 1:
                raise ExecutionLedgerStateError("ledger_order_not_found")
            return True
        finally:
            conn.close()

    def claim_order_poll(self, now: float) -> tuple[str, dict[str, Any] | None]:
        """Atomically rate-limit and claim one incomplete Binance order."""
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute(
                "SELECT attempted_at FROM execution_ledger_poll WHERE poll_kind='FILLS'"
            ).fetchone()
            if previous is not None and float(now) - float(previous[0]) < 60:
                conn.commit()
                return "DEFERRED", None
            row = conn.execute(
                """SELECT * FROM execution_orders
                     WHERE complete=0 AND checked_at<=?
                     ORDER BY checked_at,signal_id,kind,remote_id LIMIT 1""",
                (float(now) - 300,),
            ).fetchone()
            if row is None:
                conn.commit()
                return "IDLE", None
            conn.execute(
                """INSERT INTO execution_ledger_poll(poll_kind,attempted_at) VALUES('FILLS',?)
                   ON CONFLICT(poll_kind) DO UPDATE SET attempted_at=excluded.attempted_at""",
                (float(now),),
            )
            conn.execute(
                """UPDATE execution_orders SET checked_at=?,updated_at=CURRENT_TIMESTAMP
                     WHERE signal_entity_id=? AND kind=? AND remote_id=?""",
                (
                    float(now), str(row["signal_entity_id"]),
                    str(row["kind"]), str(row["remote_id"]),
                ),
            )
            conn.commit()
            result = dict(row)
            result["checked_at"] = float(now)
            return "READY", result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def poll_due(self, poll_kind: str, now: float) -> bool:
        conn = self._conn_factory()
        try:
            row = conn.execute(
                "SELECT attempted_at FROM execution_ledger_poll WHERE poll_kind=?",
                (str(poll_kind).upper(),),
            ).fetchone()
            return row is None or float(now) - float(row[0]) >= 60
        finally:
            conn.close()

    def claim_poll(self, poll_kind: str, now: float) -> bool:
        kind = str(poll_kind).upper()
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT attempted_at FROM execution_ledger_poll WHERE poll_kind=?", (kind,),
            ).fetchone()
            if row is not None and float(now) - float(row[0]) < 60:
                conn.commit()
                return False
            conn.execute(
                """INSERT INTO execution_ledger_poll(poll_kind,attempted_at) VALUES(?,?)
                   ON CONFLICT(poll_kind) DO UPDATE SET attempted_at=excluded.attempted_at""",
                (kind, float(now)),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def funding_candidates(self, limit: int = 50) -> list[dict[str, Any]]:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT e.* FROM executions e
                     WHERE e.mode='live'
                       AND NOT EXISTS(
                           SELECT 1 FROM execution_funding_coverage c
                            WHERE c.signal_entity_id=e.signal_entity_id
                       )
                     ORDER BY e.signal_id LIMIT ?""",
                (max(1, min(int(limit), 500)),),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def owned_order_sources(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Return only State-owned executions/actions that contain exchange IDs."""
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            executions = [dict(row) for row in conn.execute(
                """SELECT * FROM executions
                     WHERE mode='live' AND entry_order_id IS NOT NULL"""
            ).fetchall()]
            actions = [dict(row) for row in conn.execute(
                """SELECT * FROM execution_actions
                     WHERE exchange_order_id IS NOT NULL"""
            ).fetchall()]
            return executions, actions
        finally:
            conn.close()

    def fills(self, signal_id: int) -> list[dict[str, Any]]:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            rows = conn.execute(
                """SELECT * FROM execution_fills
                    WHERE signal_entity_id=? ORDER BY time_ms,trade_id""",
                (signal_entity_id,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def accounting_evidence(self, signal_id: int) -> dict[str, Any]:
        """Load only immutable evidence used by confirmed final accounting."""
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            fills = [dict(row) for row in conn.execute(
                """SELECT * FROM execution_fills
                    WHERE signal_entity_id=? ORDER BY time_ms,trade_id""",
                (signal_entity_id,),
            ).fetchall()]
            coverage = conn.execute(
                "SELECT status FROM execution_funding_coverage WHERE signal_entity_id=?",
                (signal_entity_id,),
            ).fetchone()
            funding = conn.execute(
                """SELECT income FROM execution_funding
                    WHERE signal_entity_id=? ORDER BY time_ms,tran_id""",
                (signal_entity_id,),
            ).fetchall()
            return {
                "fills": fills,
                "funding_status": str(coverage[0]) if coverage else None,
                "funding_income": [str(row[0]) for row in funding],
            }
        finally:
            conn.close()

    def record_funding_coverage(
        self,
        signal_id: int,
        symbol: str,
        start_ms: int,
        end_ms: int,
        rows: Sequence[Mapping[str, Any]],
        *,
        checked_at: float,
    ) -> int:
        if int(start_ms) <= 0 or int(end_ms) < int(start_ms):
            raise ExecutionLedgerStateError("ledger_funding_window_invalid")
        normalized_symbol = str(symbol).upper()
        validated = []
        for row in rows:
            if str(row.get("incomeType") or "").upper() != "FUNDING_FEE":
                raise ExecutionLedgerStateError("ledger_funding_type_invalid")
            if str(row.get("symbol") or "").upper() != normalized_symbol:
                raise ExecutionLedgerStateError("ledger_funding_symbol_mismatch")
            if str(row.get("asset") or "").upper() != "USDT":
                raise ExecutionLedgerStateError("ledger_funding_asset_unresolved")
            event_ms = int(row.get("time") or 0)
            if event_ms < int(start_ms) or event_ms > int(end_ms):
                raise ExecutionLedgerStateError("ledger_funding_outside_window")
            tran_id = str(row.get("tranId") or "")
            if not tran_id:
                raise ExecutionLedgerStateError("ledger_funding_transaction_missing")
            validated.append((
                int(signal_id), tran_id, normalized_symbol, _number(row.get("income")),
                "USDT", event_ms,
                json.dumps(dict(row), sort_keys=True, separators=(",", ":"), default=str),
            ))
        conn = self._conn_factory()
        inserted = 0
        try:
            conn.execute("BEGIN IMMEDIATE")
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            existing_coverage = conn.execute(
                """SELECT start_ms,end_ms,status FROM execution_funding_coverage
                    WHERE signal_entity_id=?""",
                (signal_entity_id,),
            ).fetchone()
            expected_coverage = (int(start_ms), int(end_ms), "COMPLETE")
            if existing_coverage is not None and tuple(existing_coverage) != expected_coverage:
                raise ExecutionLedgerStateError("ledger_funding_coverage_conflict")
            for values in validated:
                typed_values = (
                    values[0], signal_entity_id, *values[1:]
                )
                owner = conn.execute(
                    """SELECT signal_id,signal_entity_id,tran_id,symbol,income,asset,
                              time_ms,payload_json
                         FROM execution_funding WHERE tran_id=?""",
                    (values[1],),
                ).fetchone()
                if owner is not None and tuple(owner) != typed_values:
                    raise ExecutionLedgerStateError("ledger_funding_identity_conflict")
                cursor = conn.execute(
                    """INSERT OR IGNORE INTO execution_funding(
                           signal_id,signal_entity_id,tran_id,symbol,income,asset,
                           time_ms,payload_json
                       ) VALUES(?,?,?,?,?,?,?,?)""",
                    typed_values,
                )
                inserted += int(cursor.rowcount == 1)
            conn.execute(
                """INSERT OR IGNORE INTO execution_funding_coverage(
                       signal_id,signal_entity_id,start_ms,end_ms,checked_at,status
                   ) VALUES(?,?,?,?,?,?)""",
                (
                    int(signal_id), signal_entity_id, int(start_ms), int(end_ms),
                    float(checked_at), "COMPLETE",
                ),
            )
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def mark_orders_complete(self, signal_id: int) -> int:
        conn = self._conn_factory()
        try:
            signal_entity_id = self._signal_entity(conn, int(signal_id))
            cursor = conn.execute(
                """UPDATE execution_orders SET complete=1,updated_at=CURRENT_TIMESTAMP
                     WHERE signal_entity_id=? AND complete=0""",
                (signal_entity_id,),
            )
            conn.commit()
            return int(cursor.rowcount)
        finally:
            conn.close()


__all__ = ["ExecutionLedgerRepository", "ExecutionLedgerStateError"]
