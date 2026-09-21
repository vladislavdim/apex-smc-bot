"""Restart-safe import and parity gate for the confirmed execution ledger."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from apex.db.repositories.execution_ledger import ExecutionLedgerStateError
from apex.domain.ids import is_id


_MARKER = "execution_ledger_legacy_import_v1"
_TABLES = {
    "confirmed_execution_orders": "execution_orders",
    "confirmed_execution_fills": "execution_fills",
    "confirmed_execution_funding": "execution_funding",
    "confirmed_execution_funding_coverage": "execution_funding_coverage",
}


def _exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,),
    ).fetchone() is not None


def _rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    if not _exists(conn, table):
        return []
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute(f"SELECT * FROM {table}").fetchall()]


def import_legacy_execution_ledger(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    legacy = legacy_factory()
    state = state_factory()
    try:
        state.row_factory = sqlite3.Row
        marker = state.execute(
            "SELECT value_json FROM runtime_state WHERE key=?", (_MARKER,),
        ).fetchone()
        if marker is not None and not refresh:
            return {"already_complete": True, "rows": 0}
        sources = {name: _rows(legacy, name) for name in _TABLES}
        state.execute("BEGIN IMMEDIATE")
        imported = 0
        for source, target in _TABLES.items():
            for row in sources[source]:
                signal_id = int(row["signal_id"])
                execution = state.execute(
                    "SELECT signal_entity_id FROM executions WHERE signal_id=?",
                    (signal_id,),
                ).fetchone()
                if execution is None or not is_id(execution[0], "signal"):
                    raise ExecutionLedgerStateError(
                        f"ledger_import_orphan:{source}:{signal_id}"
                    )
                signal_entity_id = str(execution[0])
                if source == "confirmed_execution_orders":
                    values = (row.get("signal_id"), signal_entity_id) + tuple(
                        row.get(key) for key in (
                        "symbol", "kind", "remote_id", "is_algo",
                        "expected_side", "standard_id", "complete", "checked_at", "error",
                    ))
                    existing = state.execute(
                        """SELECT signal_id,signal_entity_id,symbol,kind,remote_id,
                                  is_algo,expected_side
                             FROM execution_orders
                            WHERE signal_entity_id=? AND kind=? AND remote_id=?""",
                        (signal_entity_id, values[3], values[4]),
                    ).fetchone()
                    if existing is not None:
                        if tuple(existing) != values[:7]:
                            raise ExecutionLedgerStateError("ledger_import_order_conflict")
                    else:
                        state.execute(
                            """INSERT INTO execution_orders(
                               signal_id,signal_entity_id,symbol,kind,remote_id,is_algo,expected_side,
                               standard_id,complete,checked_at,error
                           ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", values,
                        )
                        imported += 1
                elif source == "confirmed_execution_fills":
                    payload = row.get("payload_json", row.get("payload", "{}"))
                    values = tuple(row.get(key) for key in (
                        "symbol", "trade_id", "signal_id",
                    )) + (signal_entity_id,) + tuple(row.get(key) for key in (
                        "order_id", "kind", "qty",
                        "price", "commission", "commission_asset", "time_ms",
                    )) + (payload,)
                    existing = state.execute(
                        "SELECT * FROM execution_fills WHERE symbol=? AND trade_id=?",
                        values[:2],
                    ).fetchone()
                    if existing is not None:
                        comparable = tuple(existing[key] for key in (
                            "symbol", "trade_id", "signal_id", "signal_entity_id",
                            "order_id", "kind", "qty",
                            "price", "commission", "commission_asset", "time_ms", "payload_json",
                        ))
                        if comparable != values:
                            raise ExecutionLedgerStateError("ledger_import_fill_conflict")
                    else:
                        state.execute(
                            """INSERT INTO execution_fills(
                                   symbol,trade_id,signal_id,signal_entity_id,order_id,kind,
                                   qty,price,commission,
                                   commission_asset,time_ms,payload_json
                               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", values,
                        )
                        imported += 1
                elif source == "confirmed_execution_funding":
                    payload = row.get("payload_json", row.get("payload", "{}"))
                    values = (row.get("signal_id"), signal_entity_id) + tuple(
                        row.get(key) for key in (
                            "tran_id", "symbol", "income", "asset", "time_ms",
                        )
                    ) + (payload,)
                    owner = state.execute(
                        "SELECT signal_id,signal_entity_id,tran_id,symbol,income,asset,time_ms,payload_json "
                        "FROM execution_funding WHERE tran_id=?", (str(row["tran_id"]),),
                    ).fetchone()
                    if owner is not None and tuple(owner) != values:
                        raise ExecutionLedgerStateError("ledger_import_funding_conflict")
                    cursor = state.execute(
                        """INSERT OR IGNORE INTO execution_funding(
                               signal_id,signal_entity_id,tran_id,symbol,income,asset,time_ms,payload_json
                           ) VALUES(?,?,?,?,?,?,?,?)""", values,
                    )
                    imported += int(cursor.rowcount == 1)
                else:
                    existing = state.execute(
                        "SELECT 1 FROM execution_funding_coverage WHERE signal_entity_id=?",
                        (signal_entity_id,),
                    ).fetchone()
                    if existing is None:
                        state.execute(
                            """INSERT INTO execution_funding_coverage(
                               signal_id,signal_entity_id,start_ms,end_ms,checked_at,status
                           ) VALUES(?,?,?,?,?,?)""",
                            (signal_id, signal_entity_id) + tuple(row.get(key) for key in (
                                "start_ms", "end_ms", "checked_at", "status",
                            )),
                        )
                        imported += 1
        poll_sources = (
            ("confirmed_execution_poll", "FILLS"),
            ("confirmed_execution_funding_poll", "FUNDING"),
        )
        for table, kind in poll_sources:
            rows = _rows(legacy, table)
            if rows:
                cursor = state.execute(
                    """INSERT OR IGNORE INTO execution_ledger_poll(poll_kind,attempted_at)
                       VALUES(?,?)""",
                    (kind, rows[0]["attempted_at"]),
                )
                imported += int(cursor.rowcount == 1)
        state.execute(
            """INSERT INTO runtime_state(key,value_json) VALUES(?,?)
               ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                 updated_at=CURRENT_TIMESTAMP""",
            (_MARKER, json.dumps({"complete": True, "rows": imported}, sort_keys=True)),
        )
        state.commit()
        return {"already_complete": False, "rows": imported}
    except Exception:
        state.rollback()
        raise
    finally:
        legacy.close()
        state.close()


def execution_ledger_parity_report(
    legacy_factory: Callable[[], sqlite3.Connection],
    state_factory: Callable[[], sqlite3.Connection],
) -> dict[str, Any]:
    legacy = legacy_factory()
    state = state_factory()
    mismatches: list[str] = []
    try:
        mappings = {
            "confirmed_execution_orders": (
                "execution_orders", ("signal_id", "symbol", "kind", "remote_id", "is_algo",
                "expected_side"),
            ),
            "confirmed_execution_fills": (
                "execution_fills", ("symbol", "trade_id", "signal_id", "order_id", "kind",
                "qty", "price", "commission", "commission_asset", "time_ms"),
            ),
            "confirmed_execution_funding": (
                "execution_funding", ("signal_id", "tran_id", "symbol", "income", "asset", "time_ms"),
            ),
            "confirmed_execution_funding_coverage": (
                "execution_funding_coverage", ("signal_id",),
            ),
        }
        for source, (target, fields) in mappings.items():
            source_rows = {tuple(row.get(field) for field in fields) for row in _rows(legacy, source)}
            target_rows = {tuple(row.get(field) for field in fields) for row in _rows(state, target)}
            for row in sorted(source_rows - target_rows, key=str):
                mismatches.append(f"missing:{source}:{row}")
        return {"ok": not mismatches, "mismatches": mismatches}
    finally:
        legacy.close()
        state.close()


__all__ = ["execution_ledger_parity_report", "import_legacy_execution_ledger"]
