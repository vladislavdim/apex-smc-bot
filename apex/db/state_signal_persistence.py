"""State-first delivered signal creation with restart-safe compatibility replay."""

from __future__ import annotations

import sqlite3
from typing import Callable

from apex.domain.ids import derived_id


class StateSignalPersistence:
    def __init__(
        self,
        legacy_factory: Callable[[], sqlite3.Connection],
        state_factory: Callable[[], sqlite3.Connection],
        release_sha: str,
    ) -> None:
        self.legacy_factory = legacy_factory
        self.state_factory = state_factory
        self.release_sha = str(release_sha).strip().lower()
        if not self.release_sha:
            raise ValueError("release_sha_missing")

    def save(
        self, symbol: str, direction: str, signal_type: str, entry: float,
        tp1: float, tp2: float, tp3: float, sl: float, timeframe: str,
        estimated_hours: float, grade: str, *, confluence: float = 0,
        regime: str = "UNKNOWN",
    ) -> int | None:
        """Reserve a unique pair and ID; State commit precedes compatibility replay."""
        symbol = str(symbol or "").upper()
        if not symbol or not direction or not signal_type or any(
            value is None for value in (entry, tp1, tp2, tp3, sl)
        ):
            raise ValueError("signal_geometry_incomplete")
        legacy = self.legacy_factory()
        state = None
        try:
            legacy.execute("BEGIN IMMEDIATE")
            state = self.state_factory()
            state.execute("BEGIN IMMEDIATE")
            if legacy.execute(
                "SELECT 1 FROM signals WHERE UPPER(symbol)=? AND result='pending' LIMIT 1",
                (symbol,),
            ).fetchone() or state.execute(
                """SELECT 1 FROM signal_lifecycle WHERE UPPER(symbol)=?
                   AND status IN ('waiting_entry','active') AND result='pending' LIMIT 1""",
                (symbol,),
            ).fetchone():
                state.rollback()
                legacy.rollback()
                return None
            legacy_max = legacy.execute("SELECT COALESCE(MAX(id),0) FROM signals").fetchone()[0]
            state_max = max(state.execute(
                f"SELECT COALESCE(MAX(signal_id),0) FROM {table}"
            ).fetchone()[0] for table in (
                "signal_lifecycle", "executions", "manager_positions",
            ))
            sequence = legacy.execute(
                "SELECT 1 FROM sqlite_master WHERE name='sqlite_sequence'"
            ).fetchone()
            sequence_max = (legacy.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='signals'"
            ).fetchone() or (0,))[0] if sequence else 0
            signal_id = max(int(legacy_max), int(state_max), int(sequence_max)) + 1
            state.execute(
                """INSERT INTO signal_lifecycle(
                    signal_entity_id,signal_id,status,result,symbol,direction,
                    signal_type,timeframe,entry,sl,tp1,tp2,tp3,estimated_hours,
                    grade,confluence,regime,ownership,source
                ) VALUES(?,?,'waiting_entry','pending',?,?,?,?,?,?,?,?,?,?,?,?,?,'state','state')""",
                (derived_id("signal", self.release_sha, signal_id), signal_id, symbol,
                 direction, signal_type, timeframe, entry, sl, tp1, tp2, tp3,
                 estimated_hours, grade, confluence, regime),
            )
            state.commit()
            # The State row remains durable if compatibility commit fails.
            # A later sync_state_monitor_projection replays it before admission.
            self._project_one(legacy, signal_id)
            legacy.commit()
            return signal_id
        except Exception:
            if state is not None:
                state.rollback()
            legacy.rollback()
            raise
        finally:
            if state is not None:
                state.close()
            legacy.close()

    def _project_one(self, legacy: sqlite3.Connection, signal_id: int) -> None:
        # Reuse the same projection semantics after releasing this transaction.
        # The caller's legacy lock reserves the pair until the State commit.
        state = self.state_factory()
        try:
            state.row_factory = sqlite3.Row
            row = state.execute(
                "SELECT * FROM signal_lifecycle WHERE signal_id=? AND source='state'",
                (signal_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("state_signal_missing_after_commit")
            legacy.execute(
                """INSERT INTO signals(
                    id,symbol,direction,signal_type,entry,tp1,tp2,tp3,sl,timeframe,
                    estimated_hours,grade,result,created_at,closed_at,confluence,regime
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (signal_id, row["symbol"], row["direction"], row["signal_type"],
                 row["entry"], row["tp1"], row["tp2"], row["tp3"], row["sl"],
                 row["timeframe"], row["estimated_hours"], row["grade"],
                 row["result"], row["created_at"], row["closed_at"],
                 row["confluence"], row["regime"]),
            )
            legacy.execute(
                """INSERT INTO signal_execution_state(signal_id,status)
                   VALUES(?,'waiting_entry')""", (signal_id,),
            )
        finally:
            state.close()


__all__ = ["StateSignalPersistence"]
