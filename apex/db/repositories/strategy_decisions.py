"""Typed State DB repository for strategy-to-delivery decisions."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable, Mapping

from apex.config.settings import ApexConfig
from apex.db.connection import connect_compatibility as _connect_compatibility_db
from apex.telemetry.event_log import emit_decision_event as _emit_setup_audit_decision


DB_PATH = ApexConfig.from_env().database.compatibility_db_path
_STATE_FACTORY = None


def configure_strategy_decision_state(connection_factory=None) -> None:
    global _STATE_FACTORY
    _STATE_FACTORY = connection_factory


def _connect(db_path: str) -> sqlite3.Connection:
    conn = _connect_compatibility_db(db_path, timeout=20, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS strategy_decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        strategy TEXT NOT NULL,
        timeframe TEXT,
        direction TEXT,
        structure_direction TEXT,
        structure_event TEXT,
        outcome TEXT NOT NULL,
        stage TEXT NOT NULL,
        reason TEXT,
        groq_decision TEXT,
        groq_confidence REAL,
        evidence_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_decisions_created ON strategy_decisions(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_decisions_lookup ON strategy_decisions(strategy,direction,outcome)")
    return conn


class StrategyDecisionRepository:
    def __init__(self, connection_factory: Callable[[], sqlite3.Connection]) -> None:
        self.connection_factory = connection_factory

    def record(self, values: Mapping[str, Any]) -> None:
        conn = self.connection_factory()
        try:
            conn.execute(
                """INSERT INTO strategy_decisions
                   (symbol,strategy,timeframe,direction,structure_direction,structure_event,
                    outcome,stage,reason,groq_decision,groq_confidence,evidence_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    str(values.get("symbol") or "UNKNOWN"),
                    str(values.get("strategy") or "UNKNOWN"),
                    str(values.get("timeframe") or ""),
                    str(values.get("direction") or ""),
                    str(values.get("structure_direction") or ""),
                    str(values.get("structure_event") or ""),
                    str(values.get("outcome") or "UNKNOWN").upper(),
                    str(values.get("stage") or ""),
                    str(values.get("reason") or "")[:1000],
                    str(values.get("groq_decision") or ""),
                    float(values.get("groq_confidence") or 0),
                    json.dumps(
                        values.get("evidence") if isinstance(values.get("evidence"), Mapping) else {},
                        ensure_ascii=False, separators=(",", ":"), default=str,
                    )[:8000],
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def recent_waits(self, limit: int = 20) -> list[dict[str, Any]]:
        conn = self.connection_factory()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT symbol,direction,timeframe,strategy,evidence_json,created_at
                   FROM strategy_decisions
                   WHERE outcome='WAIT' AND stage='groq_quality_gate'
                     AND created_at >= datetime('now','-12 hours')
                   ORDER BY created_at DESC LIMIT ?""",
                (max(1, min(int(limit), 100)),),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def rejection_report(self, hours: int = 24, limit: int = 30) -> dict[str, Any]:
        period = f"-{max(1, int(hours))} hours"
        conn = self.connection_factory()
        try:
            conn.row_factory = sqlite3.Row
            recent = conn.execute(
                """SELECT symbol,strategy,timeframe,direction,reason,groq_confidence,created_at
                   FROM strategy_decisions
                   WHERE outcome='REJECT' AND stage='groq_quality_gate'
                     AND created_at >= datetime('now', ?)
                   ORDER BY created_at DESC LIMIT ?""",
                (period, max(1, min(int(limit), 100))),
            ).fetchall()
            total = int(conn.execute(
                """SELECT COUNT(*) FROM strategy_decisions
                   WHERE outcome='REJECT' AND stage='groq_quality_gate'
                     AND created_at >= datetime('now', ?)""",
                (period,),
            ).fetchone()[0] or 0)
            grouped = conn.execute(
                """SELECT UPPER(COALESCE(NULLIF(strategy,''),'UNKNOWN')) strategy,
                          COALESCE(NULLIF(reason,''),'без причины') reason,COUNT(*) count
                   FROM strategy_decisions
                   WHERE outcome='REJECT' AND stage='groq_quality_gate'
                     AND created_at >= datetime('now', ?)
                   GROUP BY UPPER(COALESCE(NULLIF(strategy,''),'UNKNOWN')),
                            COALESCE(NULLIF(reason,''),'без причины')
                   ORDER BY strategy,count DESC""",
                (period,),
            ).fetchall()
            return {
                "hours": int(hours), "total": total,
                "recent": [dict(row) for row in recent],
                "by_strategy": [dict(row) for row in grouped],
            }
        finally:
            conn.close()

    def groq_count(self, hours: int = 24) -> tuple[int, str | None]:
        conn = self.connection_factory()
        try:
            period = f"-{max(1, int(hours))} hours"
            row = conn.execute(
                """SELECT COUNT(*),MAX(created_at) FROM strategy_decisions
                   WHERE stage='groq_quality_gate' AND created_at >= datetime('now', ?)""",
                (period,),
            ).fetchone()
            return int(row[0] or 0), row[1]
        finally:
            conn.close()


def record_strategy_decision(
    candidate: dict[str, Any],
    outcome: str,
    stage: str,
    reason: str = "",
    *,
    evidence: dict[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> None:
    """Record evidence without ever blocking scanner or delivery."""
    try:
        _emit_setup_audit_decision(candidate, outcome, stage, reason, evidence)
    except Exception:
        pass
    structure = candidate.get("structure") if isinstance(candidate.get("structure"), dict) else {}
    review = candidate.get("_external_quality_review")
    review = review if isinstance(review, dict) else {}
    payload = evidence if isinstance(evidence, dict) else {}
    try:
        if _STATE_FACTORY is None:
            conn = _connect(db_path)
            conn.close()
        repository = StrategyDecisionRepository(_STATE_FACTORY or (lambda: _connect(db_path)))
        repository.record({
            "symbol": candidate.get("symbol", "UNKNOWN"),
            "strategy": candidate.get("grade") or candidate.get("strategy") or "UNKNOWN",
            "timeframe": candidate.get("timeframe", ""),
            "direction": candidate.get("direction", ""),
            "structure_direction": structure.get("direction") or candidate.get("structure_direction") or "",
            "structure_event": structure.get("event") or candidate.get("structure_event") or "",
            "outcome": outcome, "stage": stage, "reason": reason,
            "groq_decision": review.get("decision", ""),
            "groq_confidence": review.get("confidence", 0),
            "evidence": payload,
        })
    except Exception:
        return


__all__ = [
    "StrategyDecisionRepository", "configure_strategy_decision_state",
    "record_strategy_decision",
]
