"""Typed State DB repository for strategy-to-delivery decisions."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable, Mapping


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


__all__ = ["StrategyDecisionRepository"]
