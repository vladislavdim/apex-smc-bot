"""Durable, bounded audit trail for strategy-to-delivery decisions."""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any
from core.setup_audit import emit_decision_event as _emit_setup_audit_decision

DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
)


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
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


def record_strategy_decision(
    candidate: dict[str, Any],
    outcome: str,
    stage: str,
    reason: str = "",
    *,
    evidence: dict[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> None:
    """Record evidence without ever blocking the scanner or Telegram delivery."""
    try:
        _emit_setup_audit_decision(candidate, outcome, stage, reason, evidence)
    except Exception:
        pass
    structure = candidate.get("structure") if isinstance(candidate.get("structure"), dict) else {}
    review = candidate.get("_external_quality_review")
    review = review if isinstance(review, dict) else {}
    payload = evidence if isinstance(evidence, dict) else {}
    try:
        conn = _connect(db_path)
        conn.execute(
            """INSERT INTO strategy_decisions
               (symbol,strategy,timeframe,direction,structure_direction,structure_event,
                outcome,stage,reason,groq_decision,groq_confidence,evidence_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                str(candidate.get("symbol", "UNKNOWN")),
                str(candidate.get("grade") or candidate.get("strategy") or "UNKNOWN"),
                str(candidate.get("timeframe", "")), str(candidate.get("direction", "")),
                str(structure.get("direction") or candidate.get("structure_direction") or ""),
                str(structure.get("event") or candidate.get("structure_event") or ""),
                str(outcome).upper(), str(stage), str(reason)[:1000],
                str(review.get("decision", "")),
                float(review.get("confidence", 0) or 0),
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)[:8000],
            ),
        )
        # Bounded retention: detailed decisions are operational telemetry, not
        # permanent training truth. Closed outcomes live in their own tables.
        conn.execute("DELETE FROM strategy_decisions WHERE created_at < datetime('now', '-90 days')")
        conn.commit()
        conn.close()
    except Exception:
        return

