"""Durable, bounded audit trail for strategy-to-delivery decisions."""

from __future__ import annotations

import sqlite3
from apex.db.connection import connect_compatibility as _connect_compatibility_db
from apex.db.repositories.strategy_decisions import StrategyDecisionRepository
from typing import Any
from apex.config.settings import ApexConfig
from core.setup_audit import emit_decision_event as _emit_setup_audit_decision

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
