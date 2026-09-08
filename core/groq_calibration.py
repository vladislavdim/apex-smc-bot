"""Groq decision calibration and outcome memory (diagnostic only).

This store measures whether confidence tracks later outcomes.  It never
authorizes an action and never changes Manager V2 risk validation.  A missing
outcome is retained as ``PENDING`` rather than treated as a win or loss.
"""
from __future__ import annotations

import json
import os
import sqlite3
from collections import defaultdict
from statistics import mean
from typing import Any, Iterable, Mapping


DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.environ.get(
        "APEX_BRAIN_DB_PATH",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
    ),
)


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_groq_calibration_schema(db_path: str = DB_PATH) -> None:
    conn = _connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS apex_v2_groq_calibration (
            action_id TEXT PRIMARY KEY, signal_id INTEGER, strategy TEXT NOT NULL DEFAULT '',
            symbol TEXT NOT NULL DEFAULT '', action TEXT NOT NULL, confidence REAL,
            model TEXT, prompt_version TEXT, context_hash TEXT, outcome_label REAL,
            reward_r REAL, outcome_reason TEXT, latency_ms REAL,
            shadow_only INTEGER NOT NULL DEFAULT 1, predicted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resolved_at TEXT, payload_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_apex_v2_groq_calibration_lookup
          ON apex_v2_groq_calibration(strategy,action,predicted_at DESC);
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(apex_v2_groq_calibration)")}
    if "prediction_target" not in columns:
        conn.execute("ALTER TABLE apex_v2_groq_calibration ADD COLUMN prediction_target TEXT NOT NULL DEFAULT 'UNSPECIFIED'")
    if "context_version" not in columns:
        conn.execute("ALTER TABLE apex_v2_groq_calibration ADD COLUMN context_version TEXT NOT NULL DEFAULT 'v1'")
    conn.commit()
    conn.close()


def record_prediction(
    action_id: str, action: str, confidence: float | None, *, signal_id: int | None = None,
    strategy: str = "", symbol: str = "", model: str | None = None,
    prompt_version: str | None = None, context_hash: str | None = None,
    prediction_target: str = "UNSPECIFIED", context_version: str = "v1",
    payload: Mapping[str, Any] | None = None, db_path: str = DB_PATH,
) -> bool:
    """Persist a prediction once; malformed confidence is stored as NULL."""
    ensure_groq_calibration_schema(db_path)
    try:
        score = float(confidence) if confidence is not None else None
    except (TypeError, ValueError):
        score = None
    if score is not None:
        score = min(1.0, max(0.0, score))
    conn = _connect(db_path)
    changed = conn.execute(
        """INSERT OR IGNORE INTO apex_v2_groq_calibration
           (action_id,signal_id,strategy,symbol,action,confidence,model,prompt_version,context_hash,
            prediction_target,context_version,payload_json)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (str(action_id), signal_id, str(strategy).upper(), str(symbol).upper(), str(action).upper(), score,
         model, prompt_version, context_hash, str(prediction_target or "UNSPECIFIED").upper(),
         str(context_version or "v1"), json.dumps(payload or {}, ensure_ascii=False, default=str)),
    ).rowcount
    conn.commit()
    conn.close()
    return bool(changed)


def record_outcome(
    action_id: str, *, outcome_label: bool | float | None = None, reward_r: float | None = None,
    reason: str | None = None, latency_ms: float | None = None, db_path: str = DB_PATH,
) -> bool:
    ensure_groq_calibration_schema(db_path)
    label = None
    if outcome_label is not None:
        try:
            label = min(1.0, max(0.0, float(outcome_label)))
        except (TypeError, ValueError):
            label = None
    conn = _connect(db_path)
    changed = conn.execute(
        """UPDATE apex_v2_groq_calibration SET outcome_label=?,reward_r=?,outcome_reason=?,latency_ms=?,
                  resolved_at=CURRENT_TIMESTAMP WHERE action_id=?""",
        (label, reward_r, reason, latency_ms, str(action_id)),
    ).rowcount
    conn.commit()
    conn.close()
    return bool(changed)


def resolve_signal(
    signal_id: int, *, reward_r: float | None = None, outcome_label: bool | float | None = None,
    reason: str | None = None, db_path: str = DB_PATH,
) -> int:
    """Resolve only predictions explicitly targeting the whole trade outcome.

    Manager actions target incremental action-vs-HOLD effects and must be
    resolved by their own post-decision counterfactual window. Assigning the
    final trade result to every HOLD/PROTECT/PARTIAL decision is invalid
    calibration, so unspecified/action-level rows remain pending.
    """
    ensure_groq_calibration_schema(db_path)
    label = None
    if outcome_label is not None:
        try:
            label = min(1.0, max(0.0, float(outcome_label)))
        except (TypeError, ValueError):
            label = None
    conn = _connect(db_path)
    changed = conn.execute(
        """UPDATE apex_v2_groq_calibration SET outcome_label=COALESCE(?, outcome_label),
               reward_r=COALESCE(?, reward_r), outcome_reason=COALESCE(?, outcome_reason),
               resolved_at=COALESCE(resolved_at,CURRENT_TIMESTAMP)
           WHERE signal_id=? AND outcome_label IS NULL
             AND prediction_target='TRADE_TERMINAL_OUTCOME'""",
        (label, reward_r, reason, int(signal_id)),
    ).rowcount
    conn.commit(); conn.close()
    return int(changed)


def _bucket(confidence: float | None) -> str:
    if confidence is None:
        return "unknown"
    low = min(0.8, max(0.0, int(confidence * 5) / 5))
    return f"{low:.1f}-{min(1.0, low + 0.2):.1f}"


def calibration_summary(db_path: str = DB_PATH, limit: int = 2000) -> dict[str, Any]:
    ensure_groq_calibration_schema(db_path)
    conn = _connect(db_path)
    rows = conn.execute(
        """SELECT action,confidence,outcome_label,reward_r,strategy,prediction_target FROM apex_v2_groq_calibration
           ORDER BY predicted_at DESC LIMIT ?""", (max(1, int(limit)),)
    ).fetchall()
    conn.close()
    buckets: dict[str, list[tuple[float, float]]] = defaultdict(list)
    by_action: dict[str, list[sqlite3.Row]] = defaultdict(list)
    rewards: list[float] = []
    for row in rows:
        action = str(row[0]).upper()
        by_action[action].append(row)
        confidence = row[1]
        label = row[2]
        if confidence is not None and label is not None:
            buckets[_bucket(float(confidence))].append((float(confidence), float(label)))
        if row[3] is not None:
            rewards.append(float(row[3]))
    bucket_rows = []
    for name, values in sorted(buckets.items()):
        bucket_rows.append({
            "bucket": name, "n": len(values),
            "mean_confidence": round(mean(x[0] for x in values), 6),
            "observed_rate": round(mean(x[1] for x in values), 6),
            "calibration_error": round(abs(mean(x[0] for x in values) - mean(x[1] for x in values)), 6),
            "brier": round(mean((x[0] - x[1]) ** 2 for x in values), 6),
        })
    actions = []
    for action, values in sorted(by_action.items()):
        resolved = [row for row in values if row[2] is not None]
        actions.append({
            "action": action, "calls": len(values), "resolved": len(resolved),
            "mean_confidence": round(mean([float(row[1]) for row in values if row[1] is not None]), 6) if any(row[1] is not None for row in values) else None,
            "mean_outcome": round(mean([float(row[2]) for row in resolved]), 6) if resolved else None,
            "mean_reward_r": round(mean([float(row[3]) for row in resolved if row[3] is not None]), 6) if any(row[3] is not None for row in resolved) else None,
        })
    resolved_pairs = [(float(row[1]), float(row[2])) for row in rows if row[1] is not None and row[2] is not None]
    return {
        "scope": "SHADOW_DIAGNOSTICS", "calls": len(rows), "resolved": len(resolved_pairs),
        "target_contract": "versioned_prediction_target; action decisions are not labelled with whole-trade outcome",
        "brier": round(mean((p - y) ** 2 for p, y in resolved_pairs), 6) if resolved_pairs else None,
        "mean_reward_r": round(mean(rewards), 6) if rewards else None,
        "calibration_error": round(mean([abs(p - y) for p, y in resolved_pairs]), 6) if resolved_pairs else None,
        "buckets": bucket_rows, "actions": actions,
    }


__all__ = ["calibration_summary", "ensure_groq_calibration_schema", "record_outcome", "record_prediction", "resolve_signal"]
