"""Evidence gate for book-derived and other shadow rules.

The evaluator is deliberately stricter than a simple average.  It reports the
full A/B split and only proposes review after at least 30 eligible closed
trades, positive mean/median delta-R, no worse drawdown or tail loss, no
single-outlier dependence and no safety violation.  It cannot activate a rule.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import math
from statistics import median
from typing import Any, Iterable, Mapping


MIN_ELIGIBLE = 30
DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.environ.get(
        "APEX_BRAIN_DB_PATH",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
    ),
)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result and abs(result) != float("inf") else default


def _drawdown(values: list[float]) -> float:
    peak = 0.0
    cumulative = 0.0
    worst = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        worst = min(worst, cumulative - peak)
    return abs(worst)


def _profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if not losses:
        return None
    return gains / losses


def _tail_loss(values: list[float]) -> float:
    if not values:
        return 0.0
    count = max(1, int(len(values) * 0.1))
    return sum(sorted(values)[:count]) / count


def evaluate_shadow_rule(rows: Iterable[Mapping[str, Any]], *, min_eligible: int = MIN_ELIGIBLE) -> dict[str, Any]:
    """Evaluate old/new outcomes without mutating strategy or production state."""
    data = []
    seen = set()
    rejected = 0
    for row in rows:
        identity = row.get("signal_id")
        try:
            old, new = float(row["old_r"]), float(row["new_r"])
            valid = math.isfinite(old) and math.isfinite(new)
        except (KeyError, TypeError, ValueError):
            valid = False
        key = (row.get("strategy"), row.get("rule_id"), identity)
        if (not valid or identity is None or key in seen or
                row.get("status") != "CLOSED" or not row.get("closed_at") or
                not row.get("strategy") or not row.get("rule_id")):
            rejected += 1
            continue
        seen.add(key)
        data.append({**row, "old_r": old, "new_r": new})
    data.sort(key=lambda row: str(row["closed_at"]))
    mixed_cohorts = len({(row["strategy"], row["rule_id"]) for row in data}) > 1
    min_eligible = max(MIN_ELIGIBLE, int(min_eligible))
    old_pass = [row for row in data if bool(row.get("old_pass"))]
    new_pass = [row for row in data if bool(row.get("new_pass"))]
    both_pass = [row for row in data if bool(row.get("old_pass")) and bool(row.get("new_pass"))]
    new_only = [row for row in data if bool(row.get("new_pass")) and not bool(row.get("old_pass"))]
    old_only = [row for row in data if bool(row.get("old_pass")) and not bool(row.get("new_pass"))]
    deltas = [row["new_r"] - row["old_r"] for row in data]
    old_results = [_num(row.get("old_r")) for row in data]
    new_results = [_num(row.get("new_r")) for row in data]
    positive_deltas = [value for value in deltas if value > 0]
    outlier_dependent = bool(positive_deltas and max(positive_deltas) > sum(positive_deltas) * 0.5)
    safety_violations = sum(1 for row in data if bool(row.get("safety_violation")))
    mean_delta = sum(deltas) / len(deltas) if deltas else 0.0
    median_delta = median(deltas) if deltas else 0.0
    old_dd, new_dd = _drawdown(old_results), _drawdown(new_results)
    old_tail, new_tail = _tail_loss(old_results), _tail_loss(new_results)
    eligible = len(data) >= max(1, int(min_eligible))
    no_deterioration = new_dd <= old_dd and new_tail >= old_tail
    proposal = bool(eligible and not mixed_cohorts and mean_delta > 0 and median_delta > 0 and no_deterioration and not outlier_dependent and not safety_violations)
    return {
        "rejected_rows": rejected, "mixed_cohorts": mixed_cohorts,
        "eligible": len(data), "minimum_required": int(min_eligible),
        "old_pass": len(old_pass), "new_pass": len(new_pass), "both_pass": len(both_pass),
        "new_only": len(new_only), "old_only": len(old_only),
        "mean_delta_r": round(mean_delta, 8), "median_delta_r": round(float(median_delta), 8),
        "old_win_rate": round(sum(_num(row.get("old_r")) > 0 for row in data) / len(data), 8) if data else None,
        "new_win_rate": round(sum(_num(row.get("new_r")) > 0 for row in data) / len(data), 8) if data else None,
        "old_profit_factor": _profit_factor(old_results), "new_profit_factor": _profit_factor(new_results),
        "old_drawdown_r": round(old_dd, 8), "new_drawdown_r": round(new_dd, 8),
        "old_tail_loss_r": round(old_tail, 8), "new_tail_loss_r": round(new_tail, 8),
        "improved": sum(value > 0 for value in deltas), "worsened": sum(value < 0 for value in deltas),
        "outlier_dependent": outlier_dependent, "safety_violations": safety_violations,
        "no_drawdown_or_tail_deterioration": no_deterioration,
        "promotion_proposed": proposal, "auto_activated": False, "scope": "REVIEW_ONLY",
    }


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_shadow_evidence_schema(db_path: str = DB_PATH) -> None:
    conn = _connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS apex_v2_shadow_evaluations (
            strategy TEXT NOT NULL, rule_id TEXT NOT NULL, evidence_hash TEXT NOT NULL,
            summary_json TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(strategy,rule_id)
        )"""
    )
    conn.commit(); conn.close()


def persist_shadow_evaluation(strategy: str, rule_id: str, summary: Mapping[str, Any], db_path: str = DB_PATH) -> str:
    ensure_shadow_evidence_schema(db_path)
    encoded = json.dumps(dict(summary), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    conn = _connect(db_path)
    conn.execute(
        """INSERT INTO apex_v2_shadow_evaluations(strategy,rule_id,evidence_hash,summary_json)
           VALUES(?,?,?,?) ON CONFLICT(strategy,rule_id) DO UPDATE SET
             evidence_hash=excluded.evidence_hash,summary_json=excluded.summary_json,updated_at=CURRENT_TIMESTAMP""",
        (str(strategy).upper(), str(rule_id), digest, encoded),
    )
    conn.commit(); conn.close()
    return digest


def load_shadow_evaluations(db_path: str = DB_PATH, limit: int = 100) -> list[dict[str, Any]]:
    ensure_shadow_evidence_schema(db_path)
    conn = _connect(db_path)
    rows = conn.execute("SELECT strategy,rule_id,evidence_hash,summary_json,updated_at FROM apex_v2_shadow_evaluations ORDER BY updated_at DESC LIMIT ?", (max(1, int(limit)),)).fetchall()
    conn.close()
    output = []
    for row in rows:
        try:
            summary = json.loads(row[3] or "{}")
        except (TypeError, json.JSONDecodeError):
            summary = {}
        output.append({"strategy": row[0], "rule_id": row[1], "evidence_hash": row[2], "updated_at": row[4], **summary})
    return output


__all__ = ["MIN_ELIGIBLE", "ensure_shadow_evidence_schema", "evaluate_shadow_rule", "load_shadow_evaluations", "persist_shadow_evaluation"]
