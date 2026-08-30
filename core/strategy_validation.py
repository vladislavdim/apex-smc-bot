"""Out-of-sample-style reporting over objectively closed APEX signals.

This module never tunes or changes a live strategy. It reports what actually
happened and labels small samples honestly.
"""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from typing import Any


def _r_multiple(row: sqlite3.Row) -> float | None:
    result = str(row["result"] or "").lower()
    if result == "sl":
        return -1.0
    target_name = result if result in {"tp1", "tp2", "tp3"} else None
    if not target_name:
        return None
    entry, stop, target = float(row["entry"] or 0), float(row["sl"] or 0), float(row[target_name] or 0)
    risk = abs(entry - stop)
    return abs(target - entry) / risk if risk > 0 and target > 0 else None


def _wilson(wins: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total <= 0:
        return 0.0, 0.0
    p = wins / total
    denominator = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def validation_report(db_path: str, min_samples: int = 30) -> dict[str, Any]:
    """Return per-strategy/direction results; never claim quality on tiny N."""
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """SELECT id,symbol,direction,COALESCE(NULLIF(grade,''),signal_type,'UNKNOWN') strategy,
                      entry,sl,tp1,tp2,tp3,result,created_at,closed_at
               FROM signals
               WHERE lower(result) IN ('tp1','tp2','tp3','sl')
               ORDER BY COALESCE(closed_at,created_at),id"""
        ).fetchall()
    except sqlite3.Error:
        rows = []
    finally:
        conn.close()
    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    chronological: list[dict[str, Any]] = []
    for row in rows:
        r_value = _r_multiple(row)
        if r_value is None:
            continue
        strategy, direction = str(row["strategy"]).upper(), str(row["direction"]).upper()
        groups[(strategy, direction)].append(r_value)
        chronological.append({"strategy": strategy, "direction": direction, "r": r_value})
    summaries = []
    for (strategy, direction), values in sorted(groups.items()):
        wins, count = sum(value > 0 for value in values), len(values)
        low, high = _wilson(wins, count)
        summaries.append({
            "strategy": strategy, "direction": direction, "samples": count,
            "wins": wins, "losses": count - wins, "win_rate": wins / count,
            "expectancy_r": sum(values) / count, "win_rate_ci95": [low, high],
            "status": "MEASURED" if count >= min_samples else "INSUFFICIENT_SAMPLE",
        })
    return {"closed_samples": len(chronological), "min_samples": min_samples, "groups": summaries,
            "walk_forward": walk_forward_report(chronological, min_train=max(10, min_samples), test_size=10)}


def walk_forward_report(
    chronological: list[dict[str, Any]], min_train: int = 30, test_size: int = 10,
) -> dict[str, Any]:
    """Evaluate consecutive future chunks after an expanding historical window."""
    if len(chronological) < min_train + test_size:
        return {"status": "INSUFFICIENT_SAMPLE", "folds": [], "oos_samples": 0}
    folds, oos = [], []
    cursor = min_train
    while cursor < len(chronological):
        test = chronological[cursor:cursor + test_size]
        if not test:
            break
        values = [float(row["r"]) for row in test]
        oos.extend(values)
        folds.append({"train_samples": cursor, "test_samples": len(values),
                      "win_rate": sum(value > 0 for value in values) / len(values),
                      "expectancy_r": sum(values) / len(values)})
        cursor += test_size
    return {"status": "MEASURED", "folds": folds, "oos_samples": len(oos),
            "oos_win_rate": sum(value > 0 for value in oos) / len(oos),
            "oos_expectancy_r": sum(oos) / len(oos)}

