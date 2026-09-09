"""Leakage-safe, descriptive research analytics and promotion proposals."""
from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict
from typing import Any, Iterable, Mapping

from .replay import chronological_splits, metrics
from .store import ResearchStore


def wilson_interval(wins: int, total: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None
    p = wins / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    margin = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return max(0.0, centre - margin), min(1.0, centre + margin)


def promotion_proposal(deltas: Iterable[float], *, baseline_drawdown: float,
                       candidate_drawdown: float, safety_violations: int = 0,
                       minimum: int = 30) -> dict[str, Any]:
    values = [float(x) for x in deltas if x is not None and math.isfinite(float(x))]
    mean = statistics.fmean(values) if values else None
    median = statistics.median(values) if values else None
    total = sum(values)
    largest_share = max((abs(x) for x in values), default=0.0) / max(abs(total), 1e-12)
    sorted_values = sorted(values)
    tail = statistics.fmean(sorted_values[:max(1, len(values)//10)]) if values else None
    reasons = []
    if len(values) < minimum: reasons.append(f"n<{minimum}")
    if mean is None or mean <= 0: reasons.append("mean_delta_not_positive")
    if median is None or median <= 0: reasons.append("median_delta_not_positive")
    if candidate_drawdown > baseline_drawdown: reasons.append("drawdown_deteriorated")
    if tail is not None and tail < -1.0: reasons.append("tail_loss_deteriorated")
    if largest_share > .5: reasons.append("single_outlier_dependence")
    if safety_violations: reasons.append("safety_violation")
    return {"eligible": len(values), "mean_delta_r": mean, "median_delta_r": median,
            "tail_mean_delta_r": tail, "largest_outlier_share": largest_share,
            "safety_violations": safety_violations,
            "promotion_proposed": not reasons, "auto_activate": False, "reasons": reasons}


def evaluate_profile(store: ResearchStore, run_id: str, profile_id: str) -> list[dict[str, Any]]:
    """Evaluate predefined causal segments; never searches arbitrary combinations."""
    rows = store.completed_trade_rows(run_id, profile_id)
    if not rows:
        return []
    timestamps = [int(row["decision_time"]) for row in rows]
    split = chronological_splits(timestamps)
    test_set = set(split["TEST"])
    baseline = metrics([row["net_r"] for row in rows if row.get("net_r") is not None])
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        try:
            snapshot = json.loads(row.get("snapshot_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = {}
        feature = ((snapshot.get("candidate") or {}).get("feature_snapshot") or {})
        segment_values = {
            "direction": row.get("side") or "UNKNOWN",
            "session": (feature.get("session") or {}).get("name") or "UNKNOWN",
            "regime": (feature.get("regime") or {}).get("primary") or "UNKNOWN",
            "volatility": (feature.get("regime") or {}).get("volatility") or "UNKNOWN",
        }
        for name, value in segment_values.items():
            groups[(name, str(value))].append(row)
    results = []
    baseline_expectancy = baseline.get("expectancy")
    for (feature, value), members in groups.items():
        values = [float(x["net_r"]) for x in members if x.get("net_r") is not None]
        result = metrics(values)
        test_values = [float(x["net_r"]) for x in members if x.get("net_r") is not None and int(x["decision_time"]) in test_set]
        test_baseline = [float(x["net_r"]) for x in rows if x.get("net_r") is not None and int(x["decision_time"]) in test_set]
        low, high = wilson_interval(sum(1 for x in values if x > 0), len(values))
        proposal = promotion_proposal(
            [x - float(baseline_expectancy or 0) for x in values],
            baseline_drawdown=float(baseline.get("max_drawdown_r") or 0),
            candidate_drawdown=float(result.get("max_drawdown_r") or 0),
        )
        evaluation = {"research_run_id": run_id, "profile_id": profile_id,
            "feature": feature, "segment": {feature: value}, "sample_size": len(values),
            "coverage": len(values)/len(rows), "win_rate": result.get("win_rate"),
            "expectancy": result.get("expectancy"), "profit_factor": result.get("profit_factor"),
            "max_drawdown": result.get("max_drawdown_r"),
            "uplift": (result.get("expectancy") - baseline_expectancy) if result.get("expectancy") is not None and baseline_expectancy is not None else None,
            "oos_uplift": (statistics.fmean(test_values)-statistics.fmean(test_baseline)) if test_values and test_baseline else None,
            "confidence_low": low, "confidence_high": high,
            "status": "PROMOTION_CANDIDATE" if proposal["promotion_proposed"] else "LIVE_SHADOW",
            "metrics": {**result, "split_sizes": {k: len(v) for k,v in split.items()},
                        "point_in_time": True, "promotion": proposal}}
        store.save_feature_evaluation(evaluation)
        results.append(evaluation)
    return results


__all__ = ["evaluate_profile", "promotion_proposal", "wilson_interval"]
