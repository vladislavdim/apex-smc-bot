"""Paired, chronological comparisons; never an automatic production switch."""
from __future__ import annotations

import math
import statistics
from datetime import datetime
from typing import Any, Mapping, Sequence

from .replay import metrics, walk_forward_windows


def timestamp(value: Any) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.timestamp() if parsed.tzinfo else None
        except ValueError:
            return None


def paired_report(rows: Sequence[Mapping[str, Any]], *, baseline="NO_MANAGER",
                  candidate="PLAYBOOK_ONLY", train_days=180, test_days=30) -> dict:
    """Match the same attempt, reject duplicate tracks and purge late outcomes.

    This evaluates fixed policies. It does not select parameters on held-out
    data, and cannot establish an entry-filter ablation or detector parity.
    """
    groups: dict[str, dict] = {}
    duplicates: set[str] = set()
    for row in rows:
        if row.get("track") not in {baseline, candidate}:
            continue
        key = str(row.get("attempt_id") or "")
        if not key:
            continue
        group = groups.setdefault(key, {})
        if row["track"] in group:
            duplicates.add(key)
        group[row["track"]] = row
    pairs = []
    excluded = {"duplicate": len(duplicates), "incomplete": 0}
    for key, group in groups.items():
        if key in duplicates:
            continue
        a, b = group.get(baseline, {}), group.get(candidate, {})
        start = timestamp(a.get("decision_time"))
        ends = [timestamp(x.get("exit_time")) for x in (a, b)]
        values = [x.get("net_r") for x in (a, b)]
        valid = (all(x.get("status") == "CLOSED" for x in (a, b))
                 and start is not None and timestamp(b.get("decision_time")) == start
                 and all(x is not None and x >= start for x in ends)
                 and all(isinstance(x, (int, float)) and math.isfinite(x) for x in values))
        if not valid:
            excluded["incomplete"] += 1
            continue
        pairs.append({"id": key, "decision": start, "available": max(ends),
                      "baseline": float(values[0]), "candidate": float(values[1]),
                      "delta": float(values[1]) - float(values[0])})
    pairs.sort(key=lambda x: (x["decision"], x["id"]))

    def describe(selected):
        # Equity follows outcome availability; each pair uses a common clock.
        selected = sorted(selected, key=lambda x: (x["available"], x["id"]))
        deltas = [x["delta"] for x in selected]
        return {"n": len(selected), "baseline": metrics([x["baseline"] for x in selected]),
                "candidate": metrics([x["candidate"] for x in selected]),
                "mean_delta_r": statistics.fmean(deltas) if deltas else None,
                "median_delta_r": statistics.median(deltas) if deltas else None,
                "improved": sum(x > 0 for x in deltas), "worsened": sum(x < 0 for x in deltas)}

    folds = []
    if pairs:
        windows = walk_forward_windows(int(pairs[0]["decision"]),
                                       int(max(x["available"] for x in pairs)), train_days, test_days)
        for window in windows:
            train = [x for x in pairs if window["train_start"] <= x["decision"] < window["train_end"]
                     and x["available"] < window["test_start"]]
            test = [x for x in pairs if window["test_start"] <= x["decision"] < window["test_end"]
                    and x["available"] < window["test_end"]]
            folds.append({**window, "train": describe(train), "test": describe(test),
                          "purged_late_train": sum(window["train_start"] <= x["decision"] < window["train_end"]
                                                   and x["available"] >= window["test_start"] for x in pairs)})
    return {"comparison_kind": "PAIRED_FIXED_POLICY", "baseline_track": baseline,
            "candidate_track": candidate, "overall": describe(pairs), "folds": folds,
            "excluded": excluded, "auto_activate": False, "promotion_proposed": False,
            "limitations": ["production_detector_parity_required", "not_entry_filter_ablation",
                            "dependent_trades_require_clustered_uncertainty"]}
