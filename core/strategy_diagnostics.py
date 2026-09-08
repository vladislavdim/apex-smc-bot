"""Read-only funnel and release diagnostics for strategy quality work."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping


FUNNEL_STAGES = ("checks", "CORE", "TRIGGER", "RR", "PENDING_LTF", "candidate", "Groq", "delivered", "activated", "closed")


def funnel_snapshot(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Calculate rates with reached denominators, not mixed raw counts.

    Each row can contain ``stage`` and ``outcome`` (or a mapping of stage
    booleans).  Unknown/missing stages are counted separately so telemetry
    gaps are not mistaken for strategy failures.
    """
    reached = defaultdict(int)
    passed = defaultdict(int)
    unknown = defaultdict(int)
    seen = set()
    for index, row in enumerate(rows):
        identity = row.get("event_key") or row.get("attempt_id") or index
        if identity in seen:
            continue
        seen.add(identity)
        stages = row.get("stages") if isinstance(row.get("stages"), Mapping) else None
        if stages is not None:
            for stage in FUNNEL_STAGES:
                value = stages.get(stage)
                if value is None:
                    unknown[stage] += 1
                else:
                    reached[stage] += 1
                    if value is True or str(value).upper() in {"PASS", "CANDIDATE", "DELIVERED", "ACTIVATED", "CLOSED", "OK"}:
                        passed[stage] += 1
            continue
        stage = str(row.get("stage") or "").strip()
        if stage not in FUNNEL_STAGES:
            unknown["unknown"] += 1
            continue
        reached[stage] += 1
        if str(row.get("outcome") or "").upper() in {"PASS", "CANDIDATE", "DELIVERED", "ACTIVATED", "CLOSED", "OK"} or row.get("passed") is True:
            passed[stage] += 1
    steps = []
    for stage in FUNNEL_STAGES:
        denominator = reached[stage]
        steps.append({
            "stage": stage, "reached": denominator, "passed": passed[stage],
            "pass_rate": round(passed[stage] / denominator, 6) if denominator else None,
            "missing": unknown[stage],
        })
    bottleneck = min((item for item in steps if item["reached"]), key=lambda item: (item["pass_rate"], -item["reached"]), default=None)
    return {"attempts": len(seen), "steps": steps, "unknown": dict(unknown), "bottleneck": bottleneck, "scope": "READ_ONLY_DIAGNOSTICS"}


def compare_release_funnels(releases: Mapping[str, Iterable[Mapping[str, Any]]]) -> dict[str, Any]:
    """Compare comparable cohorts without mixing their denominators."""
    return {str(release): funnel_snapshot(rows) for release, rows in releases.items()}


__all__ = ["FUNNEL_STAGES", "compare_release_funnels", "funnel_snapshot"]
