"""Read-only release/canary gates for safe deploy decisions."""
from __future__ import annotations

from typing import Any, Mapping


def evaluate_release_gate(
    metrics: Mapping[str, Any], *, min_gate_coverage: float = 0.99,
    max_duplicate_rate: float = 0.0,
) -> dict[str, Any]:
    """Return a decision; callers must explicitly perform any deploy/rollback."""
    reasons: list[str] = []
    if not bool(metrics.get("tests_green")):
        reasons.append("TESTS_NOT_GREEN")
    if str(metrics.get("manager_version", "2")) != "2":
        reasons.append("MANAGER_V2_NOT_ACTIVE")
    if bool(metrics.get("v1_decider_active")):
        reasons.append("V1_DECIDER_ACTIVE")
    if not bool(metrics.get("schema_compatible", True)):
        reasons.append("SCHEMA_INCOMPATIBLE")
    if float(metrics.get("gate_coverage", 0.0) or 0.0) < float(min_gate_coverage):
        reasons.append("GATE_COVERAGE_LOW")
    if float(metrics.get("duplicate_rate", 0.0) or 0.0) > float(max_duplicate_rate):
        reasons.append("DUPLICATES_DETECTED")
    if bool(metrics.get("critical_incident")):
        reasons.append("CRITICAL_INCIDENT")
    decision = "PROCEED" if not reasons else "HOLD"
    return {
        "decision": decision, "reasons": reasons,
        "canary_only": bool(metrics.get("canary_only", True)),
        "rollback_required": "CRITICAL_INCIDENT" in reasons or "SCHEMA_INCOMPATIBLE" in reasons,
        "scope": "READ_ONLY_RELEASE_GUARD",
    }


def restart_invariants(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Check durable safety markers after a restart without mutating them."""
    reasons = []
    if snapshot.get("manager_version") not in (None, 2, "2"):
        reasons.append("manager_version")
    if snapshot.get("v1_active"):
        reasons.append("v1_active")
    if snapshot.get("live_armed") and not snapshot.get("explicit_confirmation"):
        reasons.append("live_without_confirmation")
    if snapshot.get("reconciliation_required") and snapshot.get("protective_orders_touched"):
        reasons.append("protection_touched_during_reconciliation")
    return {"ok": not reasons, "reasons": reasons, "scope": "READ_ONLY_RESTART_CHECK"}


__all__ = ["evaluate_release_gate", "restart_invariants"]
