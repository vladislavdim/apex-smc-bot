"""Risk admission may keep, reduce or block; it never increases base risk."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from apex.domain.models import Candidate, RiskDecision


@dataclass(frozen=True)
class RiskLimits:
    max_risk_pct: float
    max_leverage: float
    max_portfolio_risk_pct: float
    max_same_side_risk_pct: float
    max_cluster_risk_pct: float


@dataclass(frozen=True)
class RiskState:
    ready: bool
    daily_loss_locked: bool
    equity_quote: float
    portfolio_risk_pct: float
    same_side_risk_pct: float
    cluster_risk_pct: float


def decide_risk(
    candidate: Candidate,
    *,
    base_risk_pct: float,
    leverage: float,
    state: RiskState,
    limits: RiskLimits,
    dependency_multiplier: float = 1.0,
) -> RiskDecision:
    reasons: list[str] = []
    if not state.ready:
        reasons.append("RUNTIME_NOT_READY")
    if state.daily_loss_locked:
        reasons.append("DAILY_LOSS_LOCK")
    if leverage <= 0 or leverage > limits.max_leverage:
        reasons.append("LEVERAGE_LIMIT")
    if base_risk_pct <= 0 or base_risk_pct > limits.max_risk_pct:
        reasons.append("BASE_RISK_LIMIT")
    stop_distance = abs(candidate.entry - candidate.initial_sl)
    if not isfinite(stop_distance) or stop_distance <= 0 or state.equity_quote <= 0:
        reasons.append("INVALID_RISK_GEOMETRY")
    if state.portfolio_risk_pct >= limits.max_portfolio_risk_pct:
        reasons.append("PORTFOLIO_RISK_LIMIT")
    if state.same_side_risk_pct >= limits.max_same_side_risk_pct:
        reasons.append("SAME_SIDE_RISK_LIMIT")
    if state.cluster_risk_pct >= limits.max_cluster_risk_pct:
        reasons.append("CLUSTER_RISK_LIMIT")
    if reasons:
        return RiskDecision("BLOCK", base_risk_pct, 0.0, 0.0, tuple(reasons))

    multiplier = min(1.0, max(0.0, float(dependency_multiplier)))
    final = min(base_risk_pct, limits.max_risk_pct) * multiplier
    if final <= 0:
        return RiskDecision("BLOCK", base_risk_pct, 0.0, 0.0, ("DEPENDENCY_BLOCK",))
    risk_quote = state.equity_quote * final / 100
    quantity = risk_quote / stop_distance
    decision = "KEEP" if abs(final - base_risk_pct) < 1e-12 else "REDUCE"
    reason_codes = () if decision == "KEEP" else ("DEPENDENCY_REDUCTION",)
    return RiskDecision(decision, base_risk_pct, final, quantity, reason_codes)


__all__ = ["RiskLimits", "RiskState", "decide_risk"]
