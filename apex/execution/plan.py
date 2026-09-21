"""Immutable execution plan and deterministic Binance client order IDs."""

from __future__ import annotations

from apex.domain.ids import new_id
from apex.domain.models import Candidate, ExecutionPlan, RiskDecision


def build_execution_plan(candidate: Candidate, risk: RiskDecision) -> ExecutionPlan:
    if risk.decision not in {"KEEP", "REDUCE"} or risk.quantity <= 0:
        raise ValueError("approved_positive_risk_required")
    return ExecutionPlan(
        execution_id=new_id("execution"), candidate_id=candidate.candidate_id,
        symbol=candidate.symbol, direction=candidate.direction,
        entry=candidate.entry, sl=candidate.initial_sl,
        targets=tuple(value for value in (candidate.tp1, candidate.tp2, candidate.tp3) if value is not None),
        quantity=risk.quantity,
    )


def client_order_ids(execution_id: str) -> dict[str, str]:
    token = str(execution_id).replace("_", "-")[:24]
    return {
        "entry": f"APEX-{token}-ENTRY",
        "sl": f"APEX-{token}-SL",
        "tp1": f"APEX-{token}-TP1",
        "exit": f"APEX-{token}-EXIT",
    }


__all__ = ["build_execution_plan", "client_order_ids"]
