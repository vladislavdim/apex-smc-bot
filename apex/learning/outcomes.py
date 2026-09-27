"""Validation boundary for real, correlated trade outcomes."""
from __future__ import annotations
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome: TradeOutcome, *, candidate_id: str | None = None, execution_id: str | None = None, confirmed_position: bool) -> TradeOutcome:
    if not confirmed_position:
        raise ValueError("unconfirmed_position_forbidden")
    if not outcome.position_id:
        raise ValueError("trade_correlation_incomplete")
    if (candidate_id is None) != (execution_id is None):
        raise ValueError("trade_correlation_incomplete")
    return outcome

__all__=["require_real_outcome"]
