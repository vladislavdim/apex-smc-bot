"""Validation boundary for real, correlated trade outcomes."""
from __future__ import annotations
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome: TradeOutcome, *, candidate_id: str, execution_id: str, confirmed_position: bool) -> TradeOutcome:
    if not confirmed_position:
        raise ValueError("unconfirmed_position_forbidden")
    if not outcome.position_id or not candidate_id or not execution_id:
        raise ValueError("trade_correlation_incomplete")
    return outcome

__all__=["require_real_outcome"]
