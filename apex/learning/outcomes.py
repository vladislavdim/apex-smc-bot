"""Validation boundary for learning from real, correlated trade outcomes only."""
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome)->TradeOutcome:
    if not outcome.position_id or not outcome.outcome_id:
        raise ValueError("real_outcome_correlation_required")
    return outcome
__all__=["require_real_outcome"]
