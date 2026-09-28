"""Validation boundary for learning from confirmed, correlated live outcomes."""

from apex.domain.models import TradeOutcome


def require_real_outcome(outcome: TradeOutcome, *, confirmed_position: bool) -> TradeOutcome:
    if not confirmed_position:
        raise ValueError("unconfirmed_position_forbidden")
    if not outcome.position_id or not outcome.outcome_id:
        raise ValueError("real_outcome_correlation_required")
    return outcome


__all__ = ["require_real_outcome"]
