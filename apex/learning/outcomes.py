"""Real-outcome learning boundary."""
from __future__ import annotations
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome)->TradeOutcome:
    if not outcome.position_id: raise ValueError("confirmed_position_required")
    return outcome
__all__=["require_real_outcome"]
