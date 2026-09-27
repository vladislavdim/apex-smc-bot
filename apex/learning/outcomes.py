"""Real-outcome admission for Live Memory."""
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome,*,confirmed_position:bool=True)->TradeOutcome:
    if not confirmed_position or not outcome.position_id:
        raise ValueError("REAL_POSITION_OUTCOME_REQUIRED")
    return outcome
__all__=["require_real_outcome"]
