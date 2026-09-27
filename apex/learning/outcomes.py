"""Live-only outcome admission boundary."""
from apex.domain.models import TradeOutcome
def require_real_outcome(outcome:TradeOutcome,*,confirmed_position:bool)->TradeOutcome:
    if not confirmed_position: raise ValueError("UNCONFIRMED_POSITION_OUTCOME_FORBIDDEN")
    if not outcome.position_id: raise ValueError("POSITION_ID_REQUIRED")
    return outcome
__all__=["require_real_outcome"]
