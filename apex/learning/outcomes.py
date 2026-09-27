"""Live-learning outcome guard: only correlated real positions may learn."""
from apex.domain.models import TradeOutcome
def require_real_outcome(outcome:TradeOutcome)->TradeOutcome:
    if not outcome.position_id: raise ValueError("REAL_POSITION_REQUIRED")
    return outcome
__all__=["require_real_outcome"]
