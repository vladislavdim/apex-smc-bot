"""Live-outcome validation boundary. Only real confirmed position outcomes may enter learning."""
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome)->TradeOutcome:
    if not str(outcome.position_id).strip(): raise ValueError("confirmed_position_required")
    return outcome
__all__=["require_real_outcome"]
