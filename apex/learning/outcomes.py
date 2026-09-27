"""Live-outcome validation boundary. Only real confirmed position outcomes may enter learning."""
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome,*,confirmed_position:bool=True)->TradeOutcome:
    if not confirmed_position or not str(outcome.position_id).strip():
        raise ValueError("unconfirmed_position_forbidden")
    return outcome
__all__=["require_real_outcome"]
