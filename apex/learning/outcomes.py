"""Validation boundary for outcomes admitted to Live Memory."""
from __future__ import annotations
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome,*,confirmed_position:bool)->TradeOutcome:
    if not confirmed_position:
        raise ValueError("unconfirmed_position_forbidden")
    if not outcome.position_id:
        raise ValueError("position_identity_missing")
    return outcome
__all__=["require_real_outcome"]
