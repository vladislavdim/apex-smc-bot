"""Validated real-outcome boundary for Live Memory."""
from __future__ import annotations
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome,*,confirmed_position:bool,execution_id:str,candidate_id:str)->TradeOutcome:
    if not confirmed_position: raise ValueError("unconfirmed_position_forbidden")
    if not outcome.position_id or not execution_id or not candidate_id: raise ValueError("trade_correlation_incomplete")
    return outcome
__all__=["require_real_outcome"]
