"""Validation boundary for real, fully correlated live outcomes."""
from __future__ import annotations
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome,*,candidate_id:str,execution_id:str,confirmed_position:bool)->TradeOutcome:
    if not confirmed_position: raise ValueError("unconfirmed_position_forbidden")
    if not candidate_id or not execution_id or not outcome.position_id: raise ValueError("trade_correlation_incomplete")
    return outcome
__all__=["require_real_outcome"]
