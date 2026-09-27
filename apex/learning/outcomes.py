"""Validated real-outcome boundary for Live Memory."""
from __future__ import annotations
from apex.domain.models import TradeOutcome

def require_real_outcome(outcome:TradeOutcome,*,confirmed_position:bool,execution_id:str|None=None,candidate_id:str|None=None)->TradeOutcome:
    if not confirmed_position: raise ValueError("unconfirmed_position_forbidden")
    if not outcome.position_id: raise ValueError("trade_correlation_incomplete")
    # When correlation identifiers are supplied, require the pair; persistence
    # performs the stronger canonical candidate/execution identity check.
    if (execution_id is None) != (candidate_id is None): raise ValueError("trade_correlation_incomplete")
    return outcome
__all__=["require_real_outcome"]
