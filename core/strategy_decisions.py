"""Deprecated compatibility facade for the V3 State repository."""

from apex.db.repositories.strategy_decisions import (
    configure_strategy_decision_state,
    record_strategy_decision,
)

__all__ = ["configure_strategy_decision_state", "record_strategy_decision"]
