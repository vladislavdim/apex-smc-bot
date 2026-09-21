"""Canonical position-correlation repository facade."""
from apex.db.repositories.correlation import CorrelationError, TradeCorrelationRepository

PositionRepository = TradeCorrelationRepository

__all__ = ["CorrelationError", "TradeCorrelationRepository", "PositionRepository"]
