"""Canonical signal repository facade."""
from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository, SignalLifecycleStateError

SignalRepository = SignalLifecycleRepository

__all__ = ["SignalLifecycleRepository", "SignalLifecycleStateError", "SignalRepository"]
