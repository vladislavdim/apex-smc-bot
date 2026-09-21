"""Canonical live-learning repository facade."""
from apex.learning.live_memory import LiveMemoryError, LiveMemoryRepository

LearningRepository = LiveMemoryRepository

__all__ = ["LiveMemoryError", "LiveMemoryRepository", "LearningRepository"]
