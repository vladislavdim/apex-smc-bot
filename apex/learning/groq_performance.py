"""Small immutable Groq performance summary."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GroqPerformance:
    reviewed: int = 0
    approved: int = 0
    rejected: int = 0
    waited: int = 0


__all__ = ["GroqPerformance"]
