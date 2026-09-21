"""Process-local Groq budget telemetry and legacy strategy-AI policy."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable


_GROQ_DAILY_LIMIT = 480_000


@dataclass
class GroqTokenBudget:
    limit: int = _GROQ_DAILY_LIMIT
    clock: Callable[[], float] = time.time
    used: int = 0
    reset_at: float = 0.0

    def track(self, count: int) -> None:
        now = self.clock()
        if now - self.reset_at > 86_400:
            self.used = 0
            self.reset_at = now
        self.used += count

    def available(self) -> bool:
        return self.used < self.limit


_budget = GroqTokenBudget()
_legacy_strategy_enabled = False


def _track_tokens(count: int) -> None:
    _budget.track(count)


def _tokens_available() -> bool:
    return _budget.available()


def groq_tokens_used() -> int:
    return _budget.used


def configure_legacy_strategy_groq(enabled: bool) -> None:
    global _legacy_strategy_enabled
    _legacy_strategy_enabled = bool(enabled)


def legacy_strategy_groq_enabled() -> bool:
    """Fail closed unless the typed integration config explicitly enables it."""
    return _legacy_strategy_enabled


__all__ = [
    "GroqTokenBudget",
    "_GROQ_DAILY_LIMIT",
    "_tokens_available",
    "_track_tokens",
    "configure_legacy_strategy_groq",
    "groq_tokens_used",
    "legacy_strategy_groq_enabled",
]
