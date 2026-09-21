"""Shared strategy helpers without strategy-specific policy."""
from __future__ import annotations

from apex.domain.enums import Strategy


def normalize_strategy(value: Strategy | str) -> Strategy:
    return value if isinstance(value, Strategy) else Strategy(str(value).upper())


__all__ = ["normalize_strategy"]
