"""Process-local metric primitives for telemetry adapters."""
from __future__ import annotations

from collections import Counter

_COUNTERS: Counter[str] = Counter()


def increment(name: str, value: int = 1) -> int:
    _COUNTERS[str(name)] += int(value)
    return _COUNTERS[str(name)]


def snapshot() -> dict[str, int]:
    return dict(_COUNTERS)


__all__ = ["increment", "snapshot"]
