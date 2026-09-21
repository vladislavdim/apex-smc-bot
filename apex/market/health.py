"""Provider and symbol/timeframe market-data health registry."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import time
from typing import Any


@dataclass
class SourceHealth:
    source: str
    symbol: str
    timeframe: str
    status: str = "UNAVAILABLE"
    last_success: float | None = None
    last_closed_candle: float | None = None
    last_latency_ms: float | None = None
    requests: int = 0
    failures: int = 0
    rate_limits: int = 0
    gaps: int = 0

    def public(self, *, now: float | None = None) -> dict[str, Any]:
        current = now or time.time()
        return {
            **asdict(self),
            "age_seconds": None if self.last_success is None else max(0.0, current - self.last_success),
            "timeout_rate": self.failures / self.requests if self.requests else None,
            "rate_limit_rate": self.rate_limits / self.requests if self.requests else None,
        }


class MarketHealthRegistry:
    def __init__(self) -> None:
        self._state: dict[tuple[str, str, str], SourceHealth] = {}

    def record(
        self,
        source: str,
        symbol: str,
        timeframe: str,
        *,
        ok: bool,
        latency_ms: float | None = None,
        last_closed_candle: float | None = None,
        rate_limited: bool = False,
        gap: bool = False,
        now: float | None = None,
    ) -> SourceHealth:
        key = (source.lower(), symbol.upper(), timeframe.lower())
        state = self._state.setdefault(key, SourceHealth(*key))
        state.requests += 1
        state.last_latency_ms = latency_ms
        state.last_closed_candle = last_closed_candle or state.last_closed_candle
        state.failures += int(not ok)
        state.rate_limits += int(rate_limited)
        state.gaps += int(gap)
        state.status = "FRESH" if ok else "DEGRADED" if state.last_success else "UNAVAILABLE"
        if ok:
            state.last_success = now or time.time()
        return state

    def snapshot(self, *, now: float | None = None) -> tuple[dict[str, Any], ...]:
        return tuple(self._state[key].public(now=now) for key in sorted(self._state))


__all__ = ["MarketHealthRegistry", "SourceHealth"]
