"""CPU and event-loop responsiveness watchdogs."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Awaitable, Callable


@dataclass(frozen=True)
class CpuSnapshot:
    ratio: float
    state: str


class ProcessCpuMonitor:
    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        process_time: Callable[[], float] = time.process_time,
    ) -> None:
        self._monotonic = monotonic
        self._process_time = process_time
        self._wall = monotonic()
        self._cpu = process_time()

    def sample(self) -> CpuSnapshot:
        wall = self._monotonic()
        cpu = self._process_time()
        elapsed = max(0.000001, wall - self._wall)
        used = max(0.0, cpu - self._cpu)
        self._wall, self._cpu = wall, cpu
        ratio = min(1.0, used / elapsed)
        state = "DEGRADED" if ratio >= 0.90 else "WATCH" if ratio >= 0.75 else "NORMAL"
        return CpuSnapshot(ratio, state)


@dataclass(frozen=True)
class LagSnapshot:
    current_ms: float
    p95_ms: float
    max_ms: float
    breach_count: int
    state: str


class EventLoopLagMonitor:
    def __init__(
        self,
        *,
        interval_seconds: float = 1.0,
        sla_ms: float = 500.0,
        window: int = 120,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.interval_seconds = max(0.05, float(interval_seconds))
        self.sla_ms = max(1.0, float(sla_ms))
        self._samples: deque[float] = deque(maxlen=max(10, int(window)))
        self._breaches: deque[bool] = deque(maxlen=max(10, int(window)))
        self._monotonic = monotonic
        self._sleeper = sleeper

    async def sample(self) -> LagSnapshot:
        expected = self._monotonic() + self.interval_seconds
        await self._sleeper(self.interval_seconds)
        lag_ms = max(0.0, self._monotonic() - expected) * 1000.0
        self._samples.append(lag_ms)
        self._breaches.append(lag_ms > self.sla_ms)
        breach_count = sum(self._breaches)
        ordered = sorted(self._samples)
        p95_index = max(0, min(len(ordered) - 1, int(len(ordered) * 0.95) - 1))
        p95 = ordered[p95_index] if ordered else 0.0
        state = "DEGRADED" if breach_count >= 3 and p95 > self.sla_ms else "NORMAL"
        return LagSnapshot(lag_ms, p95, max(ordered, default=0.0), breach_count, state)


__all__ = ["CpuSnapshot", "EventLoopLagMonitor", "LagSnapshot", "ProcessCpuMonitor"]
