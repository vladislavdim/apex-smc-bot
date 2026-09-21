"""Low-overhead process memory guard.

It never stops reconciliation or Manager.  It only inhibits new entries and
lets callers skip optional work.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class MemorySnapshot:
    rss_bytes: int
    limit_bytes: int
    ratio: float
    state: str


def _rss_bytes() -> int:
    try:
        with open("/proc/self/status", encoding="utf-8") as source:
            for line in source:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


def _memory_limit_bytes(explicit: int = 0) -> int:
    if explicit > 0:
        return explicit
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path, encoding="utf-8") as source:
                raw = source.read().strip()
            if raw and raw != "max":
                value = int(raw)
                if 0 < value < 1 << 60:
                    return value
        except (OSError, ValueError):
            continue
    return 0


def memory_snapshot(
    *,
    watch_ratio: float = 0.65,
    degraded_ratio: float = 0.75,
    stop_ratio: float = 0.85,
    limit_bytes: int = 0,
) -> MemorySnapshot:
    if not (0 < watch_ratio < degraded_ratio < stop_ratio <= 1):
        raise ValueError("invalid_memory_thresholds")
    rss = _rss_bytes()
    limit = _memory_limit_bytes(limit_bytes)
    ratio = (rss / limit) if rss and limit else 0.0
    if not limit:
        state = "UNKNOWN"
    elif ratio > stop_ratio:
        state = "NEW_ENTRIES_OFF"
    elif ratio > degraded_ratio:
        state = "DEGRADED"
    elif ratio >= watch_ratio:
        state = "WATCH"
    else:
        state = "NORMAL"
    return MemorySnapshot(rss, limit, ratio, state)


def memory_snapshot_dict() -> dict[str, object]:
    return asdict(memory_snapshot())


__all__ = ["MemorySnapshot", "memory_snapshot", "memory_snapshot_dict"]
