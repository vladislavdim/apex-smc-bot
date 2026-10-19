"""Low-overhead process memory guard.

It never stops reconciliation or Manager.  It only inhibits new entries and
lets callers skip optional work.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import gc
import sys
import threading


@dataclass(frozen=True)
class MemorySnapshot:
    rss_bytes: int
    limit_bytes: int
    ratio: float
    state: str
    service_bytes: int = 0
    working_set_bytes: int = 0


@dataclass
class MemoryReliefPolicy:
    """Retry relief during sustained pressure without collecting every tick."""

    cooldown_seconds: float = 300.0
    next_attempt_at: float = 0.0

    def claim(self, snapshot: MemorySnapshot, now: float) -> bool:
        if snapshot.state not in {"WATCH", "DEGRADED", "NEW_ENTRIES_OFF"}:
            return False
        if now < self.next_attempt_at:
            return False
        self.next_attempt_at = now + max(30.0, self.cooldown_seconds)
        return True


def memory_diagnostics() -> dict[str, int]:
    """Cheap counters only: never copy the live heap or log object contents."""
    gc_stats = gc.get_stats()
    return {
        "python_blocks": sys.getallocatedblocks(),
        "threads": threading.active_count(),
        "gc_collections": sum(item["collections"] for item in gc_stats),
        "gc_collected": sum(item["collected"] for item in gc_stats),
    }


def _cgroup_memory() -> tuple[int, int]:
    """Include child processes; exclude only reclaimable inactive file pages."""
    for usage_path, stat_path in (
        ('/sys/fs/cgroup/memory.current', '/sys/fs/cgroup/memory.stat'),
        ('/sys/fs/cgroup/memory/memory.usage_in_bytes', '/sys/fs/cgroup/memory/memory.stat'),
    ):
        try:
            with open(usage_path, encoding='utf-8') as source:
                usage = int(source.read().strip())
            with open(stat_path, encoding='utf-8') as source:
                stats = {row.split()[0]: int(row.split()[1]) for row in source if len(row.split()) == 2}
            inactive = stats.get('total_inactive_file', stats.get('inactive_file', 0))
            return max(0, usage), max(0, usage - inactive)
        except (OSError, ValueError):
            continue
    return 0, 0


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
    service, working_set = _cgroup_memory()
    used = max(rss, working_set)
    ratio = (used / limit) if used and limit else 0.0
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
    return MemorySnapshot(rss, limit, ratio, state, service, working_set)


def memory_snapshot_dict() -> dict[str, object]:
    return asdict(memory_snapshot())


def release_unused_memory() -> bool:
    """Best-effort return of freed Python arenas on Linux Render workers."""
    gc.collect()
    try:
        import ctypes
        return bool(ctypes.CDLL("libc.so.6").malloc_trim(0))
    except (AttributeError, OSError):
        return False


__all__ = ["MemorySnapshot", "MemoryReliefPolicy", "memory_diagnostics", "memory_snapshot", "memory_snapshot_dict", "release_unused_memory"]
