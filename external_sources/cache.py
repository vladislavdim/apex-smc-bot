"""Small async TTL cache with stale fallback and single-flight requests."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass
class CacheEntry:
    value: Any
    saved_at: float
    expires_at: float


class TTLCache:
    def __init__(self, max_entries: int = 128) -> None:
        self.max_entries = max(1, int(max_entries))
        self._items: OrderedDict[str, CacheEntry] = OrderedDict()
        self._locks: tuple[asyncio.Lock, ...] = ()
        self._loop = None
        self._failed_until: dict[str, float] = {}

    def _prune(self, now):
        for key, entry in list(self._items.items()):
            if entry.expires_at < now:
                self._items.pop(key, None)
        self._failed_until = {key: until for key, until in self._failed_until.items() if until > now}
        while len(self._items) > self.max_entries:
            self._items.popitem(last=False)
        while len(self._failed_until) > self.max_entries:
            self._failed_until.pop(next(iter(self._failed_until)))

    def _lock(self, key):
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            if self._loop is not None and not self._loop.is_closed():
                raise RuntimeError('cache_used_by_multiple_event_loops')
            self._loop = loop
            self._locks = tuple(asyncio.Lock() for _ in range(64))
        return self._locks[hash(key) % len(self._locks)]

    async def get_or_fetch(
        self,
        key: str,
        ttl_seconds: int,
        stale_seconds: int,
        fetcher: Callable[[], Awaitable[Any]],
    ) -> tuple[Any, str, int | None]:
        now = time.time()
        self._prune(now)
        entry = self._items.get(key)
        if entry and now - entry.saved_at <= ttl_seconds:
            return entry.value, "cached", int(now - entry.saved_at)

        lock = self._lock(key)
        async with lock:
            now = time.time()
            entry = self._items.get(key)
            if entry and now - entry.saved_at <= ttl_seconds:
                return entry.value, "cached", int(now - entry.saved_at)
            if now < self._failed_until.get(key, 0):
                if entry and now - entry.saved_at <= stale_seconds:
                    return entry.value, "stale_fallback", int(now-entry.saved_at)
                raise RuntimeError("source_cooldown")
            try:
                value = await fetcher()
                if value is None:
                    raise ValueError("empty response")
                saved_at = time.time()
                self._items[key] = CacheEntry(value=value, saved_at=saved_at,
                    expires_at=saved_at + max(ttl_seconds, stale_seconds))
                self._failed_until.pop(key, None)
                self._prune(saved_at)
                return value, "fresh", 0
            except Exception:
                self._failed_until[key] = time.time() + 60
                self._prune(time.time())
                if entry and now - entry.saved_at <= stale_seconds:
                    return entry.value, "stale_fallback", int(now - entry.saved_at)
                raise

    def clear(self) -> None:
        self._items.clear()
        self._failed_until.clear()


cache = TTLCache()
