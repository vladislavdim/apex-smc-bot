"""Small async TTL cache with stale fallback and single-flight requests."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass
class CacheEntry:
    value: Any
    saved_at: float


class TTLCache:
    def __init__(self) -> None:
        self._items: dict[str, CacheEntry] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def get_or_fetch(
        self,
        key: str,
        ttl_seconds: int,
        stale_seconds: int,
        fetcher: Callable[[], Awaitable[Any]],
    ) -> tuple[Any, str, int | None]:
        now = time.time()
        entry = self._items.get(key)
        if entry and now - entry.saved_at <= ttl_seconds:
            return entry.value, "cached", int(now - entry.saved_at)

        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = time.time()
            entry = self._items.get(key)
            if entry and now - entry.saved_at <= ttl_seconds:
                return entry.value, "cached", int(now - entry.saved_at)
            try:
                value = await fetcher()
                if value is None:
                    raise ValueError("empty response")
                self._items[key] = CacheEntry(value=value, saved_at=time.time())
                return value, "fresh", 0
            except Exception:
                if entry and now - entry.saved_at <= stale_seconds:
                    return entry.value, "stale_fallback", int(now - entry.saved_at)
                raise

    def clear(self) -> None:
        self._items.clear()


cache = TTLCache()
