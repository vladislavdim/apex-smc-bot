"""Deterministic rotating batches for bounded full-market scans."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from typing import TypeVar


T = TypeVar("T")


class RotatingBatcher:
    def __init__(self) -> None:
        self._cursors: dict[str, int] = {}
        self._lock = threading.Lock()

    def take(self, key: str, items: Sequence[T], size: int) -> list[T]:
        values = list(items)
        if not values:
            return []
        count = min(len(values), max(1, int(size)))
        with self._lock:
            start = self._cursors.get(key, 0) % len(values)
            batch = [values[(start + offset) % len(values)] for offset in range(count)]
            self._cursors[key] = (start + count) % len(values)
        return batch


market_scan_batches = RotatingBatcher()
