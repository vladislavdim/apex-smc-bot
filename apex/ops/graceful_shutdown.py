"""Idempotent graceful-shutdown callback runner."""
from __future__ import annotations

import inspect
from typing import Awaitable, Callable

ShutdownCallback = Callable[[str], Awaitable[None] | None]


class GracefulShutdown:
    def __init__(self) -> None:
        self._callbacks: list[ShutdownCallback] = []
        self._running = False

    def register(self, callback: ShutdownCallback) -> None:
        self._callbacks.append(callback)

    async def run(self, reason: str) -> None:
        if self._running:
            return
        self._running = True
        for callback in reversed(self._callbacks):
            result = callback(str(reason))
            if inspect.isawaitable(result):
                await result


__all__ = ["GracefulShutdown", "ShutdownCallback"]
