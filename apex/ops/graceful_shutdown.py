"""Idempotent graceful-shutdown coordinator."""
from __future__ import annotations
import asyncio
from collections.abc import Awaitable,Callable
class GracefulShutdown:
    def __init__(self)->None: self._lock=asyncio.Lock(); self._done=False
    async def run(self,*hooks:Callable[[],Awaitable[None]])->None:
        async with self._lock:
            if self._done:return
            for hook in hooks: await hook()
            self._done=True
__all__=["GracefulShutdown"]
