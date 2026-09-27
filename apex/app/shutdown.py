"""Application-level graceful shutdown composition for APEX V3."""
from __future__ import annotations
from collections.abc import Awaitable,Callable
from apex.ops.graceful_shutdown import GracefulShutdown

ShutdownHook=Callable[[],Awaitable[None]]
def build_shutdown(*hooks:ShutdownHook)->GracefulShutdown:
    shutdown=GracefulShutdown()
    for hook in hooks: shutdown.add(hook)
    return shutdown
__all__=["GracefulShutdown","ShutdownHook","build_shutdown"]
