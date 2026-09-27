"""Application shutdown composition for APEX V3."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any,Awaitable,Callable
from apex.ops.graceful_shutdown import GracefulShutdown
@dataclass(frozen=True)
class ShutdownDependencies:
    hooks: tuple[Callable[[],Awaitable[Any]],...]=()
async def shutdown_production(deps:ShutdownDependencies)->None:
    coordinator=GracefulShutdown()
    for hook in deps.hooks: coordinator.add_hook(hook)
    await coordinator.run()
__all__=["GracefulShutdown","ShutdownDependencies","shutdown_production"]
