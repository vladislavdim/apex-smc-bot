"""Fail-closed application shutdown composition for APEX V3."""
from __future__ import annotations
import inspect
from dataclasses import dataclass
from typing import Any,Awaitable,Callable
from apex.ops.graceful_shutdown import GracefulShutdown

@dataclass(frozen=True)
class ShutdownDependencies:
    runtime:Any
    state_db_path:str
    instance_id:str
    release_lease:Callable[[],Awaitable[Any]]
    record_shutdown:Callable[...,Any]
    backup:Callable[[str],Awaitable[Any]]
    stop_market:Callable[[],Awaitable[Any]]

async def _safe_call(fn,*args,**kwargs):
    try:
        result=fn(*args,**kwargs)
        if inspect.isawaitable(result): await result
    except Exception:
        return None
    return None

async def shutdown_production(deps:ShutdownDependencies,reason:str)->None:
    try: deps.runtime.inhibit_entries("GRACEFUL_SHUTDOWN")
    except Exception: pass
    await _safe_call(deps.release_lease)
    await _safe_call(deps.record_shutdown,deps.state_db_path,reason,instance_id=deps.instance_id)
    await _safe_call(deps.backup,"render_sigterm")
    await _safe_call(deps.stop_market)

def build_shutdown(*hooks):
    coordinator=GracefulShutdown()
    for hook in hooks: coordinator.add(hook)
    return coordinator
__all__=["GracefulShutdown","ShutdownDependencies","build_shutdown","shutdown_production"]
