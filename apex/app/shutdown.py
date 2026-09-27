"""Fail-closed application shutdown composition for APEX V3."""
from __future__ import annotations
import inspect
import logging
from dataclasses import dataclass
from typing import Any,Awaitable,Callable
from apex.ops.graceful_shutdown import GracefulShutdown

@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: Callable[[],Awaitable[None]]
    record_shutdown: Callable[...,Any]
    backup: Callable[[str],Awaitable[None]]
    stop_market: Callable[[],Awaitable[None]]

async def _best_effort(label:str,call:Callable[[],Any])->None:
    try:
        value=call()
        if inspect.isawaitable(value): await value
    except Exception as exc:
        logging.warning("shutdown %s failed: %s",label,exc)

async def shutdown_production(deps:ShutdownDependencies,reason:str)->None:
    inhibit=getattr(deps.runtime,"inhibit_entries",None)
    if callable(inhibit): await _best_effort("inhibit",lambda: inhibit("GRACEFUL_SHUTDOWN"))
    await _best_effort("lease_release",deps.release_lease)
    await _best_effort("state_marker",lambda: deps.record_shutdown(deps.state_db_path,reason,instance_id=deps.instance_id))
    await _best_effort("backup",lambda: deps.backup("render_sigterm"))
    await _best_effort("market_stop",deps.stop_market)

__all__=["GracefulShutdown","ShutdownDependencies","shutdown_production"]
