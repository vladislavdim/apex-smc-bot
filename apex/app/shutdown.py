"""Canonical fail-closed production shutdown orchestration for APEX V3."""
from __future__ import annotations
import inspect
from dataclasses import dataclass
from typing import Any,Awaitable,Callable

Hook=Callable[...,Any]

@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: Hook
    record_shutdown: Hook
    backup: Hook
    stop_market: Hook|None=None

async def _call(fn:Hook|None,*args:Any)->Any:
    if fn is None:return None
    value=fn(*args)
    return await value if inspect.isawaitable(value) else value

async def shutdown_production(deps:ShutdownDependencies,reason:str)->None:
    """Stop producers, persist state, record shutdown, then release fencing.

    Entry admission is disabled first and the lease is released last, so a
    replacement worker cannot overlap this instance while durable state is
    still being checkpointed.
    """
    deps.runtime.inhibit_entries("SHUTDOWN_IN_PROGRESS")
    errors=[]
    try:
        try: await _call(deps.stop_market)
        except Exception as exc: errors.append(("stop_market",exc))
        try: await _call(deps.backup,"shutdown")
        except TypeError:
            try: await _call(deps.backup)
            except Exception as exc: errors.append(("backup",exc))
        except Exception as exc: errors.append(("backup",exc))
        try:
            value=deps.record_shutdown(deps.state_db_path,reason,instance_id=deps.instance_id)
            if inspect.isawaitable(value): await value
        except Exception as exc: errors.append(("record_shutdown",exc))
    finally:
        try: await _call(deps.release_lease)
        finally: deps.runtime.clear_instance_lease()
    if errors:
        names=",".join(name for name,_ in errors)
        raise RuntimeError(f"shutdown_hooks_failed:{names}") from errors[0][1]

__all__=["ShutdownDependencies","shutdown_production"]
