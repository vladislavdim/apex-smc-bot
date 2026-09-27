"""Fail-closed application shutdown orchestration for APEX V3."""
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
    release_lease: Hook|None=None
    record_shutdown: Hook|None=None
    backup: Hook|None=None
    stop_market: Hook|None=None

async def _call(fn:Hook|None,*args:Any)->None:
    if fn is None:return
    value=fn(*args)
    if inspect.isawaitable(value): await value

async def shutdown_production(deps:ShutdownDependencies,reason:str)->None:
    # Block new capital risk before touching external resources.
    deps.runtime.inhibit_entries("SHUTDOWN",failed=False)
    errors=[]
    for name,fn,args in (
        ("stop_market",deps.stop_market,()),
        ("record_shutdown",deps.record_shutdown,(deps.state_db_path,deps.instance_id,reason)),
        ("backup",deps.backup,()),
        ("release_lease",deps.release_lease,()),
    ):
        try: await _call(fn,*args)
        except Exception as exc: errors.append(f"{name}:{type(exc).__name__}")
    deps.runtime.clear_instance_lease()
    if errors: deps.runtime.inhibit_entries("SHUTDOWN_PARTIAL:"+",".join(errors),failed=False)

__all__=["ShutdownDependencies","shutdown_production"]
