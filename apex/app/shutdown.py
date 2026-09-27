"""Canonical application shutdown orchestration for APEX V3."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any,Awaitable,Callable

AsyncHook=Callable[...,Awaitable[Any]]

@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: AsyncHook
    record_shutdown: AsyncHook
    backup: AsyncHook
    stop_market: AsyncHook|None=None

async def _call(hook:AsyncHook|None,*args:Any)->None:
    if hook is not None:
        await hook(*args)

async def shutdown_production(deps:ShutdownDependencies,reason:str)->None:
    """Fail closed, stop producers, persist, then release the instance lease."""
    inhibit=getattr(deps.runtime,"inhibit_entries",None)
    if callable(inhibit): inhibit(f"SHUTDOWN:{reason}")
    errors=[]
    for name,hook,args in (
        ("stop_market",deps.stop_market,()),
        ("backup",deps.backup,()),
        ("record_shutdown",deps.record_shutdown,(reason,)),
        ("release_lease",deps.release_lease,()),
    ):
        try: await _call(hook,*args)
        except Exception as exc: errors.append(f"{name}:{type(exc).__name__}")
    if errors: raise RuntimeError("shutdown_incomplete:"+",".join(errors))

__all__=["ShutdownDependencies","shutdown_production"]
