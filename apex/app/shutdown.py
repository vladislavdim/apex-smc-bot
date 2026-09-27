"""Fail-closed application shutdown orchestration for APEX V3."""
from __future__ import annotations
import inspect
from dataclasses import dataclass
from typing import Any,Callable
Hook=Callable[...,Any]
@dataclass(frozen=True)
class ShutdownDependencies:
    runtime:Any; state_db_path:str; instance_id:str
    release_lease:Hook|None=None; record_shutdown:Hook|None=None; backup:Hook|None=None; stop_market:Hook|None=None
async def _call(fn:Hook|None,*args:Any,**kwargs:Any)->None:
    if fn is None:return
    value=fn(*args,**kwargs)
    if inspect.isawaitable(value): await value
async def shutdown_production(deps:ShutdownDependencies,reason:str)->None:
    # Fence capital first. Cleanup is best-effort and never re-enables entries.
    deps.runtime.inhibit_entries("GRACEFUL_SHUTDOWN")
    errors=[]
    steps=(
        ("release_lease",deps.release_lease,(),{}),
        ("record_shutdown",deps.record_shutdown,(deps.state_db_path,reason),{"instance_id":deps.instance_id}),
        ("backup",deps.backup,("render_sigterm",),{}),
        ("stop_market",deps.stop_market,(),{}),
    )
    for name,fn,args,kwargs in steps:
        try: await _call(fn,*args,**kwargs)
        except Exception as exc: errors.append(f"{name}:{type(exc).__name__}")
    if errors:
        try: deps.runtime.inhibit_entries("GRACEFUL_SHUTDOWN_PARTIAL:"+",".join(errors))
        except Exception: pass
__all__=["ShutdownDependencies","shutdown_production"]
