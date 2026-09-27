"""Application-level graceful shutdown orchestration for APEX V3."""
from __future__ import annotations
import inspect
from dataclasses import dataclass
from typing import Any,Callable
Hook=Callable[...,Any]
@dataclass(frozen=True)
class ShutdownDependencies:
    runtime:Any
    state_db_path:str
    instance_id:str
    release_lease:Hook
    record_shutdown:Hook
    backup:Hook
    stop_market:Hook|None=None
async def _await(value:Any)->Any:
    return await value if inspect.isawaitable(value) else value
async def shutdown_production(deps:ShutdownDependencies,reason:str)->None:
    """Fence first; cleanup is best-effort and never re-opens entries."""
    deps.runtime.inhibit_entries("GRACEFUL_SHUTDOWN")
    # Release ownership early so the replacement instance can acquire fencing.
    try: await _await(deps.release_lease())
    except Exception: pass
    # Persistence failures must not prevent the remaining shutdown cleanup.
    try: await _await(deps.backup("render_sigterm"))
    except Exception: pass
    try: await _await(deps.record_shutdown(deps.state_db_path,str(reason),instance_id=deps.instance_id))
    except Exception: pass
    if deps.stop_market is not None:
        try: await _await(deps.stop_market())
        except Exception: pass
    clear=getattr(deps.runtime,"clear_instance_lease",None)
    if callable(clear): clear()
    deactivate=getattr(deps.runtime,"deactivate",None)
    if callable(deactivate): deactivate()
__all__=["ShutdownDependencies","shutdown_production"]
