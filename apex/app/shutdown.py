"""Application-level graceful shutdown orchestration for APEX V3."""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

Hook = Callable[..., Any]

@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: Hook
    record_shutdown: Hook
    backup: Hook
    stop_market: Hook | None = None

async def _await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value

async def shutdown_production(deps: ShutdownDependencies, reason: str) -> None:
    """Fail closed, checkpoint state, release the lease, then deactivate runtime."""
    deps.runtime.inhibit_entries("SHUTDOWN")
    errors: list[Exception] = []
    if deps.stop_market is not None:
        try: await _await(deps.stop_market())
        except Exception as exc: errors.append(exc)
    try: await _await(deps.backup("shutdown"))
    except Exception as exc: errors.append(exc)
    try: await _await(deps.record_shutdown(deps.state_db_path, str(reason), instance_id=deps.instance_id))
    except Exception as exc: errors.append(exc)
    try: await _await(deps.release_lease())
    except Exception as exc: errors.append(exc)
    deps.runtime.clear_instance_lease()
    deps.runtime.deactivate()
    if errors:
        raise RuntimeError("graceful_shutdown_failed:" + ",".join(type(e).__name__ for e in errors))

__all__=["ShutdownDependencies","shutdown_production"]
