"""Canonical graceful production shutdown orchestration for APEX V3."""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from apex.ops.graceful_shutdown import GracefulShutdown

Hook = Callable[..., Any]


@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: Hook | None = None
    record_shutdown: Hook | None = None
    backup: Hook | None = None
    stop_market: Hook | None = None


async def _call(hook: Hook | None, *args: Any, **kwargs: Any) -> Any:
    if hook is None:
        return None
    value = hook(*args, **kwargs)
    if inspect.isawaitable(value):
        return await value
    return value


async def shutdown_production(deps: ShutdownDependencies, reason: str) -> None:
    """Stop producers, persist state, record shutdown, then release fencing.

    The lease is deliberately released last so a replacement instance cannot
    become READY while this instance is still writing its final checkpoint.
    Every step is best-effort, but the runtime is fenced from new entries first.
    """
    why = str(reason or "shutdown")[:500]
    try:
        deps.runtime.inhibit_entries("SHUTDOWN_IN_PROGRESS")
    except Exception:
        pass

    first_error: BaseException | None = None
    for hook, args, kwargs in (
        (deps.stop_market, (), {}),
        (deps.backup, ("shutdown",), {}),
        (deps.record_shutdown, (deps.state_db_path, why), {"instance_id": deps.instance_id}),
        (deps.release_lease, (), {}),
    ):
        try:
            await _call(hook, *args, **kwargs)
        except BaseException as exc:
            if first_error is None:
                first_error = exc

    try:
        deps.runtime.deactivate()
    except Exception:
        pass
    if first_error is not None:
        raise first_error


__all__ = ["GracefulShutdown", "ShutdownDependencies", "shutdown_production"]
