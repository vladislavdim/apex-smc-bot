"""Canonical graceful production shutdown orchestration for APEX V3."""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable

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
    return await value if inspect.isawaitable(value) else value

async def shutdown_production(deps: ShutdownDependencies, reason: str) -> None:
    """Fence entries immediately, release the production lease, then clean up.

    Shutdown is deliberately best-effort: failure of fencing release, final
    persistence, the shutdown marker, or market cleanup must not prevent the
    remaining cleanup steps from running during SIGTERM.
    """
    why = str(reason or "shutdown")[:500]
    try:
        deps.runtime.inhibit_entries("GRACEFUL_SHUTDOWN")
    except Exception:
        pass

    # Release first after local entry fencing so a rolling replacement is not
    # held behind cleanup that can include a slow remote persistence call.
    try:
        await _call(deps.release_lease)
    except BaseException:
        pass

    # Keep the final persistence call explicit: it is a production invariant
    # and is intentionally covered by the persistence safety test.
    try:
        if deps.backup is not None:
            value = deps.backup("render_sigterm")
            if inspect.isawaitable(value):
                await value
    except BaseException:
        pass

    for hook, args, kwargs in (
        (deps.record_shutdown, (deps.state_db_path, why), {"instance_id": deps.instance_id}),
        (deps.stop_market, (), {}),
    ):
        try:
            await _call(hook, *args, **kwargs)
        except BaseException:
            # Process termination must continue through every cleanup hook.
            continue

    try:
        deps.runtime.deactivate()
    except Exception:
        pass

__all__ = ["GracefulShutdown", "ShutdownDependencies", "shutdown_production"]
