"""Fail-closed production shutdown composition for APEX V3."""
from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: Callable[[], Awaitable[Any]]
    record_shutdown: Callable[..., Any]
    backup: Callable[[str], Awaitable[Any]]
    stop_market: Callable[[], Awaitable[Any]]


async def _best_effort(label: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    try:
        value = fn(*args, **kwargs)
        if inspect.isawaitable(value):
            await value
    except Exception as exc:
        logging.warning("shutdown %s failed safely: %s", label, exc)


async def shutdown_production(deps: ShutdownDependencies, reason: str) -> None:
    """Fence entries first, then persist/release, always stopping market I/O."""
    try:
        inhibit = getattr(deps.runtime, "inhibit_entries", None)
        if callable(inhibit):
            inhibit("GRACEFUL_SHUTDOWN")
    except Exception as exc:
        logging.warning("shutdown entry fence failed safely: %s", exc)

    await _best_effort("lease release", deps.release_lease)
    await _best_effort(
        "state marker", deps.record_shutdown, deps.state_db_path, reason,
        instance_id=deps.instance_id,
    )
    try:\n        await deps.backup("render_sigterm")\n    except Exception as exc:\n        logging.warning("shutdown state backup failed safely: %s", exc)
    await _best_effort("market cleanup", deps.stop_market)


__all__ = ["ShutdownDependencies", "shutdown_production"]
