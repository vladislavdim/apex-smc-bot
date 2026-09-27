"""Canonical application shutdown orchestration for APEX V3."""
from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: Callable[..., Any]
    record_shutdown: Callable[..., Any]
    backup: Callable[..., Any]
    stop_market: Callable[..., Any] | None = None


async def _maybe_await(value: Any) -> None:
    if inspect.isawaitable(value):
        await value


async def _safe(name: str, hook: Callable[..., Any] | None, *args: Any, **kwargs: Any) -> None:
    if hook is None:
        return
    try:
        await _maybe_await(hook(*args, **kwargs))
    except Exception as exc:
        # Shutdown is best-effort after entries are fenced. One failed cleanup
        # must never prevent the remaining persistence/transport cleanup.
        logging.warning("shutdown %s failed: %s", name, type(exc).__name__)


async def shutdown_production(deps: ShutdownDependencies, reason: str) -> None:
    """Fence entries first, then release ownership and finish every cleanup hook."""
    inhibit = getattr(deps.runtime, "inhibit_entries", None)
    if callable(inhibit):
        inhibit("GRACEFUL_SHUTDOWN")

    # Releasing the lease early prevents a terminating instance from retaining
    # production ownership while its final persistence hooks are running.
    await _safe("release_lease", deps.release_lease)
    await _safe("backup", deps.backup, "render_sigterm")
    await _safe(
        "record_shutdown", deps.record_shutdown, deps.state_db_path, reason,
        instance_id=deps.instance_id,
    )
    await _safe("stop_market", deps.stop_market)


__all__ = ["ShutdownDependencies", "shutdown_production"]
