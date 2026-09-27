"""Production shutdown orchestration for APEX V3.

Shutdown is fail-closed for new entries and best-effort for cleanup: lease,
state marker, persistence and market cleanup are attempted independently.
"""
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
    release_lease: Callable[[], Awaitable[Any]] | None = None
    record_shutdown: Callable[..., Any] | None = None
    backup: Callable[[str], Awaitable[Any]] | None = None
    stop_market: Callable[[], Any] | None = None


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def shutdown_production(deps: ShutdownDependencies, reason: str) -> None:
    """Fence entries first, then perform every cleanup step without bypassing later steps."""
    try:
        inhibit = getattr(deps.runtime, "inhibit_entries", None)
        if callable(inhibit):
            inhibit("GRACEFUL_SHUTDOWN")
    except Exception:
        logging.exception("[APEX V3] failed to inhibit entries during shutdown")

    if deps.release_lease is not None:
        try:
            await _maybe_await(deps.release_lease())
        except Exception:
            logging.exception("[APEX V3] lease release failed during shutdown")

    if deps.record_shutdown is not None:
        try:
            deps.record_shutdown(deps.state_db_path, reason, instance_id=deps.instance_id)
        except Exception:
            logging.exception("[APEX V3] shutdown marker failed")

    if deps.backup is not None:
        try:
            await _maybe_await(deps.backup("render_sigterm"))
        except Exception:
            logging.exception("[APEX V3] final persistence failed during shutdown")

    if deps.stop_market is not None:
        try:
            await _maybe_await(deps.stop_market())
        except Exception:
            logging.exception("[APEX V3] market cleanup failed during shutdown")


__all__ = ["ShutdownDependencies", "shutdown_production"]
