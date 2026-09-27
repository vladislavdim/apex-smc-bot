"""Fail-closed application shutdown orchestration for APEX V3."""
from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable

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


async def _call(fn: Hook | None, *args: Any, **kwargs: Any) -> None:
    if fn is None:
        return
    value = fn(*args, **kwargs)
    if inspect.isawaitable(value):
        await value


async def shutdown_production(deps: ShutdownDependencies, reason: str) -> None:
    """Fence entries first, then perform independent best-effort cleanup.

    Cleanup failures are logged but do not replace the canonical shutdown
    inhibit reason or prevent later cleanup steps from running.
    """
    deps.runtime.inhibit_entries("GRACEFUL_SHUTDOWN")

    try:
        await _call(deps.release_lease)
    except Exception as exc:
        logging.warning("[APEX V3] shutdown lease release failed: %s", type(exc).__name__)

    try:
        await _call(deps.record_shutdown, deps.state_db_path, reason, instance_id=deps.instance_id)
    except Exception as exc:
        logging.warning("[APEX V3] shutdown marker failed: %s", type(exc).__name__)

    try:
        if deps.backup is not None:
            value = deps.backup("render_sigterm")
            if inspect.isawaitable(value):
                await value
    except Exception as exc:
        logging.warning("[APEX V3] shutdown backup failed: %s", type(exc).__name__)

    try:
        await _call(deps.stop_market)
    except Exception as exc:
        logging.warning("[APEX V3] shutdown market stop failed: %s", type(exc).__name__)


__all__ = ["ShutdownDependencies", "shutdown_production"]
