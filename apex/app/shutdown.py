"""Production shutdown composition, independent of the worker and transport."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from apex.ops.graceful_shutdown import GracefulShutdown


@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: Callable[[], Awaitable[None]]
    record_shutdown: Callable[..., Any]
    backup: Callable[[str], Awaitable[Any]]
    stop_market: Callable[[], Awaitable[Any]] | None = None


async def shutdown_production(deps: ShutdownDependencies, reason: str) -> None:
    """Fence entries before cleanup; a failed cleanup must not skip later hooks."""
    deps.runtime.inhibit_entries("GRACEFUL_SHUTDOWN")
    try:
        await deps.release_lease()
    except Exception as exc:
        logging.warning("[APEX V3] shutdown lease release failed safely: %s", exc)
    try:
        await asyncio.to_thread(
            deps.record_shutdown, deps.state_db_path, reason,
            instance_id=deps.instance_id,
        )
    except Exception as exc:
        logging.warning("[APEX V3] shutdown marker failed safely: %s", exc)
    try:
        await asyncio.wait_for(deps.backup("render_sigterm"), timeout=30)
    except asyncio.TimeoutError:
        logging.warning("[BrainPersistence] final SIGTERM snapshot timed out safely")
    except Exception as exc:
        logging.warning("[BrainPersistence] final SIGTERM snapshot failed safely: %s", exc)
    if deps.stop_market is not None:
        try:
            await deps.stop_market()
        except Exception as exc:
            logging.warning("[APEX V3] market shutdown failed safely: %s", exc)


__all__ = ["GracefulShutdown", "ShutdownDependencies", "shutdown_production"]
