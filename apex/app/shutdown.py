"""One bounded shutdown budget; outstanding writers retain shared ownership."""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from apex.ops.graceful_shutdown import GracefulShutdown

Hook = Callable[..., Any]
_PENDING: set[asyncio.Task] = set()


@dataclass(frozen=True)
class ShutdownDependencies:
    runtime: Any
    state_db_path: str
    instance_id: str
    release_lease: Hook | None = None
    record_shutdown: Hook | None = None
    backup: Hook | None = None
    stop_market: Hook | None = None
    drain: Hook | None = None
    timeout_seconds: float = 25.0
    drain_seconds: float = 5.0
    hard_exit: Hook | None = None


@dataclass(frozen=True)
class ShutdownResult:
    completed: bool
    lease_released: bool
    timed_out: tuple[str, ...]
    duration_seconds: float


async def _call(hook: Hook | None, *args: Any, **kwargs: Any) -> Any:
    if hook is None:
        return None
    if inspect.iscoroutinefunction(hook):
        return await hook(*args, **kwargs)
    value = await asyncio.to_thread(hook, *args, **kwargs)
    return await value if inspect.isawaitable(value) else value


def _finished(task):
    _PENDING.discard(task)
    if not task.cancelled():
        task.exception()


async def shutdown_production(
    deps: ShutdownDependencies, reason: str
) -> ShutdownResult:
    """Never release a shared lease while a timed-out writer is still alive.

    Critical exchange/message intents already have remote receipts before their
    external requests. Final State checkpointing is best effort within the same
    absolute budget, never an unbounded last opportunity to save an action.
    """
    started = time.monotonic()
    deadline = started + max(0.05, float(deps.timeout_seconds))
    timeouts = []
    released = False
    deps.runtime.inhibit_entries("GRACEFUL_SHUTDOWN")
    stop = getattr(deps.runtime, "stop", None)
    if callable(stop):
        stop()  # permanently fence this production instance, even after return
    watchdog = None
    if deps.hard_exit is not None:
        watchdog = threading.Timer(
            max(0.05, deadline - time.monotonic()), deps.hard_exit
        )
        watchdog.daemon = True
        watchdog.start()

    async def bounded(name, hook, *args, limit=None, reserve=0, **kwargs):
        if hook is None:
            return True
        remaining = max(
            0,
            deadline
            - time.monotonic()
            - reserve * min(1.0, float(deps.timeout_seconds) / 25),
        )
        duration = remaining if limit is None else min(remaining, max(0, limit))
        if duration <= 0:
            timeouts.append(name)
            return False
        task = asyncio.create_task(_call(hook, *args, **kwargs))
        _PENDING.add(task)
        task.add_done_callback(_finished)
        done, _ = await asyncio.wait({task}, timeout=duration)
        if not done:
            timeouts.append(name)
            logging.error(
                "[Shutdown] phase=%s status=DEADLINE_EXCEEDED lease=retained", name
            )
            return False
        try:
            if task.result() is False:
                timeouts.append(name + ":FAILED")
                return False
            return True
        except BaseException as exc:
            logging.warning("[Shutdown] phase=%s error=%s", name, type(exc).__name__)
            timeouts.append(name + ":FAILED")
            return False

    try:
        drained = await bounded(
            "drain", deps.drain, limit=deps.drain_seconds, reserve=4
        )
        await bounded(
            "marker",
            deps.record_shutdown,
            deps.state_db_path,
            str(reason)[:500],
            instance_id=deps.instance_id,
            limit=1,
            reserve=3,
        )
        backed_up = await bounded("state", deps.backup, "render_sigterm", reserve=3)
        if drained and backed_up and not any(not task.done() for task in _PENDING):

            async def release():
                nonlocal released
                confirmed = await _call(deps.release_lease)
                released = deps.release_lease is not None and confirmed is not False
                return confirmed

            await bounded("lease", release, limit=1, reserve=2)
        await bounded("market", deps.stop_market, limit=1, reserve=1)
        if not callable(stop):
            deactivate = getattr(deps.runtime, "deactivate", None)
            if callable(deactivate):
                deactivate()
        result = ShutdownResult(
            not timeouts, released, tuple(timeouts), time.monotonic() - started
        )
        logging.warning(
            "[Shutdown] completed=%s lease_released=%s timed_out=%s duration=%.3f",
            result.completed,
            released,
            result.timed_out,
            result.duration_seconds,
        )
        if any(not task.done() for task in _PENDING) and deps.hard_exit is not None:
            deps.hard_exit()  # Python's executor join must not exceed SIGTERM budget
        return result
    finally:
        if watchdog is not None:
            watchdog.cancel()


__all__ = [
    "GracefulShutdown",
    "ShutdownDependencies",
    "ShutdownResult",
    "shutdown_production",
]
