"""One production scheduler for webhook and polling transports."""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from apex.domain.enums import ComponentState
from apex.telemetry.incidents import recover_incident, report_incident
from apex.telemetry.job_metrics import recorder_for

from .job_registry import JOB_BY_ID
from .scan_budget import ScanDeadlineExceeded
from .runtime import runtime_supervisor


JOB_COMPONENT = {
    "execution_reconcile": "binance_reconciliation",
    "trade_manager": "manager",
    "market_intelligence_primary": "market_intelligence",
    "market_fast": "scanner_fast",
    "market_mtf_1h": "scanner_mtf",
    "market_zone": "scanner_zone",
    "market_swing": "scanner_swing",
    "market_wyckoff": "scanner_wyckoff",
    "state_backup": "backup",
    "memory_backup": "memory_backup",
    "brain_backup": "brain_backup",
}


@dataclass(frozen=True)
class SchedulerCallbacks:
    signal_outcome_refresh: Callable[..., Any]
    execution_reconcile: Callable[..., Any]
    trade_manager: Callable[..., Any]
    market_intelligence_primary: Callable[..., Any]
    market_fast: Callable[..., Any]
    market_mtf_1h: Callable[..., Any]
    market_zone: Callable[..., Any]
    market_swing: Callable[..., Any]
    market_wyckoff: Callable[..., Any]
    market_ltf_watch: Callable[..., Any]
    keepalive: Callable[..., Any]
    alerts: Callable[..., Any]
    state_backup: Callable[..., Any]
    runtime_watchdog: Callable[..., Any]
    memory_backup: Callable[..., Any] | None = None
    brain_backup: Callable[..., Any] | None = None


_RUNNING_JOBS: set[asyncio.Task] = set()
_JOB_OWNERS: dict[str, asyncio.Task] = {}


def _job_finished(task: asyncio.Task) -> None:
    _RUNNING_JOBS.discard(task)
    if not task.cancelled():
        task.exception()  # retrieve late failures after a scheduler timeout


async def drain_production_jobs() -> None:
    """Call after scheduler.pause(); a timed-out to_thread still owns writes."""
    while _RUNNING_JOBS:
        await asyncio.gather(*tuple(_RUNNING_JOBS), return_exceptions=True)


async def _invoke(callback: Callable[..., Any]) -> Any:
    if inspect.iscoroutinefunction(callback):
        return await callback()
    result = await asyncio.to_thread(callback)
    if inspect.isawaitable(result):
        return await result
    return result


async def _measured_invoke(job_id, callback):
    measured = job_id.startswith('market_') or job_id.endswith('_backup')
    def record(phase):
        from apex.ops.resource_guard import memory_snapshot
        snapshot = memory_snapshot()
        logger = logging.getLogger('apex.resources')
        logger.setLevel(logging.INFO)
        logger.info('[APEX Resources] job=%s phase=%s rss=%s service=%s working_set=%s limit=%s',
                    job_id, phase, snapshot.rss_bytes, snapshot.service_bytes,
                    snapshot.working_set_bytes, snapshot.limit_bytes)
    if measured:
        record('start')
    try:
        return await _invoke(callback)
    finally:
        if measured:
            record('end')


def _guarded(job_id: str, callback: Callable[..., Any]) -> Callable[[], Any]:
    definition = JOB_BY_ID.get(job_id)
    timeout = definition.timeout_seconds if definition else 300

    @functools.wraps(callback)
    async def run() -> Any:
        previous = _JOB_OWNERS.get(job_id)
        if previous is not None and not previous.done():
            logging.warning("[APEX V3] job=%s status=SKIPPED_STILL_RUNNING", job_id)
            return False
        component = JOB_COMPONENT.get(job_id)
        component_required = bool(definition and definition.critical)
        reason_code = f"JOB_FAILED:{job_id}"
        recorder = recorder_for(job_id)
        if recorder is not None:
            recorder.__enter__()
        late = {"value": False}
        def completed(task):
            _job_finished(task)
            if _JOB_OWNERS.get(job_id) is task:
                _JOB_OWNERS.pop(job_id, None)
            if not late["value"]:
                return
            error = None if task.cancelled() else task.exception()
            success = not task.cancelled() and error is None and task.result() is not False
            if success:
                if component:
                    runtime_supervisor.mark_component(component, ComponentState.READY, required=component_required)
                    recover_incident("JOB_TIMEOUT", component)
                    recover_incident("JOB_FAILED", component)
                runtime_supervisor.clear_inhibit(reason_code)
            elif error is not None and component:
                runtime_supervisor.mark_component(component, ComponentState.DEGRADED, type(error).__name__, required=component_required)
                report_incident("JOB_FAILED", component, "ERROR", {"job_id": job_id, "late": True, "error_type": type(error).__name__})
            if recorder is not None:
                recorder.finish_late("LATE_OK" if success else "LATE_ERROR" if error else "LATE_SKIPPED",
                                     error_code=type(error).__name__ if error else "")
            logging.info("[APEX V3] job=%s status=%s", job_id, "LATE_OK" if success else "LATE_FAILED")
        try:
            work = asyncio.create_task(_measured_invoke(job_id, callback))
            _RUNNING_JOBS.add(work)
            _JOB_OWNERS[job_id] = work
            work.add_done_callback(completed)
            result = await asyncio.wait_for(asyncio.shield(work), timeout=timeout)
            if result is False:
                # A lock/admission skip did not verify the previously failing job.
                if recorder is not None:
                    recorder.finish("SKIPPED")
                return result
            if component:
                runtime_supervisor.mark_component(component, ComponentState.READY, required=component_required)
                recover_incident("JOB_TIMEOUT", component)
                recover_incident("JOB_FAILED", component)
            runtime_supervisor.clear_inhibit(reason_code)
            if recorder is not None:
                if isinstance(result, dict):
                    processed = result.get("items_processed", result.get("processed", 0))
                elif isinstance(result, (list, tuple, set)):
                    processed = len(result)
                else:
                    processed = 0
                recorder.finish("OK", items_processed=int(processed or 0))
            return result
        except asyncio.TimeoutError as exc:
            actual_timeout = exc.timeout_seconds if isinstance(exc, ScanDeadlineExceeded) else timeout
            late["value"] = not work.done()
            logging.error("[APEX V3] job=%s status=FAILED_TIMEOUT timeout=%ss", job_id, actual_timeout)
            if component:
                runtime_supervisor.mark_component(component, ComponentState.DEGRADED, "FAILED_TIMEOUT", required=component_required)
                report_incident(
                    "JOB_TIMEOUT", component,
                    "ERROR" if definition and definition.critical else "WARNING",
                    {"job_id": job_id, "timeout_seconds": actual_timeout,
                     "deadline_source": "scan" if isinstance(exc, ScanDeadlineExceeded) else "scheduler"},
                )
            # Only jobs that protect trading safety may inhibit new entries.
            # Maintenance/telemetry jobs can degrade and alert, but must never
            # stop an otherwise healthy scanner -> risk -> execution pipeline.
            if definition and definition.critical:
                runtime_supervisor.inhibit_entries(reason_code)
            else:
                runtime_supervisor.clear_inhibit(reason_code)
            if recorder is not None:
                recorder.finish("FAILED_TIMEOUT", error_code="TIMEOUT")
            raise
        except asyncio.CancelledError:
            late["value"] = True
            if recorder is not None:
                recorder.finish("CANCELLED", error_code="CANCELLED")
            raise
        except Exception as exc:
            logging.error("[APEX V3] job=%s status=ERROR type=%s", job_id, type(exc).__name__)
            if component:
                runtime_supervisor.mark_component(component, ComponentState.DEGRADED, type(exc).__name__, required=component_required)
                report_incident(
                    "JOB_FAILED", component,
                    "ERROR" if definition and definition.critical else "WARNING",
                    {"job_id": job_id, "error_type": type(exc).__name__},
                )
            if definition and definition.critical:
                runtime_supervisor.inhibit_entries(reason_code)
            if recorder is not None:
                recorder.finish("ERROR", error_code=type(exc).__name__)
            raise

    return run


def build_production_scheduler(
    callbacks: SchedulerCallbacks,
    *,
    execution_reconcile_seconds: int,
    scheduler_factory: Callable[..., Any] | None = None,
) -> Any:
    if scheduler_factory is None:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        scheduler_factory = AsyncIOScheduler
    scheduler = scheduler_factory(
        job_defaults={"misfire_grace_time": 180, "coalesce": True, "max_instances": 1}
    )

    def add(job_id: str, callback: Callable[..., Any], trigger: str, **options: Any) -> None:
        scheduler.add_job(
            _guarded(job_id, callback),
            trigger,
            id=job_id,
            max_instances=1,
            coalesce=True,
            **options,
        )

    add("signal_outcome_refresh", callbacks.signal_outcome_refresh, "interval", minutes=5, jitter=20)
    add("execution_reconcile", callbacks.execution_reconcile, "interval", seconds=max(5, int(execution_reconcile_seconds)))
    add("market_mtf_1h", callbacks.market_mtf_1h, "cron", minute="2,24", timezone="UTC")
    add("market_fast", callbacks.market_fast, "cron", minute="8,28,48", timezone="UTC")
    add("market_zone", callbacks.market_zone, "cron", minute="14,34,54", timezone="UTC")
    add("market_swing", callbacks.market_swing, "cron", minute="20,50", timezone="UTC")
    add("market_ltf_watch", callbacks.market_ltf_watch, "cron", minute="6,16,26,36,46,56", timezone="UTC")
    add("market_wyckoff", callbacks.market_wyckoff, "cron", minute=40, timezone="UTC")
    add("market_intelligence_primary", callbacks.market_intelligence_primary, "cron", minute=10, timezone="UTC")
    add("trade_manager", callbacks.trade_manager, "cron", minute="1,6,11,16,21,26,31,36,41,46,51,56", timezone="UTC")
    add("keepalive", callbacks.keepalive, "interval", minutes=10)
    add("alerts", callbacks.alerts, "interval", minutes=5)
    add("state_backup", callbacks.state_backup, "cron", minute="3,33", timezone="UTC")
    for name, minute in (("memory_backup", "13,43"), ("brain_backup", "23,53")):
        callback = getattr(callbacks, name)
        if callback is not None:
            add(name, callback, "cron", minute=minute, timezone="UTC")
    add("runtime_watchdog", callbacks.runtime_watchdog, "interval", seconds=15)
    return scheduler


__all__ = ["SchedulerCallbacks", "build_production_scheduler"]
