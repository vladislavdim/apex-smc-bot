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
from .runtime import runtime_supervisor


JOB_COMPONENT = {
    "execution_reconcile": "binance_reconciliation",
    "trade_manager": "manager",
    "market_intelligence_primary": "market_data",
    "market_fast": "scanner_fast",
    "market_mtf_1h": "scanner_mtf",
    "market_zone": "scanner_zone",
    "market_swing": "scanner_swing",
    "market_wyckoff": "scanner_wyckoff",
    "dashboard_telemetry": "dashboard_telemetry",
    "state_backup": "backup",
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
    dashboard_telemetry: Callable[..., Any]
    alerts: Callable[..., Any]
    state_backup: Callable[..., Any]
    runtime_watchdog: Callable[..., Any]


async def _invoke(callback: Callable[..., Any]) -> Any:
    if inspect.iscoroutinefunction(callback):
        return await callback()
    result = await asyncio.to_thread(callback)
    if inspect.isawaitable(result):
        return await result
    return result


def _guarded(job_id: str, callback: Callable[..., Any]) -> Callable[[], Any]:
    definition = JOB_BY_ID.get(job_id)
    timeout = definition.timeout_seconds if definition else 300

    @functools.wraps(callback)
    async def run() -> Any:
        component = JOB_COMPONENT.get(job_id)
        reason_code = f"JOB_FAILED:{job_id}"
        recorder = recorder_for(job_id)
        if recorder is not None:
            recorder.__enter__()
        try:
            result = await asyncio.wait_for(_invoke(callback), timeout=timeout)
            if component:
                runtime_supervisor.mark_component(component, ComponentState.READY)
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
        except asyncio.TimeoutError:
            logging.error("[APEX V3] job=%s status=FAILED_TIMEOUT timeout=%ss", job_id, timeout)
            if component:
                runtime_supervisor.mark_component(component, ComponentState.DEGRADED, "FAILED_TIMEOUT")
                report_incident(
                    "JOB_TIMEOUT", component,
                    "ERROR" if definition and definition.critical else "WARNING",
                    {"job_id": job_id, "timeout_seconds": timeout},
                )
            if definition and definition.critical:
                runtime_supervisor.inhibit_entries(reason_code)
            if recorder is not None:
                recorder.finish("FAILED_TIMEOUT", error_code="TIMEOUT")
            raise
        except asyncio.CancelledError:
            if recorder is not None:
                recorder.finish("CANCELLED", error_code="CANCELLED")
            raise
        except Exception as exc:
            logging.error("[APEX V3] job=%s status=ERROR type=%s", job_id, type(exc).__name__)
            if component:
                runtime_supervisor.mark_component(component, ComponentState.DEGRADED, type(exc).__name__)
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
    add("dashboard_telemetry", callbacks.dashboard_telemetry, "interval", minutes=10)
    add("alerts", callbacks.alerts, "interval", minutes=5)
    add("state_backup", callbacks.state_backup, "interval", minutes=30, jitter=120)
    add("runtime_watchdog", callbacks.runtime_watchdog, "interval", seconds=15)
    return scheduler


__all__ = ["SchedulerCallbacks", "build_production_scheduler"]
