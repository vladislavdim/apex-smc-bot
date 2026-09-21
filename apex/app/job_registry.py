"""Single declarative registry of production jobs.

Phase 1 records canonical ownership and resource policy.  The legacy scheduler
will be replaced with this registry incrementally, while tests prevent jobs
from silently regaining Shadow/Research responsibilities.
"""

from __future__ import annotations

from dataclasses import dataclass

from apex.domain.enums import JobPriority


@dataclass(frozen=True)
class JobDefinition:
    id: str
    schedule: str
    timeout_seconds: int
    priority: JobPriority
    critical: bool
    max_instances: int = 1


PRODUCTION_JOBS = (
    JobDefinition("signal_outcome_refresh", "interval:5m", 180, JobPriority.TELEMETRY, False),
    JobDefinition("execution_reconcile", "interval:configured", 45, JobPriority.BINANCE_PROTECTION, True),
    JobDefinition("trade_manager", "cron:*/5+1m", 240, JobPriority.MANAGER, True),
    JobDefinition("market_intelligence_primary", "cron:10", 180, JobPriority.CRITICAL_MARKET_DATA, True),
    JobDefinition("market_fast", "cron:8,28,48", 240, JobPriority.STRATEGY_SCANNER, False),
    JobDefinition("market_mtf_1h", "cron:2,24", 240, JobPriority.STRATEGY_SCANNER, False),
    JobDefinition("market_zone", "cron:14,34,54", 240, JobPriority.STRATEGY_SCANNER, False),
    JobDefinition("market_swing", "cron:20,50", 240, JobPriority.STRATEGY_SCANNER, False),
    JobDefinition("market_wyckoff", "cron:40", 300, JobPriority.STRATEGY_SCANNER, False),
    JobDefinition("market_ltf_watch", "cron:6,16,26,36,46,56", 180, JobPriority.STRATEGY_SCANNER, False),
    JobDefinition("runtime_watchdog", "interval:15s", 5, JobPriority.BINANCE_PROTECTION, True),
    JobDefinition("keepalive", "interval:10m", 30, JobPriority.TELEMETRY, False),
    JobDefinition("alerts", "interval:5m", 60, JobPriority.TELEGRAM, False),
    JobDefinition("dashboard_telemetry", "interval:10m", 60, JobPriority.TELEMETRY, False),
    JobDefinition("state_backup", "interval:30m", 120, JobPriority.STATE_BACKUP, True),
    JobDefinition("live_learning", "interval:1h", 300, JobPriority.LEARNING, False),
)

JOB_BY_ID = {job.id: job for job in PRODUCTION_JOBS}


def validate_registry() -> None:
    if len(JOB_BY_ID) != len(PRODUCTION_JOBS):
        raise ValueError("duplicate production job id")
    forbidden = ("shadow", "research", "replay", "backtest", "counterfactual")
    for job in PRODUCTION_JOBS:
        if any(word in job.id.lower() for word in forbidden):
            raise ValueError(f"non-production job registered: {job.id}")
        if job.timeout_seconds <= 0 or job.max_instances != 1:
            raise ValueError(f"unsafe job policy: {job.id}")


validate_registry()


__all__ = ["JOB_BY_ID", "PRODUCTION_JOBS", "JobDefinition", "validate_registry"]
