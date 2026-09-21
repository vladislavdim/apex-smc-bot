"""Ordered startup readiness stages."""

from __future__ import annotations

from enum import Enum


class StartupStage(str, Enum):
    PROCESS_STARTED = "PROCESS_STARTED"
    LOAD_CONFIG = "LOAD_CONFIG"
    RESTORE_STATE_DB = "RESTORE_STATE_DB"
    STATE_DB_INTEGRITY = "STATE_DB_INTEGRITY"
    MIGRATIONS = "MIGRATIONS"
    MEMORY_DB_CHECK = "MEMORY_DB_CHECK"
    BINANCE_ACCOUNT_CHECK = "BINANCE_ACCOUNT_CHECK"
    EXECUTION_RECONCILIATION = "EXECUTION_RECONCILIATION"
    MANAGER_RECONCILIATION = "MANAGER_RECONCILIATION"
    SCHEDULER_READY = "SCHEDULER_READY"
    GATE_READY = "GATE_READY"
    MARKET_DATA_READY = "MARKET_DATA_READY"
    TELEGRAM_READY = "TELEGRAM_READY"
    READY = "READY"


STAGE_ORDER = tuple(StartupStage)


def stage_index(stage: StartupStage | str) -> int:
    value = stage if isinstance(stage, StartupStage) else StartupStage(str(stage))
    return STAGE_ORDER.index(value)


__all__ = ["STAGE_ORDER", "StartupStage", "stage_index"]
