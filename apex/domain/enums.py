"""Canonical enums used at V3 module boundaries."""

from __future__ import annotations

from enum import Enum, IntEnum


class RuntimeStatus(str, Enum):
    STARTING = "STARTING"
    RECONCILING = "RECONCILING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    NEW_ENTRIES_OFF = "NEW_ENTRIES_OFF"
    MANAGER_EXITS_ONLY = "MANAGER_EXITS_ONLY"
    FAILED = "FAILED"


class ComponentState(str, Enum):
    UNKNOWN = "UNKNOWN"
    STARTING = "STARTING"
    READY = "READY"
    FRESH = "FRESH"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"
    FAILED = "FAILED"


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    @classmethod
    def normalize(cls, value: object) -> "Direction":
        text = str(value or "").strip().upper()
        aliases = {"BULLISH": cls.LONG, "BUY": cls.LONG, "BEARISH": cls.SHORT, "SELL": cls.SHORT}
        if text in aliases:
            return aliases[text]
        return cls(text)


class Strategy(str, Enum):
    FAST = "FAST"
    MTF = "MTF"
    SWING = "SWING"
    ZONE = "ZONE"
    WYCKOFF = "WYCKOFF"


class SourceMode(str, Enum):
    PRIMARY_MARKET = "PRIMARY_MARKET"
    EXECUTION = "EXECUTION"
    LIVE_CONTEXT = "LIVE_CONTEXT"
    PROXY = "PROXY"
    INTERNAL = "INTERNAL"


class JobPriority(IntEnum):
    BINANCE_PROTECTION = 0
    MANAGER = 1
    CRITICAL_MARKET_DATA = 2
    STRATEGY_SCANNER = 3
    RISK_EXECUTION = 4
    TELEGRAM = 5
    TELEMETRY = 6
    STATE_BACKUP = 7
    LEARNING = 8
    REPORT_MAINTENANCE = 9


class IncidentSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Decision(str, Enum):
    APPROVE = "APPROVE"
    WAIT = "WAIT"
    REJECT = "REJECT"


class StrategyOutcome(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WAIT = "WAIT"
