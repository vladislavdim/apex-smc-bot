"""Stable machine-readable business and safety reason codes."""

from __future__ import annotations

import re

FAST_NO_RETEST = "FAST_NO_RETEST"
FAST_VOLUME_LOW = "FAST_VOLUME_LOW"
MTF_HTF_CONFLICT = "MTF_HTF_CONFLICT"
ZONE_NO_REACTION = "ZONE_NO_REACTION"
WY_PHASE_INCOMPLETE = "WY_PHASE_INCOMPLETE"
RR_BELOW_MIN = "RR_BELOW_MIN"
GATE_STALE_15M = "GATE_STALE_15M"
GROQ_WAIT = "GROQ_WAIT"
RISK_CLUSTER_LIMIT = "RISK_CLUSTER_LIMIT"
BINANCE_REJECTED = "BINANCE_REJECTED"
RUNTIME_NOT_READY = "RUNTIME_NOT_READY"
WORKER_RESTART_LOOP = "WORKER_RESTART_LOOP"
MEMORY_PRESSURE = "MEMORY_PRESSURE"

ALL_REASON_CODES = frozenset(
    value for name, value in globals().items()
    if name.isupper() and isinstance(value, str)
)

_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_]*(?::[A-Za-z0-9_.-]+)?$")


def validate_reason_code(value: object) -> str:
    code = str(value or "")
    if not _REASON_CODE.fullmatch(code):
        raise ValueError(f"invalid_reason_code:{code[:64]}")
    return code


def validate_reason_codes(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(validate_reason_code(value) for value in values)

__all__ = sorted(ALL_REASON_CODES) + [
    "ALL_REASON_CODES", "validate_reason_code", "validate_reason_codes",
]
