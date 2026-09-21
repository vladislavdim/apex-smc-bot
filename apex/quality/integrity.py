"""Universal candidate integrity checks that never mutate geometry."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from apex.domain.enums import Direction
from apex.domain.models import Candidate


@dataclass(frozen=True)
class IntegrityResult:
    valid: bool
    reason_codes: tuple[str, ...]
    calculated_rr: float | None
    warning_codes: tuple[str, ...] = ()


def validate_candidate(candidate: Candidate, *, critical_data_fresh: bool = True, minimum_rr: float = 2.0) -> IntegrityResult:
    reasons: list[str] = []
    warnings: list[str] = []
    values = (candidate.entry, candidate.initial_sl, candidate.tp1, candidate.tp2, candidate.rr)
    if candidate.tp3 is not None:
        values += (candidate.tp3,)
    if not all(isfinite(float(value)) and float(value) > 0 for value in values):
        reasons.append("GEOMETRY_NON_FINITE")
        return IntegrityResult(False, tuple(reasons), None, tuple(warnings))
    if not critical_data_fresh:
        reasons.append("CRITICAL_DATA_STALE")
    targets = (candidate.tp1, candidate.tp2) + ((candidate.tp3,) if candidate.tp3 is not None else ())
    if not isinstance(candidate.direction, Direction):
        reasons.append("DIRECTION_INVALID")
    elif candidate.direction is Direction.LONG:
        if not candidate.initial_sl < candidate.entry:
            reasons.append("LONG_SL_NOT_BELOW_ENTRY")
        if not all(target > candidate.entry for target in targets):
            reasons.append("LONG_TARGET_NOT_ABOVE_ENTRY")
        if list(targets) != sorted(targets):
            reasons.append("LONG_TARGET_ORDER_INVALID")
    elif candidate.direction is Direction.SHORT:
        if not candidate.initial_sl > candidate.entry:
            reasons.append("SHORT_SL_NOT_ABOVE_ENTRY")
        if not all(target < candidate.entry for target in targets):
            reasons.append("SHORT_TARGET_NOT_BELOW_ENTRY")
        if list(targets) != sorted(targets, reverse=True):
            reasons.append("SHORT_TARGET_ORDER_INVALID")
    risk = abs(candidate.entry - candidate.initial_sl)
    # Production defines admission RR from TP1. TP2/TP3 remain optional
    # monotonic extensions and must never inflate the minimum RR check.
    calculated_rr = abs(candidate.tp1 - candidate.entry) / risk if risk else None
    if risk <= 0:
        reasons.append("ZERO_RISK_DISTANCE")
    if calculated_rr is not None and calculated_rr < minimum_rr:
        reasons.append("RR_BELOW_MIN")
    if calculated_rr is not None and abs(calculated_rr - candidate.rr) > 0.15:
        warnings.append("REPORTED_RR_DIFFERS_FROM_TP1")
    return IntegrityResult(not reasons, tuple(reasons), calculated_rr, tuple(warnings))


__all__ = ["IntegrityResult", "validate_candidate"]
