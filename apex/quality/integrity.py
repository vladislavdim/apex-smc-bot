"""Universal candidate integrity checks that never mutate geometry."""

from __future__ import annotations

from dataclasses import dataclass
import math
from math import isfinite
from typing import Any

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


def _legacy_price(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def _legacy_strategy_name(candidate: dict[str, Any]) -> str:
    raw = candidate.get("scan_type") or candidate.get("grade") or candidate.get("signal_type") or "MTF"
    strategy = str(raw).upper()
    return "FAST" if strategy == "FAST_DEAL" else strategy


def validate_legacy_candidate(candidate: dict[str, Any], current_price: Any = None) -> dict[str, Any]:
    """Validate the compatibility candidate without changing its geometry."""
    errors: list[str] = []
    warnings: list[str] = []
    direction = str(candidate.get("direction", "")).upper()
    if direction not in {"BULLISH", "BEARISH"}:
        errors.append("direction must be BULLISH or BEARISH")
    symbol = str(candidate.get("symbol", "")).upper().replace("/", "")
    if not symbol or not symbol.endswith("USDT"):
        errors.append("symbol must be a USDT pair")
    entry = _legacy_price(candidate.get("entry"))
    sl = _legacy_price(candidate.get("sl"))
    tp1 = _legacy_price(candidate.get("tp1", candidate.get("tp")))
    raw_tp2 = candidate.get("tp2")
    raw_tp3 = candidate.get("tp3")
    tp2 = _legacy_price(raw_tp2) if raw_tp2 not in (None, 0, "") else tp1
    tp3 = _legacy_price(raw_tp3) if raw_tp3 not in (None, 0, "") else tp2
    for name, value in (("entry", entry), ("sl", sl), ("tp1", tp1), ("tp2", tp2), ("tp3", tp3)):
        if value is None:
            errors.append(f"{name} must be a positive finite price")
    calculated_rr = None
    if not errors and entry is not None and sl is not None and tp1 is not None and tp2 is not None and tp3 is not None:
        if direction == "BULLISH":
            if not sl < entry < tp1:
                errors.append("BULLISH levels must satisfy SL < entry < TP1")
            if not tp1 <= tp2 <= tp3:
                errors.append("BULLISH targets must be monotonic TP1 <= TP2 <= TP3")
        elif direction == "BEARISH":
            if not tp1 < entry < sl:
                errors.append("BEARISH levels must satisfy TP1 < entry < SL")
            if not tp1 >= tp2 >= tp3:
                errors.append("BEARISH targets must be monotonic TP1 >= TP2 >= TP3")
        risk = abs(entry - sl)
        if risk <= 0:
            errors.append("stop distance must be greater than zero")
        else:
            calculated_rr = abs(tp1 - entry) / risk
            strategy = _legacy_strategy_name(candidate)
            minimum_rr = 2.0
            if calculated_rr < minimum_rr:
                errors.append(f"TP1 risk/reward is below {minimum_rr:.1f} for {strategy} ({calculated_rr:.2f})")
            supplied_rr = candidate.get("rr")
            if supplied_rr not in (None, ""):
                try:
                    supplied = float(supplied_rr)
                    if math.isfinite(supplied) and abs(supplied - calculated_rr) > 0.15:
                        warnings.append(f"reported RR {supplied:.2f} differs from TP1 RR {calculated_rr:.2f}")
                except (TypeError, ValueError):
                    warnings.append("reported RR is not numeric")
        current = _legacy_price(current_price)
        if current is not None:
            if direction == "BULLISH" and current <= sl:
                errors.append("current price is already at or below the stop")
            elif direction == "BEARISH" and current >= sl:
                errors.append("current price is already at or above the stop")
            if direction == "BULLISH" and current >= tp1:
                errors.append("current price has already reached TP1")
            elif direction == "BEARISH" and current <= tp1:
                errors.append("current price has already reached TP1")
    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "calculated_rr": round(calculated_rr, 4) if calculated_rr is not None else None,
    }


__all__ = ["IntegrityResult", "validate_candidate", "validate_legacy_candidate"]
