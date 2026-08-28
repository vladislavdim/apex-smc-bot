"""Deterministic safety checks for an already calculated trade candidate.

The validator never calculates or repairs trade levels.  A strategy remains the
only authority for direction, entry, stop and targets; this module merely
rejects malformed or internally contradictory candidates before Groq,
Telegram, persistence or (in the future) exchange execution can see them.
"""

from __future__ import annotations

import math
from typing import Any


def _price(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def validate_candidate(candidate: dict[str, Any], current_price: Any = None) -> dict[str, Any]:
    """Return a non-mutating validation report for a strategy candidate."""
    errors: list[str] = []
    warnings: list[str] = []
    direction = str(candidate.get("direction", "")).upper()
    if direction not in {"BULLISH", "BEARISH"}:
        errors.append("direction must be BULLISH or BEARISH")

    symbol = str(candidate.get("symbol", "")).upper().replace("/", "")
    if not symbol or not symbol.endswith("USDT"):
        errors.append("symbol must be a USDT pair")

    entry = _price(candidate.get("entry"))
    sl = _price(candidate.get("sl"))
    tp1 = _price(candidate.get("tp1", candidate.get("tp")))
    raw_tp2 = candidate.get("tp2")
    raw_tp3 = candidate.get("tp3")
    tp2 = _price(raw_tp2) if raw_tp2 not in (None, 0, "") else tp1
    tp3 = _price(raw_tp3) if raw_tp3 not in (None, 0, "") else tp2
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
            if calculated_rr < 1.0:
                errors.append(f"TP1 risk/reward is below 1.0 ({calculated_rr:.2f})")
            supplied_rr = candidate.get("rr")
            if supplied_rr not in (None, ""):
                try:
                    supplied = float(supplied_rr)
                    if math.isfinite(supplied) and abs(supplied - calculated_rr) > 0.15:
                        warnings.append(
                            f"reported RR {supplied:.2f} differs from TP1 RR {calculated_rr:.2f}"
                        )
                except (TypeError, ValueError):
                    warnings.append("reported RR is not numeric")

        current = _price(current_price)
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
