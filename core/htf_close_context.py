"""Closed weekly/monthly candle context for higher-timeframe Groq review.

This module never decides whether a trade is valid and never changes levels.
It only describes already-closed HTF candles for selected slower strategies.
"""
from __future__ import annotations

from typing import Any, Callable

_SUPPORTED = ("MTF", "SWING", "WYCKOFF")


def strategy_uses_htf_close_context(strategy: str) -> bool:
    name = str(strategy or "").upper()
    return any(token in name for token in _SUPPORTED)


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bar_summary(bar: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    o, h, l, c = (_number(bar.get(k)) for k in ("open", "high", "low", "close"))
    ph, pl, pc = (_number(previous.get(k)) for k in ("high", "low", "close"))
    if None in (o, h, l, c):
        return {"available": False}
    span = h - l
    close_position = round((c - l) / span * 100.0, 1) if span > 0 else 50.0
    if ph is not None and c > ph:
        relation = "ABOVE_PREVIOUS_HIGH"
    elif pl is not None and c < pl:
        relation = "BELOW_PREVIOUS_LOW"
    else:
        relation = "INSIDE_PREVIOUS_RANGE"
    if c > o:
        direction = "BULLISH"
    elif c < o:
        direction = "BEARISH"
    else:
        direction = "DOJI"
    return {
        "available": True,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "previous_high": ph,
        "previous_low": pl,
        "previous_close": pc,
        "direction": direction,
        "close_position_pct": close_position,
        "close_vs_previous_range": relation,
    }


def build_htf_close_context(
    symbol: str,
    strategy: str,
    candle_loader: Callable[[str, str, int], list] | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "used": False,
        "strategy": str(strategy or ""),
        "weekly": {"available": False},
        "monthly": {"available": False},
    }
    if not candle_loader or not strategy_uses_htf_close_context(strategy):
        return result
    result["used"] = True
    for tf, key in (("1w", "weekly"), ("1M", "monthly")):
        try:
            rows = candle_loader(str(symbol), tf, 5) or []
            clean = [row for row in rows if isinstance(row, dict)]
            # Exchange candle endpoints normally include the currently-forming
            # candle as the final row. Only immutable closed candles belong here.
            closed = clean[:-1] if len(clean) >= 3 else []
            if len(closed) >= 2:
                result[key] = _bar_summary(closed[-1], closed[-2])
        except Exception as exc:
            result[key] = {"available": False, "error": type(exc).__name__}
    return result


def format_htf_close_context(context: dict[str, Any]) -> str:
    if not context.get("used"):
        return ""
    lines = [
        "HIGHER-TIMEFRAME CLOSED CANDLE CONTEXT:",
        "Context only. Never reject/approve solely because of this block and never alter entry/SL/TP/RR.",
    ]
    for key, label in (("weekly", "WEEKLY"), ("monthly", "MONTHLY")):
        row = context.get(key) or {}
        if not row.get("available"):
            lines.append(f"- {label}: unavailable")
            continue
        lines.append(
            f"- {label}: direction={row.get('direction')}; close={row.get('close')}; "
            f"previous_high={row.get('previous_high')}; previous_low={row.get('previous_low')}; "
            f"close_position={row.get('close_position_pct')}% of closed candle range; "
            f"relation={row.get('close_vs_previous_range')}"
        )
    return "\n".join(lines)
