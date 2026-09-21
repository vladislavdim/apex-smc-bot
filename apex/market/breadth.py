"""Universe breadth from aligned, closed Gate candles."""

from __future__ import annotations

from typing import Any, Mapping


def market_breadth(latest_by_symbol: Mapping[str, tuple[Mapping[str, Any], ...]]) -> dict[str, Any]:
    bullish = bearish = unchanged = 0
    above_ema = 0
    accepted = 0
    closed_by_symbol = {
        symbol: [
            row for row in rows
            if row.get("is_closed") is True and row.get("close_time") is not None
        ]
        for symbol, rows in latest_by_symbol.items()
    }
    latest_close = max(
        (float(rows[-1]["close_time"]) for rows in closed_by_symbol.values() if rows),
        default=None,
    )
    excluded_stale = 0
    for closed in closed_by_symbol.values():
        if len(closed) < 2:
            continue
        if latest_close is None or float(closed[-1]["close_time"]) != latest_close:
            excluded_stale += 1
            continue
        previous, current = float(closed[-2]["close"]), float(closed[-1]["close"])
        accepted += 1
        bullish += int(current > previous)
        bearish += int(current < previous)
        unchanged += int(current == previous)
        sample = closed[-20:]
        ema = float(sample[0]["close"])
        alpha = 2 / (len(sample) + 1)
        for row in sample[1:]:
            ema = alpha * float(row["close"]) + (1 - alpha) * ema
        above_ema += int(current > ema)
    if not accepted:
        return {"available": False, "source": "gate", "mode": "LIVE_CONTEXT", "symbols": 0, "excluded_stale": excluded_stale}
    return {
        "available": True, "source": "gate", "mode": "LIVE_CONTEXT", "symbols": accepted,
        "bullish_pct": bullish / accepted, "bearish_pct": bearish / accepted,
        "unchanged_pct": unchanged / accepted, "above_ema_pct": above_ema / accepted,
        "breadth_impulse": (bullish - bearish) / accepted,
        "as_of_close_time": latest_close, "excluded_stale": excluded_stale,
    }


__all__ = ["market_breadth"]
