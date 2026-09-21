"""Bounded candle-derived volume profile for V3 context."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


def volume_profile(candles: Iterable[Mapping[str, Any]], *, bins: int = 20, value_area: float = 0.70) -> dict[str, Any]:
    rows = [row for row in candles if row.get("is_closed") is True]
    if not rows or bins < 2:
        return {"available": False, "source": "gate", "mode": "LIVE_CONTEXT"}
    low = min(float(row["low"]) for row in rows)
    high = max(float(row["high"]) for row in rows)
    if high <= low:
        return {"available": False, "source": "gate", "mode": "LIVE_CONTEXT"}
    width = (high - low) / bins
    buckets = [0.0] * bins
    for row in rows:
        price = (float(row["high"]) + float(row["low"]) + float(row["close"])) / 3
        index = min(bins - 1, max(0, int((price - low) / width)))
        buckets[index] += max(0.0, float(row.get("volume") or 0.0))
    poc_index = max(range(bins), key=buckets.__getitem__)
    chosen = {poc_index}
    target = sum(buckets) * min(1.0, max(0.0, value_area))
    accumulated = buckets[poc_index]
    left, right = poc_index - 1, poc_index + 1
    while accumulated < target and (left >= 0 or right < bins):
        left_value = buckets[left] if left >= 0 else -1
        right_value = buckets[right] if right < bins else -1
        index = left if left_value >= right_value else right
        chosen.add(index)
        accumulated += buckets[index]
        if index == left:
            left -= 1
        else:
            right += 1
    center = lambda index: low + (index + 0.5) * width
    return {
        "available": True, "source": "gate", "mode": "LIVE_CONTEXT",
        "poc": center(poc_index), "val": center(min(chosen)), "vah": center(max(chosen)),
        "bins": bins, "samples": len(rows), "value_area": value_area,
    }


__all__ = ["volume_profile"]
