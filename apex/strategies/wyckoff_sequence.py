"""Closed-candle chronology proof for WYCKOFF Spring/Distribution candidates.

Reaccumulation remains outside this policy until that subtype has an
independent chronology oracle.
"""

from __future__ import annotations

from typing import Any, Mapping

from apex.domain.models import MarketSnapshot
from apex.market.candles import candle_close_time, candle_open_time
from apex.market.runtime_cache import get_confirmed_candles


def _phase_map(
    kind: str, daily: list[Mapping[str, Any]], bars: list[Mapping[str, Any]]
):
    # Lazy import avoids coupling module initialization to native detector setup.
    from apex.strategies.native import SnapshotContext

    context = SnapshotContext.__new__(SnapshotContext)
    if kind == "accumulation":
        return context._find_wyckoff_phases_accumulation(daily, bars)
    return context._find_wyckoff_phases_distribution(daily, bars)


def strict_sequence_policy(snapshot: MarketSnapshot, subtype: str) -> dict[str, Any]:
    """Return a fail-closed chronology proof for supported candidate subtypes."""
    key = str(subtype or "").upper()
    if key not in {"SPRING", "DISTRIBUTION"}:
        return {"applies": False, "sequence_complete": True, "status": "NOT_APPLICABLE"}

    kind = "accumulation" if key == "SPRING" else "distribution"
    # Match SnapshotContext.detect_wyckoff_spring/distribution exactly.
    # Older daily bars must not select a different SC/BC/AR anchor.
    daily = get_confirmed_candles(list(snapshot.candles.get("1d") or ())[-61:])
    bars = get_confirmed_candles(list(snapshot.candles.get("4h") or ())[-121:])
    as_of = snapshot.as_of.timestamp()
    climax, trigger, confirmation = (
        ("SC", "Spring", "SOS") if kind == "accumulation" else ("BC", "UTAD", "SOW")
    )

    def reject(status: str) -> dict[str, Any]:
        return {
            "applies": True,
            "sequence_complete": False,
            "status": status,
            "evidence": (),
        }

    for rows, timeframe in ((daily, "1d"), (bars, "4h")):
        opens = [candle_open_time(row) for row in rows]
        closes = [candle_close_time(row, timeframe) for row in rows]
        if (
            any(not isinstance(row, Mapping) for row in rows)
            or any(value is None for value in (*opens, *closes))
            or any(left >= right for left, right in zip(opens, opens[1:]))
            or any(value > as_of for value in closes)
        ):
            return reject("TIMING_UNKNOWN")

    # Use exactly the rows seen by the native detector, and validate their
    # timing before private phase helpers can inspect malformed input.
    phases = _phase_map(kind, daily, bars)
    if climax not in phases or "AR" not in phases:
        return reject("INCOMPLETE")

    anchors = [climax, "AR"] + (["ST"] if "ST" in phases else [])
    anchor_times = [
        candle_close_time(daily[phases[name]["idx"]], "1d") for name in anchors
    ]
    if any(left >= right for left, right in zip(anchor_times, anchor_times[1:])):
        return reject("ORDER_CONFLICT")
    if any(value > as_of for value in anchor_times):
        return reject("TIMING_UNKNOWN")

    level = phases.get("ST", phases[climax])["price"]
    target = phases["AR"]["price"]
    average = sum(row["volume"] for row in bars[-20:-10]) / 10 if len(bars) >= 20 else 1

    def closed(row: Mapping[str, Any]) -> bool:
        return (
            row.get("is_closed") is not False and candle_close_time(row, "4h") <= as_of
        )

    triggers = []
    for index, row in enumerate(bars[-30:], start=max(0, len(bars) - 30)):
        if not closed(row) or candle_open_time(row) < anchor_times[-1]:
            continue
        crossed = (
            row["low"] < level < row["close"]
            if kind == "accumulation"
            else row["high"] > level > row["close"]
        )
        wick = (
            row["close"] - row["low"]
            if kind == "accumulation"
            else row["high"] - row["close"]
        ) / (row["high"] - row["low"] + 1e-6)
        if crossed and wick > 0.4:
            triggers.append((index, row))

    confirmations = []
    for index, row in enumerate(bars[-10:], start=max(0, len(bars) - 10)):
        beyond = (
            row["close"] > target if kind == "accumulation" else row["close"] < target
        )
        if closed(row) and beyond and row["volume"] > average * 1.5:
            confirmations.append((index, row))

    for first_index, first in triggers:
        for last_index, last in confirmations:
            if last_index > first_index and candle_open_time(last) >= candle_close_time(
                first, "4h"
            ):
                evidence = [
                    {"phase": name, "timeframe": "1d", "close_time": value}
                    for name, value in zip(anchors, anchor_times)
                ]
                evidence.extend(
                    (
                        {
                            "phase": trigger,
                            "timeframe": "4h",
                            "close_time": candle_close_time(first, "4h"),
                            "index": first_index,
                        },
                        {
                            "phase": confirmation,
                            "timeframe": "4h",
                            "close_time": candle_close_time(last, "4h"),
                            "index": last_index,
                        },
                    )
                )
                return {
                    "applies": True,
                    "sequence_complete": True,
                    "status": "ORDERED",
                    "evidence": tuple(evidence),
                }
    return reject("INCOMPLETE_OR_ORDER_CONFLICT")


__all__ = ["strict_sequence_policy"]
