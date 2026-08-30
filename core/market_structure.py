"""Pure, symmetric market-structure analysis for confirmed OHLCV candles.

The module deliberately knows nothing about entries, stops, targets, Telegram,
or Groq.  It only classifies confirmed swing points and reports a BOS/CHoCH
when a candle *closes* through the latest confirmed structural pivot.
"""

from __future__ import annotations

from typing import Any, Iterable


Direction = str
Swing = dict[str, Any]
StructureEvent = dict[str, Any]


def find_swings(candles: list[dict], lookback: int = 5) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """Return confirmed centred swing highs and lows.

    A pivot needs ``lookback`` candles on both sides.  Consequently, the
    currently forming edge of a series can never become a swing prematurely.
    Callers are still responsible for removing the exchange's mutable candle.
    """
    if lookback < 1 or len(candles) < lookback * 2 + 1:
        return [], []

    highs: list[tuple[int, float]] = []
    lows: list[tuple[int, float]] = []
    for idx in range(lookback, len(candles) - lookback):
        window = candles[idx - lookback:idx + lookback + 1]
        high = float(candles[idx]["high"])
        low = float(candles[idx]["low"])
        if high == max(float(c["high"]) for c in window):
            highs.append((idx, high))
        if low == min(float(c["low"]) for c in window):
            lows.append((idx, low))
    return highs, lows


def classify_swings(
    highs: Iterable[tuple[int, float]],
    lows: Iterable[tuple[int, float]],
    *,
    confirmation_bars: int = 0,
) -> list[Swing]:
    """Classify pivots without giving the first observations a directional vote.

    The first high is ``H`` and the first low is ``L``.  Equal highs/lows are
    explicitly neutral (``EH``/``EL``), avoiding the former implicit bullish
    ``HH`` + ``HL`` seed.
    """
    high_points = list(highs)
    low_points = list(lows)
    result: list[Swing] = []

    for pos, (idx, raw_price) in enumerate(high_points):
        price = float(raw_price)
        if pos == 0:
            kind = "H"
        else:
            previous = float(high_points[pos - 1][1])
            tolerance = max(abs(previous), abs(price), 1.0) * 1e-10
            kind = "HH" if price > previous + tolerance else "LH" if price < previous - tolerance else "EH"
        result.append({
            "idx": int(idx),
            "price": price,
            "kind": kind,
            "side": "HIGH",
            "confirmed_idx": int(idx) + max(0, int(confirmation_bars)),
        })

    for pos, (idx, raw_price) in enumerate(low_points):
        price = float(raw_price)
        if pos == 0:
            kind = "L"
        else:
            previous = float(low_points[pos - 1][1])
            tolerance = max(abs(previous), abs(price), 1.0) * 1e-10
            kind = "HL" if price > previous + tolerance else "LL" if price < previous - tolerance else "EL"
        result.append({
            "idx": int(idx),
            "price": price,
            "kind": kind,
            "side": "LOW",
            "confirmed_idx": int(idx) + max(0, int(confirmation_bars)),
        })

    return sorted(result, key=lambda swing: (swing["idx"], swing["side"]))


def infer_structure_direction(classified: list[Swing], *, at_index: int | None = None) -> Direction | None:
    """Return the latest established HH+HL or LH+LL structure.

    A mixed pullback does not invent a new direction: after a trend has been
    established it remains active until an opposite coherent structure is
    confirmed.  If no coherent pair has ever existed, mixed/equal/incomplete
    structures remain unresolved.
    """
    known = [
        swing for swing in classified
        if at_index is None or int(swing.get("confirmed_idx", swing["idx"])) <= at_index
    ]
    latest_high: str | None = None
    latest_low: str | None = None
    direction: Direction | None = None
    for swing in known:
        if swing.get("side") == "HIGH":
            latest_high = swing.get("kind")
        elif swing.get("side") == "LOW":
            latest_low = swing.get("kind")

        if latest_high == "HH" and latest_low == "HL":
            direction = "BULLISH"
        elif latest_high == "LH" and latest_low == "LL":
            direction = "BEARISH"
    return direction


def _candle_time(candle: dict, fallback_index: int) -> Any:
    for key in ("timestamp", "time", "open_time", "openTime", "ts"):
        if candle.get(key) is not None:
            return candle[key]
    return fallback_index


def detect_latest_structure_event(
    candles: list[dict],
    classified: list[Swing],
    *,
    max_break_age: int = 1,
) -> StructureEvent | None:
    """Return the latest close-confirmed BOS/CHoCH, never a wick-only break.

    ``max_break_age=1`` means the latest closed candle itself must cross the
    pivot.  Larger values are useful for a sweep setup that allows a few bars
    for displacement and confirmation.
    """
    if len(candles) < 2 or max_break_age < 1:
        return None

    first_idx = max(1, len(candles) - int(max_break_age))
    events: list[StructureEvent] = []
    for idx in range(first_idx, len(candles)):
        confirmed = [
            swing for swing in classified
            if int(swing.get("confirmed_idx", swing["idx"])) <= idx
            and int(swing["idx"]) < idx
        ]
        highs = [s for s in confirmed if s.get("side") == "HIGH"]
        lows = [s for s in confirmed if s.get("side") == "LOW"]
        if len(highs) < 2 or len(lows) < 2:
            continue

        prior_direction = infer_structure_direction(confirmed, at_index=idx)
        if prior_direction is None:
            continue

        previous_close = float(candles[idx - 1]["close"])
        close = float(candles[idx]["close"])
        last_high = highs[-1]
        last_low = lows[-1]
        high_level = float(last_high["price"])
        low_level = float(last_low["price"])
        high_tolerance = max(abs(high_level), 1.0) * 1e-10
        low_tolerance = max(abs(low_level), 1.0) * 1e-10

        direction: Direction | None = None
        level: float | None = None
        pivot: Swing | None = None
        if previous_close <= high_level + high_tolerance and close > high_level + high_tolerance:
            direction, level, pivot = "BULLISH", high_level, last_high
        elif previous_close >= low_level - low_tolerance and close < low_level - low_tolerance:
            direction, level, pivot = "BEARISH", low_level, last_low

        if direction is None or level is None or pivot is None:
            continue

        event_type = "BOS" if direction == prior_direction else "CHoCH"
        events.append({
            "type": event_type,
            "direction": direction,
            "level": level,
            "candle_index": idx,
            "candle_time": _candle_time(candles[idx], idx),
            "pivot_index": int(pivot["idx"]),
            "prior_direction": prior_direction,
            "closed": True,
        })

    return events[-1] if events else None


def analyze_market_structure(
    candles: list[dict],
    *,
    swing_lookback: int = 5,
    max_break_age: int = 1,
) -> dict[str, Any]:
    """Return symmetric trend state plus an optional confirmed BOS/CHoCH."""
    highs, lows = find_swings(candles, lookback=swing_lookback)
    classified = classify_swings(highs, lows, confirmation_bars=swing_lookback)
    trend_direction = infer_structure_direction(classified, at_index=len(candles) - 1)
    event = detect_latest_structure_event(
        candles,
        classified,
        max_break_age=max_break_age,
    )
    # A close-confirmed CHoCH is the first structural shift, so downstream
    # direction follows the event while the previous paired trend remains
    # available explicitly for auditing.
    direction = event["direction"] if event else trend_direction
    return {
        "direction": direction,
        "trend_direction": trend_direction,
        "event": event,
        "highs": highs,
        "lows": lows,
        "classified": classified,
    }


def events_with_trend_fallback(
    candles: list[dict],
    classified: list[Swing],
    *,
    max_break_age: int = 1,
) -> list[StructureEvent]:
    """Compatibility result: real event first, otherwise a symmetric TREND."""
    event = detect_latest_structure_event(candles, classified, max_break_age=max_break_age)
    if event:
        return [event]
    direction = infer_structure_direction(classified, at_index=len(candles) - 1 if candles else None)
    if direction is None:
        return []
    return [{
        "type": "TREND",
        "direction": direction,
        "level": None,
        "candle_index": len(candles) - 1,
        "candle_time": _candle_time(candles[-1], len(candles) - 1) if candles else None,
        "prior_direction": direction,
        "closed": True,
    }]
