"""Market-anchored structural Entry/SL/TP calculation.

This module owns the frozen legacy calculation while the strategy detectors
are migrated out of the market facade.  It deliberately preserves the
existing geometry and rejection rules.
"""

from __future__ import annotations

import logging

from apex.market.engine_bridge import get_liquidity_heatmap
from apex.market.indicators import average_true_range
from apex.market.legacy_zones import find_fvg, find_ob
from apex.market.structure_bridge import find_swings


def smart_round(price, direction_multiplier=1.0) -> float:
    """Round a price while preserving significant digits."""
    del direction_multiplier  # Retained for the legacy call contract.
    if not price or price == 0:
        return price
    if price >= 10:
        return round(price, 3)
    if price >= 1:
        return round(price, 4)
    if price >= 0.1:
        return round(price, 5)
    if price >= 0.01:
        return round(price, 6)
    if price >= 0.001:
        return round(price, 7)
    return round(price, 10)


def select_structural_targets(
    entry: float,
    sl: float,
    candidates: list,
    direction: str,
    min_rr: float,
    max_rr: float | None,
) -> tuple[float | None, float | None]:
    """Select TP1/TP2 only from supplied market-anchored levels."""
    risk = abs(float(entry) - float(sl))
    if risk <= 0:
        return None, None

    levels = []
    for value in candidates:
        try:
            level = float(value)
        except (TypeError, ValueError):
            continue
        if level <= 0:
            continue
        if direction == "BULLISH" and level <= entry:
            continue
        if direction == "BEARISH" and level >= entry:
            continue
        if not any(
            abs(level - existing) <= max(abs(level), 1.0) * 1e-9
            for existing in levels
        ):
            levels.append(level)

    levels.sort(key=lambda level: abs(level - entry))
    tp1_index = None
    for index, level in enumerate(levels):
        rr = abs(level - entry) / risk
        if rr >= min_rr and (max_rr is None or rr <= max_rr):
            tp1_index = index
            break
    if tp1_index is None:
        return None, None

    tp1 = levels[tp1_index]
    tp2 = levels[tp1_index + 1] if tp1_index + 1 < len(levels) else None
    return smart_round(tp1), smart_round(tp2) if tp2 is not None else None


def calc_smart_levels(candles, direction, price, timeframe="1h"):
    """Build MTF levels from confirmed structure; never fabricate a level."""
    try:
        if direction not in ("BULLISH", "BEARISH"):
            raise ValueError("unknown direction")
        if not candles or len(candles) < 30 or not price or price <= 0:
            raise ValueError("insufficient confirmed candles")

        raw_highs, raw_lows = find_swings(candles, lookback=8)
        if not raw_highs or not raw_lows:
            raise ValueError("confirmed swing structure is absent")

        impulse_low = impulse_high = None
        if direction == "BULLISH":
            for high_index, high_price in reversed(raw_highs):
                preceding_lows = [item for item in raw_lows if item[0] < high_index]
                if preceding_lows and high_price > preceding_lows[-1][1]:
                    impulse_low = float(preceding_lows[-1][1])
                    impulse_high = float(high_price)
                    break
        else:
            for low_index, low_price in reversed(raw_lows):
                preceding_highs = [item for item in raw_highs if item[0] < low_index]
                if preceding_highs and preceding_highs[-1][1] > low_price:
                    impulse_high = float(preceding_highs[-1][1])
                    impulse_low = float(low_price)
                    break

        if impulse_low is None or impulse_high is None or impulse_high <= impulse_low:
            raise ValueError("confirmed directional impulse is absent")

        atr = average_true_range(candles, 14)
        if not atr or atr <= 0:
            raise ValueError("ATR is unavailable")

        impulse_range = impulse_high - impulse_low
        if direction == "BULLISH":
            ote_entry = impulse_high - impulse_range * 0.705
            ote_low = impulse_high - impulse_range * 0.79
            ote_high = impulse_high - impulse_range * 0.62
        else:
            ote_entry = impulse_low + impulse_range * 0.705
            ote_low = impulse_low + impulse_range * 0.62
            ote_high = impulse_low + impulse_range * 0.79

        def _zone_mitigated(zone):
            origin = zone.get("index")
            if not isinstance(origin, int):
                return True
            bottom = float(zone["bottom"])
            top = float(zone["top"])
            for candle in candles[origin + 2:]:
                if float(candle["low"]) <= top and float(candle["high"]) >= bottom:
                    return True
            return False

        zone_candidates = []
        for zone_kind, zone in (
            ("OB", find_ob(candles, direction)),
            ("FVG", find_fvg(candles, direction)),
        ):
            if not zone or _zone_mitigated(zone):
                continue
            overlap_low = max(float(zone["bottom"]), ote_low)
            overlap_high = min(float(zone["top"]), ote_high)
            if overlap_low > overlap_high:
                continue
            level = min(max(ote_entry, overlap_low), overlap_high)
            zone_candidates.append((abs(float(price) - level), zone_kind, zone, level))

        if not zone_candidates:
            raise ValueError("no fresh OB/FVG overlap with OTE")
        _, zone_kind, entry_zone, entry_raw = min(
            zone_candidates,
            key=lambda item: (item[0], 0 if item[1] == "OB" else 1),
        )
        entry = smart_round(entry_raw)

        heatmap = get_liquidity_heatmap(candles) or {}
        buffer = atr * ({"1h": 0.25, "4h": 0.30}.get(timeframe, 0.25))

        if direction == "BULLISH":
            stop_anchors = [impulse_low, float(entry_zone["bottom"])]
            sell_stops = heatmap.get("nearest_sell_stops")
            sell_stop_price = (
                sell_stops.get("price") if isinstance(sell_stops, dict) else sell_stops
            )
            if (
                sell_stop_price
                and sell_stop_price < entry
                and abs(float(sell_stop_price) - impulse_low) <= atr
            ):
                stop_anchors.append(float(sell_stop_price))
            sl = smart_round(min(stop_anchors) - buffer)
        else:
            stop_anchors = [impulse_high, float(entry_zone["top"])]
            buy_stops = heatmap.get("nearest_buy_stops")
            buy_stop_price = (
                buy_stops.get("price") if isinstance(buy_stops, dict) else buy_stops
            )
            if (
                buy_stop_price
                and buy_stop_price > entry
                and abs(float(buy_stop_price) - impulse_high) <= atr
            ):
                stop_anchors.append(float(buy_stop_price))
            sl = smart_round(max(stop_anchors) + buffer)

        risk = abs(entry - sl)
        if risk <= 0:
            raise ValueError("invalid structural stop")
        max_stop_atr = {"1h": 3.0, "4h": 3.5}.get(timeframe, 3.0)
        if risk < atr * 0.20 or risk > atr * max_stop_atr:
            raise ValueError("structural stop is outside the risk envelope")

        target_candidates = []
        if direction == "BULLISH":
            target_candidates.extend(float(value) for _, value in raw_highs)
            target_candidates.extend((
                impulse_low + impulse_range * 1.272,
                impulse_low + impulse_range * 1.618,
            ))
            wanted_liquidity = "buy_stops"
            opposing_direction = "BEARISH"
        else:
            target_candidates.extend(float(value) for _, value in raw_lows)
            target_candidates.extend((
                impulse_high - impulse_range * 1.272,
                impulse_high - impulse_range * 1.618,
            ))
            wanted_liquidity = "sell_stops"
            opposing_direction = "BULLISH"

        for level in heatmap.get("levels", []):
            if isinstance(level, dict) and level.get("type") == wanted_liquidity:
                target_candidates.append(level.get("price"))

        for opposing_zone in (
            find_ob(candles, opposing_direction),
            find_fvg(candles, opposing_direction),
        ):
            if opposing_zone:
                target_candidates.extend((
                    opposing_zone.get("bottom"), opposing_zone.get("top")
                ))

        tp1, tp2 = select_structural_targets(
            entry=entry,
            sl=sl,
            candidates=target_candidates,
            direction=direction,
            min_rr=2.0,
            max_rr=None,
        )
        if tp1 is None or tp2 is None:
            raise ValueError("two structural targets are unavailable")
        if abs(tp2 - entry) / risk > 6.0:
            raise ValueError("second structural target is too distant")

        tp3 = tp2
        rr = round(abs(tp1 - entry) / risk, 2)
        return {
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl_pct": round(risk / entry * 100, 2),
            "tp1_pct": round(abs(tp1 - entry) / entry * 100, 2),
            "tp2_pct": round(abs(tp2 - entry) / entry * 100, 2),
            "tp3_pct": round(abs(tp3 - entry) / entry * 100, 2),
            "rr": rr,
            "mitigated": False,
            "entry_zone": zone_kind,
            "source": f"structure+fib_ote+{zone_kind.lower()}",
        }
    except Exception as error:
        logging.debug(
            "calc_smart_levels rejected (%s %s): %s",
            direction, timeframe, error,
        )
        return None


__all__ = ["calc_smart_levels", "select_structural_targets", "smart_round"]
