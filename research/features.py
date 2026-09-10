"""Point-in-time feature snapshots built only from closed Gate candles.

Structure reuses ``core.market_structure`` — the same deterministic structure
implementation imported by the live scanner. OB/FVG/CVD/VWAP/Profile/Fibonacci
reuse the pure helpers from ``core.smc_engine`` where available.
"""
from __future__ import annotations

import math
import statistics
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Iterable, Mapping

from core.market_structure import analyze_market_structure
from .store import stable_id


FEATURE_VERSION = "research-features-v3"
TIMEFRAME_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def percentile_rank(values: Iterable[float], value: float) -> float | None:
    clean = sorted(_number(v) for v in values if math.isfinite(_number(v)))
    if not clean:
        return None
    below = sum(v < value for v in clean)
    equal = sum(v == value for v in clean)
    return round((below + 0.5 * equal) / len(clean) * 100, 3)


def zscore(values: list[float], value: float) -> float | None:
    if len(values) < 2:
        return None
    mean = statistics.fmean(values)
    std = statistics.pstdev(values)
    return round((value - mean) / std, 4) if std > 0 else 0.0


def ema(values: list[float], period: int) -> float | None:
    if len(values) < period or period <= 0:
        return None
    result = statistics.fmean(values[:period]); factor = 2.0 / (period + 1)
    for value in values[period:]:
        result = value * factor + result * (1 - factor)
    return result


def atr_series(candles: list[Mapping[str, Any]], period: int = 14) -> list[float]:
    if len(candles) < 2:
        return []
    true_ranges = []
    for previous, candle in zip(candles, candles[1:]):
        high, low, prev_close = _number(candle.get("high")), _number(candle.get("low")), _number(previous.get("close"))
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if len(true_ranges) < period:
        return []
    return [statistics.fmean(true_ranges[i - period:i]) for i in range(period, len(true_ranges) + 1)]


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    changes = [b - a for a, b in zip(values[-period - 1:-1], values[-period:])]
    gains = statistics.fmean(max(0.0, change) for change in changes)
    losses = statistics.fmean(max(0.0, -change) for change in changes)
    if losses == 0:
        return 100.0
    return round(100 - 100 / (1 + gains / losses), 4)


def _macd(values: list[float]) -> dict[str, float | None]:
    line = None
    fast, slow = ema(values, 12), ema(values, 26)
    if fast is not None and slow is not None:
        line = fast - slow
    history = []
    if len(values) >= 35:
        for end in range(26, len(values) + 1):
            a, b = ema(values[:end], 12), ema(values[:end], 26)
            if a is not None and b is not None:
                history.append(a - b)
    signal = ema(history, 9) if len(history) >= 9 else None
    histogram = line - signal if line is not None and signal is not None else None
    previous_histogram = None
    if len(history) >= 10:
        previous_signal = ema(history[:-1], 9)
        previous_histogram = history[-2] - previous_signal if previous_signal is not None else None
    return {"line": line, "signal": signal, "histogram": histogram,
            "histogram_slope": histogram - previous_histogram if histogram is not None and previous_histogram is not None else None,
            "zero_cross": bool(len(history) > 1 and history[-2] * history[-1] <= 0) if history else False}


def validate_candles(candles: list[Mapping[str, Any]], timeframe: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    expected = TIMEFRAME_SECONDS.get(timeframe)
    seen: set[int] = set()
    previous: int | None = None
    for index, candle in enumerate(candles):
        ts = int(candle.get("open_time", candle.get("timestamp", 0)) or 0)
        values = {name: _number(candle.get(name)) for name in ("open", "high", "low", "close", "volume")}
        if ts in seen:
            issues.append({"type": "DUPLICATE", "open_time": ts, "severity": "ERROR"})
        seen.add(ts)
        if previous is not None:
            if ts <= previous:
                issues.append({"type": "TIMESTAMP_ORDER", "open_time": ts, "severity": "ERROR"})
            elif expected and ts - previous != expected:
                issues.append({"type": "MISSING_CANDLES", "open_time": ts, "severity": "WARNING",
                               "missing": max(0, (ts - previous) // expected - 1)})
        if min(values["open"], values["high"], values["low"], values["close"]) <= 0:
            issues.append({"type": "ZERO_OR_NEGATIVE_PRICE", "open_time": ts, "severity": "ERROR"})
        if values["low"] > values["high"] or values["high"] < max(values["open"], values["close"]) or values["low"] > min(values["open"], values["close"]):
            issues.append({"type": "OHLC_INCONSISTENT", "open_time": ts, "severity": "ERROR"})
        if values["volume"] < 0:
            issues.append({"type": "NEGATIVE_VOLUME", "open_time": ts, "severity": "ERROR"})
        if not bool(candle.get("is_closed", True)):
            issues.append({"type": "OPEN_CANDLE", "open_time": ts, "severity": "ERROR"})
        previous = ts
    return issues


def _volume_profile(candles: list[Mapping[str, Any]], bins: int = 24) -> dict[str, Any]:
    if not candles:
        return {}
    low = min(_number(c.get("low")) for c in candles); high = max(_number(c.get("high")) for c in candles)
    if high <= low:
        return {}
    size = (high - low) / bins; buckets = [0.0] * bins
    for candle in candles:
        typical = (_number(candle.get("high")) + _number(candle.get("low")) + _number(candle.get("close"))) / 3
        index = min(bins - 1, max(0, int((typical - low) / size)))
        buckets[index] += max(0.0, _number(candle.get("volume")))
    poc_idx = max(range(bins), key=buckets.__getitem__); total = sum(buckets)
    ranked = sorted(range(bins), key=lambda i: buckets[i], reverse=True)
    accepted: set[int] = set(); accumulated = 0.0
    for index in ranked:
        accepted.add(index); accumulated += buckets[index]
        if total <= 0 or accumulated / total >= 0.70:
            break
    vah_idx, val_idx = max(accepted), min(accepted)
    center = lambda i: low + (i + 0.5) * size
    return {"poc": center(poc_idx), "vah": center(vah_idx), "val": center(val_idx),
            "hvn": [center(i) for i in ranked[:3]], "lvn": [center(i) for i in ranked[-3:]],
            "bins": bins}


def _ob_fvg(candles: list[dict[str, Any]], direction: str | None) -> tuple[Any, Any, Any]:
    if not direction:
        return None, None, None
    try:
        from core.smc_engine import find_ob, find_fvg, detect_breaker_block
        return find_ob(candles, direction), find_fvg(candles, direction), detect_breaker_block(candles, direction)
    except Exception:
        return None, None, None


def _cvd_proxy(candles: list[Mapping[str, Any]]) -> dict[str, Any]:
    deltas=[]
    for candle in candles:
        high, low = _number(candle.get("high")), _number(candle.get("low"))
        close, open_ = _number(candle.get("close")), _number(candle.get("open"))
        volume = max(0.0, _number(candle.get("volume")))
        location = ((close - low) / (high - low) - 0.5) * 2 if high > low else (1 if close > open_ else -1 if close < open_ else 0)
        deltas.append(volume * location)
    value = sum(deltas)
    return {"kind": "CVD_PROXY", "value": value, "delta": deltas[-1] if deltas else 0,
            "slope": sum(deltas[-5:]) if deltas else 0,
            "percentile": percentile_rank([abs(x) for x in deltas], abs(deltas[-1])) if deltas else None,
            "approximation": True}


def _session(open_time: int) -> dict[str, Any]:
    dt = datetime.fromtimestamp(open_time, tz=timezone.utc)
    warsaw = dt.astimezone(ZoneInfo("Europe/Warsaw"))
    hour = dt.hour
    if 0 <= hour < 7:
        name = "ASIA"
    elif 7 <= hour < 12:
        name = "LONDON"
    elif 12 <= hour < 16:
        name = "OVERLAP"
    elif 16 <= hour < 22:
        name = "NEW_YORK"
    else:
        name = "OFF_HOURS"
    return {"name": name, "hour_utc": hour, "hour_warsaw": warsaw.hour,
            "weekday": dt.weekday(), "weekend": dt.weekday() >= 5}


def _important_period_levels(candles: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Compute only levels fully known at the current candle's open time."""
    if not candles:
        return {}
    current_ts = int(candles[-1].get("open_time", candles[-1].get("timestamp", 0)) or 0)
    current = datetime.fromtimestamp(current_ts, tz=timezone.utc)
    day_groups: dict[Any, list[Mapping[str, Any]]] = {}
    week_groups: dict[Any, list[Mapping[str, Any]]] = {}
    for candle in candles:
        ts = int(candle.get("open_time", candle.get("timestamp", 0)) or 0)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        day_groups.setdefault(dt.date(), []).append(candle)
        iso = dt.isocalendar()
        week_groups.setdefault((iso.year, iso.week), []).append(candle)
    previous_days = [key for key in day_groups if key < current.date()]
    previous_weeks = [key for key in week_groups if key < (current.isocalendar().year, current.isocalendar().week)]
    result: dict[str, Any] = {}
    if previous_days:
        key = max(previous_days); rows = day_groups[key]
        result.update({"previous_day_high": max(_number(x.get("high")) for x in rows),
                       "previous_day_low": min(_number(x.get("low")) for x in rows),
                       "previous_day": str(key)})
    if previous_weeks:
        key = max(previous_weeks); rows = week_groups[key]
        result.update({"previous_week_high": max(_number(x.get("high")) for x in rows),
                       "previous_week_low": min(_number(x.get("low")) for x in rows),
                       "previous_week": f"{key[0]}-W{key[1]:02d}"})
    return result


def _regime(closes: list[float], atr_pct: float | None, range_percentile: float | None) -> dict[str, str]:
    ema20, ema50 = ema(closes, 20), ema(closes, 50)
    slope = closes[-1] - closes[-6] if len(closes) >= 6 else 0
    if ema20 is None or ema50 is None:
        primary = "UNKNOWN"
    elif ema20 > ema50 and slope > 0:
        primary = "TREND_UP"
    elif ema20 < ema50 and slope < 0:
        primary = "TREND_DOWN"
    else:
        primary = "RANGE"
    volatility = "HIGH_VOL" if atr_pct is not None and atr_pct >= 70 else "LOW_VOL" if atr_pct is not None and atr_pct <= 30 else "NORMAL_VOL"
    phase = "EXPANSION" if range_percentile is not None and range_percentile >= 75 else "COMPRESSION" if range_percentile is not None and range_percentile <= 25 else "TRANSITION"
    return {"primary": primary, "volatility": volatility, "phase": phase}


def compute_feature_snapshot(symbol: str, timeframe: str, candles: list[Mapping[str, Any]], *,
                             dataset_version: str = "gate-v1",
                             benchmark: Mapping[str, list[Mapping[str, Any]]] | None = None,
                             external: Mapping[str, Any] | None = None) -> dict[str, Any]:
    closed = [dict(c) for c in candles if bool(c.get("is_closed", True))]
    closed.sort(key=lambda c: int(c.get("open_time", c.get("timestamp", 0)) or 0))
    if len(closed) < 20:
        raise ValueError("at least 20 closed candles are required")
    issues = validate_candles(closed, timeframe)
    as_of = int(closed[-1].get("close_time") or closed[-1].get("open_time") or closed[-1].get("timestamp") or 0)
    closes = [_number(c.get("close")) for c in closed]
    volumes = [max(0.0, _number(c.get("volume"))) for c in closed]
    ranges = [max(0.0, _number(c.get("high")) - _number(c.get("low"))) for c in closed]
    atrs = atr_series(closed)
    atr = atrs[-1] if atrs else None
    price = closes[-1]
    structure = analyze_market_structure(closed, swing_lookback=5, max_break_age=4)
    direction = structure.get("direction")
    ob, fvg, breaker = _ob_fvg(closed, direction)
    volume_mean = statistics.fmean(volumes[-20:]) if len(volumes) >= 20 else None
    volume_std_window = volumes[-100:]
    volume_percentile = percentile_rank(volume_std_window, volumes[-1])
    current_range = ranges[-1]
    range_percentile = percentile_rank(ranges[-100:], current_range)
    atr_percentile = percentile_rank(atrs[-100:], atr) if atr is not None else None
    vwap_denominator = sum(volumes)
    vwap = sum(((float(c["high"])+float(c["low"])+float(c["close"]))/3)*v for c,v in zip(closed,volumes))/vwap_denominator if vwap_denominator else None
    profile = _volume_profile(closed[-200:])
    swing_high = max(closes[-50:]); swing_low = min(closes[-50:]); span = swing_high - swing_low
    fib = {"0.5": swing_low + span * .5, "0.618": swing_low + span * .618,
           "0.705": swing_low + span * .705, "0.786": swing_low + span * .786}
    relative_strength: dict[str, Any] = {}
    for name, series in (benchmark or {}).items():
        bench = [_number(c.get("close")) for c in series if c.get("close") is not None]
        if len(closes) >= 2 and len(bench) >= 2 and closes[-2] and bench[-2]:
            relative_strength[name.upper()] = round((closes[-1]/closes[-2]-1) - (bench[-1]/bench[-2]-1), 8)
    event = structure.get("event") or {}
    period_levels = _important_period_levels(closed)
    compression_duration = 0
    if len(ranges) >= 20:
        threshold = statistics.fmean(ranges[-20:])
        for value in reversed(ranges[:-1]):
            if value <= threshold:
                compression_duration += 1
            else:
                break
    result = {
        "feature_version": FEATURE_VERSION, "dataset_version": dataset_version,
        "source": "GATE", "symbol": symbol.upper(), "timeframe": timeframe, "as_of": as_of,
        "point_in_time": True, "closed_candles_only": True,
        "price": price,
        "structure": {"direction": direction, "trend_direction": structure.get("trend_direction"),
                      "event": event.get("type"), "event_direction": event.get("direction"),
                      "event_level": event.get("level"),
                      "event_age": len(closed)-1-int(event.get("candle_index",len(closed)-1)) if event else None,
                      "swings": structure.get("classified", [])[-12:]},
        "location": {"ob": ob, "fvg": fvg, "breaker": breaker, "vwap": vwap,
                     "distance_vwap_atr": (price-vwap)/atr if vwap is not None and atr else None,
                     "volume_profile": profile, "distance_poc_atr": (price-profile["poc"])/atr if profile and atr else None,
                     "fibonacci": fib, "important_levels": period_levels},
        "participation": {"volume": volumes[-1], "volume_sma20": volume_mean,
                          "relative_volume": volumes[-1]/volume_mean if volume_mean else None,
                          "volume_percentile": volume_percentile,
                          "volume_zscore": zscore(volume_std_window, volumes[-1]),
                          "volume_acceleration": volumes[-1]/statistics.fmean(volumes[-5:-1]) if len(volumes)>=5 and statistics.fmean(volumes[-5:-1]) else None,
                          "cvd": _cvd_proxy(closed[-200:]), "venue": "GATE",
                          "venue_normalized": True},
        "volatility": {"atr": atr, "atr_price_pct": atr/price*100 if atr and price else None,
                       "atr_percentile": atr_percentile, "candle_range": current_range,
                       "range_percentile": range_percentile,
                       "compression_duration": compression_duration if len(ranges)>=20 else None,
                       "expansion_velocity": current_range/statistics.fmean(ranges[-20:-1]) if len(ranges)>=20 and statistics.fmean(ranges[-20:-1]) else None},
        "momentum": {"rsi": rsi(closes), "macd": _macd(closes)},
        "relative_strength": relative_strength,
        "regime": _regime(closes, atr_percentile, range_percentile),
        "session": _session(int(closed[-1].get("open_time", closed[-1].get("timestamp", 0)) or 0)),
        "derivatives": dict(external or {}),
        "causal_domains": ["LOCATION", "STRUCTURE", "TRIGGER", "PARTICIPATION", "DERIVATIVES", "CONTEXT", "GEOMETRY"],
        "data_quality": {"status": "INVALID" if any(x["severity"] == "ERROR" for x in issues) else "WARNING" if issues else "VALID",
                         "issues": issues[-100:], "candle_count": len(closed)},
    }
    return result


def levels_from_snapshot(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    symbol, timeframe, as_of = snapshot["symbol"], snapshot["timeframe"], int(snapshot["as_of"])
    location = snapshot.get("location") or {}; structure = snapshot.get("structure") or {}
    levels=[]
    for swing in structure.get("swings") or []:
        price = _number(swing.get("price")); kind = str(swing.get("kind") or "SWING")
        levels.append({"level_id":stable_id("swing-level",symbol,timeframe,kind,swing.get("idx"),swing.get("confirmed_idx"),price),
                       "symbol":symbol,"timeframe":timeframe,"level_type":kind,"direction":"",
                       "low":price,"high":price,"created_at_ts":as_of,"status":"ACTIVE",
                       "attributes":{"pivot_index":swing.get("idx"),"confirmed_idx":swing.get("confirmed_idx")}})
    ob = location.get("ob")
    if isinstance(ob, Mapping):
        low = _number(ob.get("bottom", ob.get("low"))); high = _number(ob.get("top", ob.get("high")))
        if low > 0 and high >= low:
            levels.append({"level_id":stable_id("ob-level",symbol,timeframe,ob.get("idx",ob.get("created_at")),low,high),
                           "symbol":symbol,"timeframe":timeframe,"level_type":"OB","direction":structure.get("direction") or "",
                           "low":low,"high":high,"created_at_ts":as_of,"status":"ACTIVE","attributes":dict(ob)})
    fvg = location.get("fvg")
    if isinstance(fvg, Mapping):
        low = _number(fvg.get("bottom", fvg.get("low"))); high = _number(fvg.get("top", fvg.get("high")))
        if low > 0 and high >= low:
            levels.append({"level_id":stable_id("fvg-level",symbol,timeframe,fvg.get("idx",fvg.get("created_at")),low,high),
                           "symbol":symbol,"timeframe":timeframe,"level_type":"FVG","direction":structure.get("direction") or "",
                           "low":low,"high":high,"created_at_ts":as_of,"status":"ACTIVE","attributes":dict(fvg)})
    profile = location.get("volume_profile") or {}
    for key in ("poc","vah","val"):
        price = _number(profile.get(key))
        if price > 0:
            levels.append({"level_id":stable_id("profile-level",symbol,timeframe,key,price),
                           "symbol":symbol,"timeframe":timeframe,"level_type":key.upper(),"direction":"",
                           "low":price,"high":price,"created_at_ts":as_of,"status":"ACTIVE","attributes":{}})
    vwap = _number(location.get("vwap"))
    if vwap > 0:
        levels.append({"level_id":stable_id("vwap-level",symbol,timeframe,vwap),
                       "symbol":symbol,"timeframe":timeframe,"level_type":"VWAP","direction":"",
                           "low":vwap,"high":vwap,"created_at_ts":as_of,"status":"ACTIVE","attributes":{}})
    important = location.get("important_levels") if isinstance(location.get("important_levels"), Mapping) else {}
    for key, level_type in (("previous_day_high", "PDH"), ("previous_day_low", "PDL"),
                             ("previous_week_high", "PWH"), ("previous_week_low", "PWL")):
        price = _number(important.get(key))
        if price > 0:
            levels.append({"level_id":stable_id("period-level",symbol,timeframe,level_type,important.get(key),price),
                           "symbol":symbol,"timeframe":timeframe,"level_type":level_type,"direction":"",
                           "low":price,"high":price,"created_at_ts":as_of,"status":"ACTIVE",
                           "attributes":{"period_key":important.get(key.replace("_high", "").replace("_low", ""))}})
    return levels


__all__ = ["FEATURE_VERSION", "TIMEFRAME_SECONDS", "compute_feature_snapshot", "levels_from_snapshot",
           "percentile_rank", "validate_candles"]
