"""Versioned Gate close-return clusters; missing evidence never means zero risk."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite, log, sqrt
from typing import Any, Callable, Mapping

from apex.market.candles import candle_open_time

VERSION = "gate-return-clusters-v1"
WINDOW = 168
MIN_OBSERVATIONS = 96


@dataclass(frozen=True)
class ClusterExposure:
    risk_quote: float
    symbols: tuple[str, ...]
    evidence: Mapping[str, Any]


def _returns(rows, as_of):
    closes = {}
    for row in rows:
        opened = candle_open_time(row)
        try:
            close = float(row["close"])
        except (KeyError, TypeError, ValueError):
            return {}
        if (
            opened is None
            or not isfinite(close)
            or close <= 0
            or row.get("is_closed") is False
            or opened + 3600 > as_of
        ):
            continue
        closes[opened] = close
    ordered = sorted(closes)[-(WINDOW + 1) :]
    if not ordered or as_of - (ordered[-1] + 3600) > 3690:
        return {}
    return {
        right: log(closes[right] / closes[left])
        for left, right in zip(ordered, ordered[1:])
        if right - left == 3600
    }


def _correlation(left, right):
    keys = sorted(left.keys() & right.keys())[-WINDOW:]
    if len(keys) < MIN_OBSERVATIONS:
        return None
    x = [left[key] for key in keys]
    y = [right[key] for key in keys]
    mx = sum(x) / len(x)
    my = sum(y) / len(y)
    vx = sum((v - mx) ** 2 for v in x)
    vy = sum((v - my) ** 2 for v in y)
    if vx <= 1e-20 or vy <= 1e-20:
        return None
    return max(
        -1.0, min(1.0, sum((a - mx) * (b - my) for a, b in zip(x, y)) / sqrt(vx * vy))
    )


def assess_cluster_exposure(
    symbol: str,
    exposures: Mapping[str, float],
    *,
    as_of: float,
    candles: Callable | None = None,
    threshold: float = 0.75,
) -> ClusterExposure:
    if not isfinite(threshold) or not 0 < threshold <= 1:
        raise ValueError("invalid_cluster_threshold")
    target = symbol.upper()
    risks = {str(key).upper(): float(value) for key, value in exposures.items()}
    if any(not isfinite(value) or value < 0 for value in risks.values()):
        raise ValueError("invalid_cluster_exposure")
    symbols = sorted(set(risks) | {target})
    parents = {key: key for key in symbols}

    def root(key):
        while parents[key] != key:
            parents[key] = parents[parents[key]]
            key = parents[key]
        return key

    def join(left, right):
        parents[root(right)] = root(left)

    series = {}
    missing = []
    correlations = {}
    if len(symbols) > 1:
        for key in symbols:
            try:
                series[key] = _returns(candles(key) if candles else [], as_of)
            except Exception:
                series[key] = {}
            if not series[key]:
                missing.append(key)
        for index, left in enumerate(symbols):
            for right in symbols[index + 1 :]:
                correlation = _correlation(series[left], series[right])
                correlations[f"{left}:{right}"] = correlation
                # UNKNOWN takes no diversification credit, including missing
                # peers and insufficient aligned history. No direction netting.
                if correlation is None or abs(correlation) >= threshold:
                    join(left, right)
    members = tuple(key for key in symbols if root(key) == root(target))
    unknown = bool(missing) or any(value is None for value in correlations.values())
    return ClusterExposure(
        sum(risks.get(key, 0) for key in members),
        members,
        {
            "version": VERSION,
            "source": "GATE",
            "as_of": as_of,
            "window_hours": WINDOW,
            "minimum_observations": MIN_OBSERVATIONS,
            "threshold": threshold,
            "status": "CONSERVATIVE_UNKNOWN" if unknown else "OBSERVED",
            "unknown_symbols": missing,
            "correlations": correlations,
        },
    )
