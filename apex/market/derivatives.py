"""Normalized point-in-time derivatives context with explicit unknowns."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from math import isfinite
from typing import Any, Iterable, Mapping


MAX_AGE_SECONDS = {
    "OPEN_INTEREST": 2 * 3600,
    "FUNDING": 9 * 3600,
    "LONG_SHORT_RATIO": 2 * 3600,
    "LIQUIDATIONS": 2 * 3600,
}


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


@dataclass(frozen=True)
class ContextObservation:
    feature: str
    event_time: int
    received_at: int
    source: str
    values: Mapping[str, Any]
    availability: str = "LIVE"
    quality: str = "VALID"
    point_in_time: bool = True

    def public(self, *, as_of: int) -> dict[str, Any]:
        age = max(0, int(as_of) - self.event_time)
        maximum = MAX_AGE_SECONDS.get(self.feature, 0)
        usable = self.quality == "VALID" and bool(maximum) and age <= maximum and self.received_at <= as_of
        return {
            "feature": self.feature,
            "source": self.source,
            "event_time": self.event_time,
            "received_at": self.received_at,
            "age_seconds": age,
            "freshness": "FRESH" if usable else "UNAVAILABLE" if self.quality != "VALID" else "STALE",
            "availability": self.availability,
            "quality": self.quality,
            "point_in_time": self.point_in_time,
            "available": usable,
            "value": dict(self.values) if usable else None,
        }


def normalize_contract_stats(row: Mapping[str, Any], *, received_at: int) -> tuple[ContextObservation, ...]:
    event_time = int(_number(row.get("time")) or 0)
    source = "gate_contract_stats"
    oi_values = {"contracts": _number(row.get("open_interest")), "usd": _number(row.get("open_interest_usd"))}
    oi = ContextObservation(
        "OPEN_INTEREST", event_time, received_at, source,
        oi_values, quality="VALID" if event_time > 0 and any(value is not None for value in oi_values.values()) else "UNAVAILABLE",
    )
    def ratio(explicit: Any, numerator: Any, denominator: Any) -> float | None:
        value = _number(explicit)
        if value is not None:
            return value
        long_value, short_value = _number(numerator), _number(denominator)
        return long_value / short_value if long_value is not None and short_value else None

    ratio_values = {
        "accounts": ratio(row.get("lsr_account"), row.get("long_users"), row.get("short_users")),
        "takers": ratio(row.get("lsr_taker"), row.get("long_taker_size"), row.get("short_taker_size")),
        "top_accounts": ratio(row.get("top_lsr_account"), row.get("top_long_account"), row.get("top_short_account")),
        "top_positions": ratio(row.get("top_lsr_size"), row.get("top_long_size"), row.get("top_short_size")),
    }
    ratios = ContextObservation(
        "LONG_SHORT_RATIO", event_time, received_at, source,
        ratio_values, quality="VALID" if event_time > 0 and any(value is not None for value in ratio_values.values()) else "UNAVAILABLE",
    )
    long_usd = _number(row.get("long_liq_usd_new", row.get("long_liq_usd")))
    short_usd = _number(row.get("short_liq_usd_new", row.get("short_liq_usd")))
    total = None if long_usd is None or short_usd is None else long_usd + short_usd
    liquidation_values = {
        "long_usd": long_usd,
        "short_usd": short_usd,
        "imbalance": None if not total else (long_usd - short_usd) / total,
    }
    liquidations = ContextObservation(
        "LIQUIDATIONS", event_time, received_at, source,
        liquidation_values,
        quality="VALID" if event_time > 0 and (long_usd is not None or short_usd is not None) else "UNAVAILABLE",
    )
    return oi, ratios, liquidations


def normalize_funding(row: Mapping[str, Any], *, received_at: int) -> ContextObservation:
    event_time = int(_number(row.get("t", row.get("time"))) or 0)
    rate = _number(row.get("r", row.get("rate")))
    return ContextObservation(
        "FUNDING", event_time, received_at, "gate_funding", {"rate": rate},
        quality="VALID" if event_time > 0 and rate is not None else "UNAVAILABLE",
    )


class PointInTimeContext:
    def __init__(self, observations: Iterable[ContextObservation] = ()) -> None:
        grouped: dict[str, list[ContextObservation]] = {}
        for row in observations:
            grouped.setdefault(row.feature, []).append(row)
        self._rows = {key: sorted(values, key=lambda row: row.event_time) for key, values in grouped.items()}
        self._times = {key: [row.event_time for row in values] for key, values in self._rows.items()}

    def as_of(self, timestamp: int) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for feature, rows in self._rows.items():
            position = bisect_right(self._times[feature], int(timestamp)) - 1
            while position >= 0 and rows[position].received_at > int(timestamp):
                position -= 1
            if position >= 0:
                output[feature.lower()] = rows[position].public(as_of=int(timestamp))
        for feature in MAX_AGE_SECONDS:
            output.setdefault(feature.lower(), {
                "feature": feature, "source": None, "event_time": None,
                "received_at": None, "age_seconds": None, "freshness": "UNAVAILABLE",
                "availability": "UNKNOWN", "quality": "UNKNOWN",
                "point_in_time": True, "available": False, "value": None,
            })
        return output


__all__ = [
    "ContextObservation", "MAX_AGE_SECONDS", "PointInTimeContext",
    "normalize_contract_stats", "normalize_funding",
]
