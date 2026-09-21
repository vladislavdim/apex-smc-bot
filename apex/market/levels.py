"""Canonical closed-candle lifecycle for structural price levels."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Mapping


class LevelState(str, Enum):
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    TOUCHED = "TOUCHED"
    REACTED = "REACTED"
    SWEPT = "SWEPT"
    BROKEN = "BROKEN"
    FLIPPED = "FLIPPED"
    EXPIRED = "EXPIRED"


class LevelSide(str, Enum):
    DEMAND = "DEMAND"
    SUPPLY = "SUPPLY"


@dataclass(frozen=True)
class PriceLevel:
    level_id: str
    symbol: str
    kind: str
    side: LevelSide
    lower: float
    upper: float
    created_at: float
    state: LevelState = LevelState.CREATED
    last_event_at: float | None = None
    touches: int = 0
    broken_from: LevelSide | None = None
    expires_at: float | None = None

    def __post_init__(self) -> None:
        if not self.level_id or not self.symbol:
            raise ValueError("level_identity_required")
        if self.lower >= self.upper:
            raise ValueError("level_lower_must_be_below_upper")


def activate(level: PriceLevel) -> PriceLevel:
    if level.state is not LevelState.CREATED:
        return level
    return replace(level, state=LevelState.ACTIVE, last_event_at=level.created_at)


def advance_level(level: PriceLevel, candle: Mapping[str, Any]) -> PriceLevel:
    """Advance one level using exactly one confirmed venue candle."""
    if candle.get("is_closed") is not True:
        return level
    timestamp = float(candle.get("close_time", candle.get("closed_at", 0)) or 0)
    if timestamp <= 0 or timestamp < level.created_at:
        return level
    if level.expires_at is not None and timestamp >= level.expires_at:
        return replace(level, state=LevelState.EXPIRED, last_event_at=timestamp)
    if level.state is LevelState.EXPIRED:
        return level

    high, low, close = float(candle["high"]), float(candle["low"]), float(candle["close"])
    intersects = high >= level.lower and low <= level.upper

    if level.state is LevelState.BROKEN:
        # A broken demand level can become supply after a closed retest from
        # below; broken supply mirrors this from above.
        if level.broken_from is LevelSide.DEMAND and intersects and close < level.lower:
            return replace(level, state=LevelState.FLIPPED, side=LevelSide.SUPPLY, last_event_at=timestamp)
        if level.broken_from is LevelSide.SUPPLY and intersects and close > level.upper:
            return replace(level, state=LevelState.FLIPPED, side=LevelSide.DEMAND, last_event_at=timestamp)
        return level

    if level.side is LevelSide.DEMAND:
        if close < level.lower:
            return replace(level, state=LevelState.BROKEN, broken_from=LevelSide.DEMAND, last_event_at=timestamp)
        if low < level.lower <= close:
            return replace(level, state=LevelState.SWEPT, touches=level.touches + 1, last_event_at=timestamp)
        if level.state in {LevelState.TOUCHED, LevelState.SWEPT} and close > level.upper:
            return replace(level, state=LevelState.REACTED, last_event_at=timestamp)
    else:
        if close > level.upper:
            return replace(level, state=LevelState.BROKEN, broken_from=LevelSide.SUPPLY, last_event_at=timestamp)
        if high > level.upper >= close:
            return replace(level, state=LevelState.SWEPT, touches=level.touches + 1, last_event_at=timestamp)
        if level.state in {LevelState.TOUCHED, LevelState.SWEPT} and close < level.lower:
            return replace(level, state=LevelState.REACTED, last_event_at=timestamp)

    if intersects and level.state in {LevelState.CREATED, LevelState.ACTIVE, LevelState.REACTED}:
        return replace(level, state=LevelState.TOUCHED, touches=level.touches + 1, last_event_at=timestamp)
    if level.state is LevelState.CREATED:
        return activate(level)
    return level


__all__ = ["LevelSide", "LevelState", "PriceLevel", "activate", "advance_level"]
