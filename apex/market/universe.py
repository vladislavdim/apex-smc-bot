"""Shared point-in-time market context derived from closed Gate candles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Mapping

from .breadth import market_breadth
from .candles import confirmed_candles
from .relative_strength import relative_strength


def market_cap_omission() -> dict[str, Any]:
    """Explicitly represent the deliberate absence of an approved provider."""
    return {
        "available": False,
        "status": "NOT_COLLECTED",
        "mode": "LIVE_CONTEXT",
        "value": None,
        "reason_code": "MARKET_CAP_SOURCE_NOT_APPROVED",
    }


@dataclass(frozen=True)
class UniverseContext:
    as_of: datetime
    candles: Mapping[str, Mapping[str, tuple[Mapping[str, Any], ...]]]

    def for_symbol(
        self, symbol: str, timeframes: tuple[str, ...], *, as_of: datetime | None = None,
    ) -> dict[str, Any]:
        key = str(symbol).upper()
        boundary = as_of or self.as_of
        if boundary.tzinfo is None:
            boundary = boundary.replace(tzinfo=timezone.utc)
        boundary = boundary.astimezone(timezone.utc)
        by_timeframe: dict[str, Any] = {}
        for timeframe in dict.fromkeys(timeframes):
            universe = {
                item_symbol: confirmed_candles(rows, timeframe, as_of=boundary)
                for item_symbol, rows in self.candles.get(timeframe, {}).items()
            }
            symbol_rows = universe.get(key, ())
            strength = {}
            for benchmark in ("BTCUSDT", "ETHUSDT"):
                benchmark_rows = universe.get(benchmark, ())
                strength[benchmark] = relative_strength(
                    symbol_rows, benchmark_rows, benchmark=benchmark,
                )
            by_timeframe[timeframe] = {
                "breadth": market_breadth(universe),
                "relative_strength": strength,
            }
        return {
            "authority": "LIVE_CONTEXT",
            "can_change_strategy_gate": False,
            "as_of": boundary.isoformat(),
            "timeframes": by_timeframe,
            "market_cap": market_cap_omission(),
        }


def build_universe_context(
    raw_by_timeframe: Mapping[
        str, Mapping[str, list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]]
    ],
    *,
    as_of: datetime,
) -> UniverseContext:
    boundary = as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
    boundary = boundary.astimezone(timezone.utc)
    normalized = {
        timeframe: {
            str(symbol).upper(): confirmed_candles(rows, timeframe, as_of=boundary)
            for symbol, rows in universe.items()
        }
        for timeframe, universe in raw_by_timeframe.items()
    }
    return UniverseContext(boundary, normalized)


class UniverseContextStore:
    """Thread-safe rolling projection fed by the existing scanner candle load."""

    def __init__(self, *, max_bars: int = 100) -> None:
        self._lock = RLock()
        self._candles: dict[str, dict[str, tuple[Mapping[str, Any], ...]]] = {}
        self._max_bars = min(500, max(20, int(max_bars)))

    def update(
        self, symbol: str, timeframe: str,
        rows: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...],
        *, as_of: datetime | None = None,
    ) -> int:
        boundary = as_of or datetime.now(timezone.utc)
        confirmed = confirmed_candles(rows, timeframe, as_of=boundary)
        if not confirmed:
            return 0
        with self._lock:
            self._candles.setdefault(str(timeframe), {})[str(symbol).upper()] = confirmed[-self._max_bars:]
        return min(len(confirmed), self._max_bars)

    def view(
        self, symbol: str, timeframes: tuple[str, ...],
        *, as_of: datetime | None = None,
    ) -> dict[str, Any]:
        boundary = as_of or datetime.now(timezone.utc)
        with self._lock:
            selected = {
                timeframe: dict(self._candles.get(timeframe, {}))
                for timeframe in dict.fromkeys(timeframes)
            }
        return build_universe_context(selected, as_of=boundary).for_symbol(
            symbol, timeframes, as_of=boundary,
        )

    def clear(self) -> None:
        with self._lock:
            self._candles.clear()

    def stored_bars(self, symbol: str, timeframe: str) -> int:
        with self._lock:
            return len(self._candles.get(timeframe, {}).get(str(symbol).upper(), ()))


universe_context_store = UniverseContextStore()


__all__ = [
    "UniverseContext", "UniverseContextStore", "build_universe_context",
    "market_cap_omission", "universe_context_store",
]
