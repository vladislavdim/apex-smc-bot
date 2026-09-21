from __future__ import annotations

from typing import Any, Callable

from apex.domain.enums import Strategy
from apex.domain.models import MarketSnapshot
from .base import StrategyTrace
from .legacy_bridge import run_legacy_detector


class ZoneStrategy:
    strategy = Strategy.ZONE

    def __init__(self, detector: Callable[..., Any], snapshot_detector: Callable[..., Any] | None = None) -> None:
        self.detector = detector
        self.snapshot_detector = snapshot_detector

    def evaluate(self, symbol: str, **kwargs: Any) -> tuple[StrategyTrace, ...]:
        timeframe = str(kwargs.get("timeframe") or "4h")
        passive = bool(kwargs.get("passive_watch", False))
        return (run_legacy_detector(
            self.strategy, symbol, self.detector, symbol, timeframe,
            passive_watch=passive,
        ),)

    def evaluate_snapshot(self, snapshot: MarketSnapshot, **kwargs: Any) -> tuple[StrategyTrace, ...]:
        if self.snapshot_detector is None:
            raise RuntimeError("snapshot_detector_not_registered:ZONE")
        timeframe = str(kwargs.get("timeframe") or "4h")
        return (run_legacy_detector(
            self.strategy, snapshot.symbol, self.snapshot_detector, snapshot,
            timeframe=timeframe,
            passive_watch=bool(kwargs.get("passive_watch", False)),
        ),)


__all__ = ["ZoneStrategy"]
