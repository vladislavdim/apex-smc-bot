from __future__ import annotations

from typing import Any, Callable

from apex.domain.enums import Strategy
from apex.domain.models import MarketSnapshot
from .base import StrategyTrace
from .legacy_bridge import run_legacy_detector


class FastStrategy:
    strategy = Strategy.FAST

    def __init__(
        self,
        detector: Callable[..., Any],
        snapshot_detector: Callable[..., Any] | None = None,
    ) -> None:
        self.detector = detector
        self.snapshot_detector = snapshot_detector

    def evaluate(self, symbol: str, **kwargs: Any) -> tuple[StrategyTrace, ...]:
        return (run_legacy_detector(self.strategy, symbol, self.detector, symbol),)

    def evaluate_snapshot(
        self, snapshot: MarketSnapshot, **kwargs: Any,
    ) -> tuple[StrategyTrace, ...]:
        if self.snapshot_detector is None:
            raise RuntimeError("snapshot_detector_not_registered:FAST")
        return (run_legacy_detector(
            self.strategy, snapshot.symbol, self.snapshot_detector, snapshot,
        ),)


__all__ = ["FastStrategy"]
