from __future__ import annotations

from typing import Any, Callable, Iterable

from apex.domain.enums import Strategy
from apex.domain.models import MarketSnapshot
from .base import StrategyTrace
from .legacy_bridge import run_legacy_detector


class WyckoffStrategy:
    strategy = Strategy.WYCKOFF

    def __init__(
        self,
        detectors: Iterable[Callable[..., Any]],
        snapshot_detectors: Iterable[Callable[..., Any]] | None = None,
    ) -> None:
        self.detectors = tuple(detectors)
        if len(self.detectors) != 3:
            raise ValueError("wyckoff_requires_spring_distribution_reaccumulation")
        self.snapshot_detectors = (
            tuple(snapshot_detectors) if snapshot_detectors is not None else None
        )
        if self.snapshot_detectors is not None and len(self.snapshot_detectors) != 3:
            raise ValueError("wyckoff_snapshot_requires_three_subtypes")

    def evaluate(self, symbol: str, **kwargs: Any) -> tuple[StrategyTrace, ...]:
        # Preserve all three existing subtypes during migration. Deduplication
        # belongs after candidate construction, never inside one detector.
        return tuple(
            run_legacy_detector(self.strategy, symbol, detector, symbol)
            for detector in self.detectors
        )

    def evaluate_snapshot(self, snapshot: MarketSnapshot, **kwargs: Any) -> tuple[StrategyTrace, ...]:
        if self.snapshot_detectors is None:
            raise RuntimeError("snapshot_detector_not_registered:WYCKOFF")
        return tuple(
            run_legacy_detector(
                self.strategy, snapshot.symbol, detector, snapshot,
            )
            for detector in self.snapshot_detectors
        )


__all__ = ["WyckoffStrategy"]
