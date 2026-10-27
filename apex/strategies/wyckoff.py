from __future__ import annotations

from dataclasses import replace
from typing import Any, Callable, Iterable

from apex.domain.enums import Strategy
from apex.domain.models import MarketSnapshot
from .base import CheckTrace, StrategyTrace
from .legacy_bridge import run_legacy_detector


class WyckoffStrategy:
    strategy = Strategy.WYCKOFF

    def __init__(
        self,
        detectors: Iterable[Callable[..., Any]],
        snapshot_detectors: Iterable[Callable[..., Any]] | None = None,
        sequence_policy: Callable[[MarketSnapshot, str], dict[str, Any]] | None = None,
    ) -> None:
        self.detectors = tuple(detectors)
        if len(self.detectors) != 3:
            raise ValueError("wyckoff_requires_spring_distribution_reaccumulation")
        self.snapshot_detectors = (
            tuple(snapshot_detectors) if snapshot_detectors is not None else None
        )
        if self.snapshot_detectors is not None and len(self.snapshot_detectors) != 3:
            raise ValueError("wyckoff_snapshot_requires_three_subtypes")
        self.sequence_policy = sequence_policy

    def evaluate(self, symbol: str, **kwargs: Any) -> tuple[StrategyTrace, ...]:
        # Preserve all three existing subtypes during migration. Deduplication
        # belongs after candidate construction, never inside one detector.
        return tuple(
            run_legacy_detector(self.strategy, symbol, detector, symbol)
            for detector in self.detectors
        )

    def evaluate_snapshot(
        self, snapshot: MarketSnapshot, **kwargs: Any
    ) -> tuple[StrategyTrace, ...]:
        if self.snapshot_detectors is None:
            raise RuntimeError("snapshot_detector_not_registered:WYCKOFF")
        traces = tuple(
            run_legacy_detector(
                self.strategy,
                snapshot.symbol,
                detector,
                snapshot,
            )
            for detector in self.snapshot_detectors
        )
        if self.sequence_policy is None:
            return traces
        output = []
        for trace in traces:
            if trace.outcome != "CANDIDATE" or trace.raw_result is None:
                output.append(trace)
                continue
            proof = self.sequence_policy(snapshot, trace.subtype)
            if not proof.get("applies") or proof.get("sequence_complete"):
                output.append(trace)
                continue
            code = "WYCKOFF_SEQUENCE_ORDER"
            check = CheckTrace(
                code=code,
                label="Closed-candle WYCKOFF phase chronology",
                condition="climax < reaction < test < trigger < confirmation",
                outcome="FAIL",
                role="HARD_GATE",
                blocking=True,
                sequence=len(trace.checks) + 1,
                actual_value=proof.get("status"),
                required_value="ORDERED",
            )
            output.append(
                replace(
                    trace,
                    outcome="FILTERED",
                    raw_result=None,
                    checks=trace.checks + (check,),
                    stop={"code": code, "reason": proof.get("status")},
                )
            )
        return tuple(output)


__all__ = ["WyckoffStrategy"]
