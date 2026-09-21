"""Proof-gated selection between legacy and immutable snapshot evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from apex.domain.enums import Strategy
from apex.market.snapshots import SnapshotBuild

from .base import StrategyTrace
from .parity_corpus import load_corpus_directory
from .parity_runner import load_activation_verdict
from .registry import StrategyRegistry


class SnapshotProvider(Protocol):
    def build(self, strategy: Strategy | str, symbol: str, **kwargs: Any) -> SnapshotBuild: ...


class SnapshotEvaluationBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class StrategyActivationSwitch:
    requested: bool
    active: bool
    reason: str
    verdict_sha256: str = ""

    @classmethod
    def from_proof(
        cls,
        *,
        requested: bool,
        corpus_directory: str | Path,
        verdict_path: str | Path,
    ) -> "StrategyActivationSwitch":
        if not requested:
            return cls(False, False, "SNAPSHOT_ACTIVATION_NOT_REQUESTED")
        try:
            cases = load_corpus_directory(corpus_directory)
            verdict = load_activation_verdict(verdict_path, cases)
        except (OSError, TypeError, ValueError) as exc:
            return cls(True, False, f"SNAPSHOT_PROOF_INVALID:{exc}")
        return cls(True, True, "SNAPSHOT_PROOF_READY", str(verdict["verdict_sha256"]))

    def evaluate(
        self,
        registry: StrategyRegistry,
        provider: SnapshotProvider,
        strategy: Strategy | str,
        symbol: str,
        **kwargs: Any,
    ) -> tuple[StrategyTrace, ...]:
        if not self.active:
            return registry.evaluate(strategy, symbol, **kwargs)
        build = provider.build(strategy, symbol)
        if build.wait_reason_codes:
            raise SnapshotEvaluationBlocked(
                "snapshot_market_data_not_ready:" + ",".join(build.wait_reason_codes)
            )
        if not build.snapshot.candles or any(
            not rows for rows in build.snapshot.candles.values()
        ):
            raise SnapshotEvaluationBlocked("snapshot_market_data_incomplete")
        return registry.evaluate_snapshot(strategy, build.snapshot, **kwargs)


__all__ = [
    "SnapshotEvaluationBlocked", "SnapshotProvider", "StrategyActivationSwitch",
]
