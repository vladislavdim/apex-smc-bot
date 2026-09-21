"""Canonical strategy interface and decision chronology."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from apex.domain.enums import Strategy
from apex.domain.models import MarketSnapshot


@dataclass(frozen=True)
class CheckTrace:
    code: str
    label: str
    condition: str
    outcome: str
    role: str
    blocking: bool
    sequence: int
    actual_value: Any = None
    required_value: Any = None


@dataclass(frozen=True)
class StrategyTrace:
    strategy: Strategy
    symbol: str
    outcome: str
    raw_result: Mapping[str, Any] | None
    checks: tuple[CheckTrace, ...]
    stop: Mapping[str, Any] | None
    attempt_key: str | None
    subtype: str = ""

    @property
    def first_check(self) -> CheckTrace | None:
        return self.checks[0] if self.checks else None

    @property
    def last_check(self) -> CheckTrace | None:
        return self.checks[-1] if self.checks else None


class StrategyAdapter(Protocol):
    strategy: Strategy

    def evaluate(self, symbol: str, **kwargs: Any) -> tuple[StrategyTrace, ...]: ...


class SnapshotStrategyAdapter(StrategyAdapter, Protocol):
    def evaluate_snapshot(
        self, snapshot: MarketSnapshot, **kwargs: Any,
    ) -> tuple[StrategyTrace, ...]: ...


def trace_payload(trace: StrategyTrace) -> dict[str, Any]:
    """Bounded machine-readable explanation attached to a live candidate."""
    checks = [{
        "check_id": row.code,
        "category": row.role,
        "label": row.label,
        "condition": row.condition,
        "actual_value": row.actual_value,
        "required_value": row.required_value,
        "outcome": row.outcome,
        "reason_code": row.code if row.outcome != "PASS" else "",
        "blocking": row.blocking,
        "sequence": row.sequence,
    } for row in trace.checks]
    return {
        "strategy": trace.strategy.value,
        "subtype": trace.subtype,
        "symbol": trace.symbol,
        "outcome": trace.outcome,
        "attempt_key": trace.attempt_key,
        "checks": checks,
        "first_check": checks[0] if checks else None,
        "last_check": checks[-1] if checks else None,
        "stop": dict(trace.stop) if trace.stop is not None else None,
    }


__all__ = [
    "CheckTrace", "SnapshotStrategyAdapter", "StrategyAdapter",
    "StrategyTrace", "trace_payload",
]
