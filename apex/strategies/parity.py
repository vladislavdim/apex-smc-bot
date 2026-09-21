"""Behavioral gate for replacing a legacy strategy detector.

The migration is allowed to change implementation structure, but not observable
trading behavior.  Both adapters must evaluate the same immutable market case;
the gate compares the ordered decision journal, terminal reason and candidate
geometry.  Runtime IDs and diagnostic snapshots are intentionally excluded.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from apex.domain.enums import Direction, Strategy
from apex.domain.models import MarketSnapshot

from .base import CheckTrace, StrategyAdapter, StrategyTrace


_GEOMETRY_FIELDS = ("direction", "entry", "sl", "tp", "tp1", "tp2", "tp3", "rr")


@dataclass(frozen=True)
class ParityMismatch:
    path: str
    legacy: Any
    replacement: Any


@dataclass(frozen=True)
class ParityReport:
    case_id: str
    strategy: Strategy
    symbol: str
    matched: bool
    mismatches: tuple[ParityMismatch, ...]


class StrategyParityError(RuntimeError):
    def __init__(self, report: ParityReport) -> None:
        paths = ",".join(row.path for row in report.mismatches[:8])
        super().__init__(f"strategy_parity_failed:{report.case_id}:{paths}")
        self.report = report


def _stable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _stable(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_stable(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return tuple(sorted((_stable(item) for item in value), key=repr))
    return value


def _direction(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return Direction.normalize(value).value
    except (TypeError, ValueError):
        return str(value).strip().upper()


def _geometry(trace: StrategyTrace) -> tuple[tuple[str, Any], ...] | None:
    row = trace.raw_result
    if not isinstance(row, Mapping):
        return None
    values: list[tuple[str, Any]] = []
    for field in _GEOMETRY_FIELDS:
        value = row.get(field)
        if field == "direction":
            value = _direction(value)
        values.append((field, _stable(value)))
    return tuple(values)


def _check(check: CheckTrace) -> tuple[Any, ...]:
    return (
        check.sequence,
        check.code,
        check.condition,
        check.outcome,
        check.role,
        check.blocking,
        _stable(check.actual_value),
        _stable(check.required_value),
    )


def _stop(trace: StrategyTrace) -> tuple[tuple[str, Any], ...] | None:
    if not isinstance(trace.stop, Mapping):
        return None
    # Local-variable snapshots are diagnostic evidence, not business behavior.
    fields = ("code", "condition", "blocking_mapping", "blocking_check_code")
    return tuple((field, _stable(trace.stop.get(field))) for field in fields)


def _trace_contract(trace: StrategyTrace) -> dict[str, Any]:
    return {
        "strategy": trace.strategy.value,
        "subtype": trace.subtype,
        "symbol": trace.symbol,
        "outcome": trace.outcome,
        "checks": tuple(_check(row) for row in trace.checks),
        "stop": _stop(trace),
        "geometry": _geometry(trace),
    }


def compare_traces(
    case_id: str,
    legacy: Sequence[StrategyTrace],
    replacement: Sequence[StrategyTrace],
) -> ParityReport:
    """Compare two results from the same frozen point-in-time market case."""
    if not case_id.strip():
        raise ValueError("parity_case_id_required")
    if not legacy:
        raise ValueError("legacy_trace_required")
    strategy = legacy[0].strategy
    symbol = legacy[0].symbol
    mismatches: list[ParityMismatch] = []
    if len(legacy) != len(replacement):
        mismatches.append(ParityMismatch("traces.length", len(legacy), len(replacement)))
    for index in range(max(len(legacy), len(replacement))):
        if index >= len(legacy) or index >= len(replacement):
            continue
        left = _trace_contract(legacy[index])
        right = _trace_contract(replacement[index])
        for key in left:
            if left[key] != right[key]:
                mismatches.append(ParityMismatch(f"traces[{index}].{key}", left[key], right[key]))
    return ParityReport(
        case_id=case_id,
        strategy=strategy,
        symbol=symbol,
        matched=not mismatches,
        mismatches=tuple(mismatches),
    )


def evaluate_parity(
    case_id: str,
    legacy: StrategyAdapter,
    replacement: StrategyAdapter,
    symbol: str,
    **kwargs: Any,
) -> ParityReport:
    """Run both implementations with identical arguments and compare output."""
    if legacy.strategy != replacement.strategy:
        raise ValueError("parity_strategy_mismatch")
    legacy_traces = legacy.evaluate(symbol, **kwargs)
    replacement_traces = replacement.evaluate(symbol, **kwargs)
    return compare_traces(case_id, legacy_traces, replacement_traces)


def evaluate_snapshot_parity(
    case_id: str,
    legacy: StrategyAdapter,
    replacement: StrategyAdapter,
    snapshot: MarketSnapshot,
    **kwargs: Any,
) -> ParityReport:
    """Compare legacy symbol evaluation with snapshot-only replacement."""
    if legacy.strategy != replacement.strategy:
        raise ValueError("parity_strategy_mismatch")
    evaluator = getattr(replacement, "evaluate_snapshot", None)
    if not callable(evaluator):
        raise ValueError("snapshot_parity_adapter_required")
    legacy_traces = legacy.evaluate(snapshot.symbol, **kwargs)
    replacement_traces = evaluator(snapshot, **kwargs)
    return compare_traces(case_id, legacy_traces, replacement_traces)


def require_parity(report: ParityReport) -> None:
    """Fail closed; callers may only activate a replacement after this passes."""
    if not report.matched:
        raise StrategyParityError(report)


__all__ = [
    "ParityMismatch",
    "ParityReport",
    "StrategyParityError",
    "compare_traces",
    "evaluate_parity",
    "evaluate_snapshot_parity",
    "require_parity",
]
