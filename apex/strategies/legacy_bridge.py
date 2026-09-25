"""Temporary audited bridge preserving exact legacy detector output."""

from __future__ import annotations

import copy
from typing import Any, Callable, Mapping

from apex.domain.enums import Strategy
from apex.domain.models import MarketSnapshot
from apex.market.snapshot_scope import use_market_snapshot
from apex.telemetry.event_log import take_last_completed_attempt

from .base import CheckTrace, StrategyTrace


def run_legacy_detector(
    strategy: Strategy,
    symbol: str,
    detector: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> StrategyTrace:
    # Discard a stale trace from an earlier call on this worker thread.
    take_last_completed_attempt()
    result = detector(*args, **kwargs)
    payload = take_last_completed_attempt(strategy=strategy.value, symbol=symbol) or {}
    checks = tuple(
        CheckTrace(
            code=str(row.get("code") or ""),
            label=str(row.get("label") or ""),
            condition=str(row.get("condition") or ""),
            outcome=str(row.get("state") or "UNKNOWN").upper(),
            role=str(row.get("role") or "OBSERVED_CHECK"),
            blocking=bool(row.get("blocking_stop")),
            sequence=index,
            actual_value=copy.deepcopy(row.get("actual_value")),
            required_value=copy.deepcopy(row.get("required_value")),
        )
        for index, row in enumerate(payload.get("checks") or (), start=1)
        if isinstance(row, Mapping)
    )
    raw = copy.deepcopy(result) if isinstance(result, Mapping) else None
    outcome = str(payload.get("outcome") or (
        "CANDIDATE" if raw else "FILTERED"
    )).upper()
    return StrategyTrace(
        strategy=strategy, symbol=symbol.upper(), outcome=outcome,
        raw_result=raw, checks=checks,
        stop=copy.deepcopy(payload.get("stop")) if isinstance(payload.get("stop"), Mapping) else None,
        attempt_key=str(payload.get("attempt_key")) if payload.get("attempt_key") else None,
        subtype=str(payload.get("subtype") or "").upper(),
    )


def snapshot_symbol_detector(detector: Callable[..., Any]) -> Callable[..., Any]:
    """Adapt a symbol detector to snapshot-only candle reads.

    This does not activate the replacement. It supplies a parity candidate
    whose candle dependencies are fenced by ``use_market_snapshot``.
    """
    def evaluate(snapshot: MarketSnapshot, **kwargs: Any):
        with use_market_snapshot(snapshot):
            return detector(snapshot.symbol, **kwargs)

    return evaluate


__all__ = ["run_legacy_detector", "snapshot_symbol_detector"]
