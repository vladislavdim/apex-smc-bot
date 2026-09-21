"""Single strategy registry shared by manual and scheduled callers."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Mapping

from apex.domain.enums import Strategy
from apex.domain.models import MarketSnapshot
from .base import StrategyAdapter, StrategyTrace


class StrategyRegistry:
    def __init__(self, adapters: Mapping[Strategy, StrategyAdapter] | None = None) -> None:
        self._adapters = dict(adapters or {})

    def register(self, adapter: StrategyAdapter) -> None:
        existing = self._adapters.get(adapter.strategy)
        if existing is not None and existing is not adapter:
            raise ValueError(f"strategy_already_registered:{adapter.strategy.value}")
        self._adapters[adapter.strategy] = adapter

    def evaluate(self, strategy: Strategy | str, symbol: str, **kwargs: Any) -> tuple[StrategyTrace, ...]:
        key = strategy if isinstance(strategy, Strategy) else Strategy(str(strategy).upper())
        try:
            adapter = self._adapters[key]
        except KeyError as exc:
            raise RuntimeError(f"strategy_not_registered:{key.value}") from exc
        return adapter.evaluate(symbol.upper(), **kwargs)

    def evaluate_snapshot(
        self,
        strategy: Strategy | str,
        snapshot: MarketSnapshot,
        **kwargs: Any,
    ) -> tuple[StrategyTrace, ...]:
        key = strategy if isinstance(strategy, Strategy) else Strategy(str(strategy).upper())
        try:
            adapter = self._adapters[key]
        except KeyError as exc:
            raise RuntimeError(f"strategy_not_registered:{key.value}") from exc
        evaluator = getattr(adapter, "evaluate_snapshot", None)
        if not callable(evaluator):
            raise RuntimeError(f"snapshot_adapter_not_registered:{key.value}")
        return evaluator(snapshot, **kwargs)

    def complete(self) -> bool:
        return set(self._adapters) == set(Strategy)

    def registered_adapters(self) -> Mapping[Strategy, StrategyAdapter]:
        """Expose a read-only registry view for offline parity verification."""
        return MappingProxyType(self._adapters)


__all__ = ["StrategyRegistry"]
