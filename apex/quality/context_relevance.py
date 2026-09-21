"""Filter optional context according to a strategy's declared data contract."""

from __future__ import annotations

from typing import Any, Mapping

from apex.domain.enums import Strategy
from apex.strategies.data_contracts import Relevance, contract_for


_EXTERNAL_ALIASES = {
    "oi": "open_interest", "oi_velocity": "open_interest", "oi_change": "open_interest",
    "funding": "funding", "long_short_ratio": "long_short_ratio",
    "liquidations": "liquidations",
    "cvd_real": "live_tape", "taker_imbalance": "live_tape", "taker_flow": "live_tape",
    "spread": "microstructure", "depth_imbalance": "microstructure",
    "microprice": "microstructure", "visible_liquidity": "microstructure",
    "options": "options_context", "btc_options": "options_context",
    "onchain": "onchain_activity", "defi": "slow_regime",
}


def relevant_context(strategy: Strategy | str, context: Mapping[str, Any]) -> dict[str, Any]:
    contract = contract_for(strategy)
    allowed = {
        name for name, relevance in contract.fields.items()
        if relevance in {Relevance.REQUIRED, Relevance.USEFUL}
    }
    return {name: value for name, value in context.items() if name in allowed}


def relevant_external_context(
    strategy: Strategy | str, context: Mapping[str, Any],
    market_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project normalized provider data into contract-named Groq context."""
    contract = contract_for(strategy)
    selected: dict[str, Any] = {}
    for field in contract.relevant_fields():
        source_key = _EXTERNAL_ALIASES.get(field, field)
        if source_key in context:
            selected[field] = context[source_key]
    universe = market_context if isinstance(market_context, Mapping) else {}
    timeframes = universe.get("timeframes") if isinstance(universe.get("timeframes"), Mapping) else {}
    relevant = set(contract.relevant_fields())
    for field in ("relative_strength", "breadth"):
        if field not in relevant:
            continue
        values = {
            timeframe: payload[field]
            for timeframe, payload in timeframes.items()
            if isinstance(payload, Mapping) and field in payload
        }
        if values:
            selected[field] = values
    return {
        "strategy": contract.strategy.value,
        "fields": selected,
        "data_quality": context.get("data_quality", {}),
        "conflicts": context.get("conflicts", []),
        "market_context_as_of": universe.get("as_of"),
        "rule": "LIVE_CONTEXT_ONLY_NOT_A_STRATEGY_GATE",
    }


__all__ = ["relevant_context", "relevant_external_context"]
