"""Which data each production strategy may consume."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping

from apex.domain.enums import Strategy


class Relevance(str, Enum):
    REQUIRED = "REQUIRED"
    USEFUL = "USEFUL"
    LOW_RELEVANCE = "LOW_RELEVANCE"
    NOT_USED = "NOT_USED"


@dataclass(frozen=True)
class StrategyDataContract:
    strategy: Strategy
    fields: Mapping[str, Relevance]

    def relevant_fields(self) -> tuple[str, ...]:
        return tuple(name for name, relevance in self.fields.items() if relevance in {Relevance.REQUIRED, Relevance.USEFUL})

    def required_fields(self) -> tuple[str, ...]:
        return tuple(name for name, relevance in self.fields.items() if relevance is Relevance.REQUIRED)


def _contract(strategy: Strategy, required: tuple[str, ...], useful: tuple[str, ...], low: tuple[str, ...] = ()) -> StrategyDataContract:
    fields = {name: Relevance.REQUIRED for name in required}
    fields.update({name: Relevance.USEFUL for name in useful})
    fields.update({name: Relevance.LOW_RELEVANCE for name in low})
    return StrategyDataContract(strategy, fields)


CONTRACTS: Mapping[Strategy, StrategyDataContract] = {
    Strategy.FAST: _contract(
        Strategy.FAST,
        ("gate_15m", "gate_1h_4h_context", "session", "bos_choch", "ob_fvg", "volume", "structural_targets"),
        ("gate_5m", "spread", "depth_imbalance", "microprice", "taker_imbalance", "cvd_real", "oi_velocity", "funding", "long_short_ratio", "liquidations", "slippage_history"),
        ("options", "onchain", "defi", "slow_whale_flow"),
    ),
    Strategy.MTF: _contract(
        Strategy.MTF,
        ("gate_15m", "gate_1h", "gate_4h", "gate_1d_context", "ob_fvg", "premium_discount", "htf_direction", "fresh_15m_structure", "structural_geometry"),
        ("oi", "funding", "long_short_ratio", "relative_strength", "breadth", "btc_regime", "volume_percentile"),
        ("short_orderbook_noise",),
    ),
    Strategy.ZONE: _contract(
        Strategy.ZONE,
        ("location_4h", "zone_lifecycle", "structure_1h", "rejection", "structural_targets"),
        ("zone_touches", "mitigation_count", "sweep_reclaim", "oi_change", "visible_liquidity", "taker_flow", "volume_percentile"),
    ),
    Strategy.SWING: _contract(
        Strategy.SWING,
        ("thesis_4h", "fresh_structure_1h", "execution_15m", "structural_geometry"),
        ("btc_regime", "relative_strength", "breadth", "oi", "funding", "long_short_ratio", "options", "htf_volume"),
        ("five_second_orderbook",),
    ),
    Strategy.WYCKOFF: _contract(
        Strategy.WYCKOFF,
        ("phase_1d_4h", "range", "volume_behavior", "spring_sos", "utad_sow", "reaccumulation", "creek_ice", "structural_target"),
        ("volume_profile", "poc_migration", "oi", "funding", "long_short_ratio", "btc_options", "breadth", "relative_strength"),
        ("rsi", "macd", "random_orderbook_imbalance"),
    ),
}


def contract_for(strategy: Strategy | str) -> StrategyDataContract:
    key = strategy if isinstance(strategy, Strategy) else Strategy(str(strategy).upper())
    return CONTRACTS[key]


__all__ = ["CONTRACTS", "Relevance", "StrategyDataContract", "contract_for"]
