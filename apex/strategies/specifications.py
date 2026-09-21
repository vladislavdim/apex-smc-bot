"""Machine-readable production strategy specifications.

These mirror existing live rules during migration; they do not change any
threshold or candidate geometry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from apex.domain.enums import Strategy


@dataclass(frozen=True)
class StrategySpecification:
    strategy: Strategy
    working_timeframe: str
    context_timeframes: tuple[str, ...]
    core: tuple[str, ...]
    trigger: tuple[str, ...]
    location: tuple[str, ...]
    geometry: tuple[str, ...]
    management_timeframe: str


SPECIFICATIONS: Mapping[Strategy, StrategySpecification] = {
    Strategy.FAST: StrategySpecification(Strategy.FAST, "15m", ("1h", "4h"), ("session", "htf_support"), ("fresh_bos_choch", "retest", "displacement", "volume"), ("ob_or_fvg",), ("structural_sl", "structural_target", "rr_gte_2"), "5m"),
    Strategy.MTF: StrategySpecification(Strategy.MTF, "15m", ("1h", "4h", "1d", "1w"), ("htf_thesis",), ("fresh_15m_bos_choch",), ("ob_or_fvg", "premium_discount"), ("structural_sl", "structural_target", "rr_gte_2"), "15m"),
    Strategy.ZONE: StrategySpecification(Strategy.ZONE, "1h", ("4h", "1d"), ("zone_lifecycle",), ("closed_1h_structure", "rejection"), ("4h_location",), ("structural_sl", "structural_target", "rr_gte_2"), "15m"),
    Strategy.SWING: StrategySpecification(Strategy.SWING, "15m", ("1h", "4h", "1d", "1w"), ("4h_thesis",), ("fresh_1h_structure", "15m_execution"), ("htf_ob_or_fvg",), ("structural_sl", "structural_target", "rr_gte_2"), "1h"),
    Strategy.WYCKOFF: StrategySpecification(Strategy.WYCKOFF, "4h", ("1d", "1w"), ("phase", "range", "volume_behavior"), ("spring_sos_or_utad_sow",), ("creek_ice",), ("structural_sl", "structural_target", "rr_gte_2"), "1h"),
}


def specification_for(strategy: Strategy | str) -> StrategySpecification:
    key = strategy if isinstance(strategy, Strategy) else Strategy(str(strategy).upper())
    return SPECIFICATIONS[key]


__all__ = ["SPECIFICATIONS", "StrategySpecification", "specification_for"]
