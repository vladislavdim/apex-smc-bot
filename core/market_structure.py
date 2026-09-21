"""Compatibility façade for the canonical V3 Structure Engine."""

from apex.market.structure import (
    analyze_market_structure,
    classify_swings,
    detect_latest_structure_event,
    events_with_trend_fallback,
    find_swings,
    infer_structure_direction,
)


__all__ = [
    "analyze_market_structure",
    "classify_swings",
    "detect_latest_structure_event",
    "events_with_trend_fallback",
    "find_swings",
    "infer_structure_direction",
]
