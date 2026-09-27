"""Manager structural-analysis boundary.

Manager consumes canonical market facts; it does not own or duplicate market
structure calculation.
"""
from apex.market.structure import (
    classify_swings,
    detect_latest_structure_event,
    find_swings,
    infer_structure_direction,
)

__all__ = [
    "classify_swings",
    "detect_latest_structure_event",
    "find_swings",
    "infer_structure_direction",
]
