"""Manager structural-analysis boundary.

Manager consumes canonical market structure facts; it does not own scanners.
"""
from apex.market.structure import analyze_market_structure,detect_latest_structure_event,infer_structure_direction
__all__=["analyze_market_structure","detect_latest_structure_event","infer_structure_direction"]
