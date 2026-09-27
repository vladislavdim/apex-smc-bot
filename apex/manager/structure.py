"""Manager structural-analysis boundary.
Consumes canonical market structure; it does not calculate trade geometry.
"""
from apex.market.structure import analyze_market_structure,detect_latest_structure_event,infer_structure_direction
__all__=["analyze_market_structure","detect_latest_structure_event","infer_structure_direction"]
