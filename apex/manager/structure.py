"""Manager structural-analysis boundary.
Manager consumes canonical market structure but does not own market-data logic.
"""
from apex.market.structure import analyze_market_structure,detect_latest_structure_event
__all__=["analyze_market_structure","detect_latest_structure_event"]
