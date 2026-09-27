"""Manager structural-analysis boundary.
Manager consumes market facts; it never owns scanner geometry.
"""
from apex.market.structure import analyze_market_structure,detect_latest_structure_event
__all__=["analyze_market_structure","detect_latest_structure_event"]
