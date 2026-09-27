"""Manager structural-analysis boundary.
Consumes canonical market facts; it does not own scanners or candle transport.
"""
from apex.market.structure import analyze_market_structure,detect_latest_structure_event
__all__=["analyze_market_structure","detect_latest_structure_event"]
