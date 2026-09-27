"""Manager structural-analysis boundary.

Manager consumes confirmed market structure; it does not own market analysis.
"""
from apex.market.structure import analyze_market_structure
__all__=["analyze_market_structure"]
