"""Manager structural-analysis boundary.

Manager consumes canonical market structure; it does not calculate entries,
stops, targets, or submit exchange orders.
"""
from apex.market.structure import analyze_structure
__all__=["analyze_structure"]
