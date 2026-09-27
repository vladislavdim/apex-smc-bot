"""Manager structural-analysis boundary.

Manager consumes canonical market structure; it never owns scanner geometry.
"""
from apex.market.structure import analyze_market_structure


def manager_structure(candles, *, swing_lookback=5, max_break_age=1):
    return analyze_market_structure(candles, swing_lookback=swing_lookback, max_break_age=max_break_age)


__all__=["analyze_market_structure","manager_structure"]
