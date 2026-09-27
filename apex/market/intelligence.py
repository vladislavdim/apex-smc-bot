"""Canonical Market Intelligence lifecycle boundary.

During cutover the proven implementation remains behind this module; runtime
code depends only on apex.market and can be migrated internally without
changing the application composition root.
"""
from core.market_intelligence import refresh_market_intelligence,start_market_intelligence,stop_market_intelligence
__all__=["refresh_market_intelligence","start_market_intelligence","stop_market_intelligence"]
