"""APEX core package bootstrap."""

# Coordinate only CoinGecko public GET requests across modules.  The guard is
# transparent to callers and leaves all strategy/data fallback logic unchanged.
try:
    from .coingecko_guard import install as _install_coingecko_guard
    _install_coingecko_guard()
except Exception:
    # A protection layer must never prevent APEX from starting.
    pass
