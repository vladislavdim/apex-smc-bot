"""APEX core package bootstrap."""

# Coordinate only CoinGecko public GET requests across modules. The guard is
# transparent to callers and leaves all strategy/data fallback logic unchanged.
try:
    from .coingecko_guard import install as _install_coingecko_guard
    _install_coingecko_guard()
except Exception:
    # A protection layer must never prevent APEX from starting.
    pass

# Passive release-cohort / FAST timing observability. This is deliberately
# fail-open and runs before bot.py or stats_server.py import their core modules.
# It never changes a trading predicate, Entry/SL/TP/RR, Groq decision, or order.
try:
    from .runtime_observability import install as _install_runtime_observability
    _install_runtime_observability()
    from .runtime_observability_overrides import apply as _apply_runtime_observability_overrides
    _apply_runtime_observability_overrides()
    from .runtime_observability_fixups import apply as _apply_runtime_observability_fixups
    _apply_runtime_observability_fixups()
except Exception:
    pass
