"""Central market-data policy for APEX.

Strategy calculations are Gate USD-M first and, by default, Gate only.  Other
exchange adapters remain available for explicit diagnostics, but they are not
allowed to fan out during normal scans.  Binance is reserved for deterministic
execution after a candidate has passed the strategy and Groq quality gates.
"""

from __future__ import annotations

import os
from collections.abc import Mapping


DEFAULT_MARKET_DATA_PROVIDERS = ("gate",)
_ALLOWED_PROVIDERS = {"gate", "binance", "bybit", "hyperliquid"}


def configured_market_data_providers(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    source = os.environ if environ is None else environ
    raw = str(source.get("APEX_MARKET_DATA_PROVIDERS", "gate"))
    providers: list[str] = []
    for item in raw.split(","):
        provider = item.strip().lower()
        if provider in _ALLOWED_PROVIDERS and provider not in providers:
            providers.append(provider)
    return tuple(providers) or DEFAULT_MARKET_DATA_PROVIDERS


def provider_enabled(provider: str, environ: Mapping[str, str] | None = None) -> bool:
    return provider.strip().lower() in configured_market_data_providers(environ)
