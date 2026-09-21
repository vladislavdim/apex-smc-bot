"""Central market-data policy for APEX.

Strategy calculations and exchange market context use Gate USD-M only.  Other
exchange adapters remain in the codebase for isolated development diagnostics,
but configuration cannot add them to normal scans.  Binance is reserved for
deterministic execution after a candidate passes every quality gate.
"""

from __future__ import annotations

from collections.abc import Mapping
from apex.config.settings import ApexConfig


DEFAULT_MARKET_DATA_PROVIDERS = ("gate",)
_ALLOWED_MARKET_DATA_PROVIDERS = {"gate"}


def configured_market_data_providers(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    configured = ApexConfig.from_env(environ).operational.market_data_providers
    providers: list[str] = []
    for provider in configured:
        # Ignore stale/accidental environment configuration that would
        # otherwise fan scans out across exchanges. Binance is execution-only.
        if provider in _ALLOWED_MARKET_DATA_PROVIDERS and provider not in providers:
            providers.append(provider)
    return tuple(providers) or DEFAULT_MARKET_DATA_PROVIDERS


def provider_enabled(provider: str, environ: Mapping[str, str] | None = None) -> bool:
    return provider.strip().lower() in configured_market_data_providers(environ)
