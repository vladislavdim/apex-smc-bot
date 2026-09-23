"""The single production source-authority and provenance registry."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from apex.config.settings import ApexConfig
from apex.domain.enums import SourceMode


class SourcePolicyError(ValueError):
    pass


@dataclass(frozen=True)
class SourceSpec:
    source: str
    owner: str
    role: str
    mode: SourceMode
    freshness_seconds: int | None
    required: bool = False
    budget_key: str = "custom"
    provenance: str = "provider-normalized public context"
    fallback: str = "omit context when unavailable"
    coverage: str = "provider-dependent"
    rate_limit: str = "bounded local rolling budget"
    cost: str = "provider terms vary"
    can_influence_entry: bool = False
    can_execute: bool = False

    @property
    def market_data(self) -> bool:
        """Compatibility field; only PRIMARY_MARKET has strategy authority."""
        return self.mode is SourceMode.PRIMARY_MARKET


def _context(
    source: str, owner: str, role: str, freshness: int | None,
    *, budget: str = "custom", provenance: str = "provider-normalized public context",
    fallback: str = "omit context when unavailable", coverage: str = "provider-dependent",
) -> SourceSpec:
    return SourceSpec(
        source, owner, role, SourceMode.LIVE_CONTEXT, freshness,
        budget_key=budget, provenance=provenance, fallback=fallback, coverage=coverage,
    )


_SOURCES = (
    SourceSpec(
        "gate", "Gate.io", "candles/price/volume/structure/scanning",
        SourceMode.PRIMARY_MARKET, 120, required=True, budget_key="gate",
        provenance="Gate USD-M public market data",
        fallback="fail closed on stale or missing critical data",
        coverage="configured Gate USD-M symbols", can_influence_entry=True,
    ),
    SourceSpec(
        "binance", "Binance", "account/orders/fills/protection",
        SourceMode.EXECUTION, None, required=True, budget_key="execution",
        provenance="confirmed account order and fill state",
        fallback="preserve protection and reconcile",
        coverage="configured futures account", can_execute=True,
    ),
    _context("gate_ws", "Gate.io", "trades/orderbook/microstructure", 5, budget="gate", provenance="sequence-verified Gate WebSocket", fallback="Gate REST snapshot or UNKNOWN", coverage="explicitly enabled symbols"),
    _context("gate_derivatives", "Gate.io", "OI/funding/long-short/liquidations", 120, budget="gate", provenance="Gate USD-M contract statistics"),
    _context("live_market_tape", "Gate.io", "bounded trades/liquidations tape", 120, budget="gate", provenance="Gate public WebSocket tape", fallback="UNKNOWN when disconnected", coverage="explicitly enabled symbols"),
    _context("public_futures", "Public futures provider", "derivatives fallback", 120),
    _context("crypto_monitor", "Public monitor", "OI/funding/liquidations", 120),
    _context("crypto_whale_tracker", "Public tracker", "large-transfer context", 900),
    _context("deepbluealpha", "DeepBlueAlpha", "provider-labelled flow context", 3600, budget="deepbluealpha"),
    _context("open_labels_initiative", "Open Labels Initiative", "on-chain labels", 86400, budget="mempool", provenance="public labels; not institutional-flow proof"),
    _context("coinmetrics", "Coin Metrics", "network activity/reference context", 21600, budget="coinmetrics", coverage="community-supported assets"),
    _context("coinalyze", "Coinalyze", "derivatives consensus", 120, budget="coinalyze"),
    _context("hyperliquid", "Hyperliquid", "derivatives context", 120, budget="hyperliquid"),
    _context("defillama", "DefiLlama", "slow protocol/stablecoin regime", 21600, budget="defillama"),
    _context("deribit", "Deribit", "BTC/ETH options volatility", 1800, budget="deribit", coverage="Deribit BTC/ETH options"),
    _context("mempool", "mempool.space", "Bitcoin on-chain context", 900, budget="mempool", coverage="Bitcoin network"),
    _context("dexscreener", "DEX Screener", "DEX liquidity context", 1800, budget="dexscreener"),
    _context("news", "Public news/calendar", "macro calendar/news risk", 900, budget="news", provenance="public calendar and RSS; no directional prediction"),
    _context("bls", "U.S. Bureau of Labor Statistics", "official macro actuals", 21600, budget="bls", provenance="BLS published observations", coverage="configured U.S. series"),
    SourceSpec("cvd_proxy", "APEX", "candle-estimated CVD", SourceMode.PROXY, 120, provenance="derived from confirmed Gate candles"),
    SourceSpec("liquidity_proxy", "APEX", "candle liquidity proxy", SourceMode.PROXY, 120, provenance="derived from confirmed Gate candles"),
    SourceSpec("pair_registry", "APEX", "symbol/provenance mapping", SourceMode.INTERNAL, None, required=True, provenance="local provider-symbol registry", fallback="do not request unsupported provider", coverage="configured symbols", cost="internal"),
)

SOURCES: Mapping[str, SourceSpec] = {item.source: item for item in _SOURCES}
REGISTRY = SOURCES
SOURCE_ALIASES: Mapping[str, str] = {
    "coinmetrics_community": "coinmetrics",
    "deribit_options": "deribit",
    "btc_mempool": "mempool",
}

DEFAULT_MARKET_DATA_PROVIDERS = ("gate",)
_ALLOWED_MARKET_DATA_PROVIDERS = {"gate"}


def configured_market_data_providers(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Return only production-authorized primary market-data providers."""
    configured = ApexConfig.from_env(environ).operational.market_data_providers
    providers: list[str] = []
    for provider in configured:
        if provider in _ALLOWED_MARKET_DATA_PROVIDERS and provider not in providers:
            providers.append(provider)
    return tuple(providers) or DEFAULT_MARKET_DATA_PROVIDERS


def provider_enabled(provider: str, environ: Mapping[str, str] | None = None) -> bool:
    return provider.strip().lower() in configured_market_data_providers(environ)


def source_spec(source: str) -> SourceSpec:
    raw = str(source or "").strip().lower()
    key = SOURCE_ALIASES.get(raw, raw)
    try:
        return SOURCES[key]
    except KeyError as exc:
        raise SourcePolicyError(f"unknown_source:{key or 'empty'}") from exc


get_source = source_spec


def authorize(source: str, purpose: str) -> bool:
    spec = source_spec(source)
    requested = str(purpose or "").strip().lower()
    if requested in {"candles", "price", "volume", "indicators", "mtf", "structure", "scanner", "market_data"}:
        if spec.mode is not SourceMode.PRIMARY_MARKET:
            raise SourcePolicyError(f"primary_market_required:{spec.source}")
    elif requested in {"execution", "order", "fill", "protection", "validated_execution"}:
        if spec.mode is not SourceMode.EXECUTION:
            raise SourcePolicyError(f"execution_source_required:{spec.source}")
    elif requested in {"context", "live_context", "microstructure", "derivatives", "proxy"}:
        if spec.mode not in {SourceMode.LIVE_CONTEXT, SourceMode.PROXY, SourceMode.INTERNAL}:
            raise SourcePolicyError(f"context_source_forbidden:{spec.source}")
    else:
        raise SourcePolicyError(f"unknown_purpose:{requested or 'empty'}")
    return True


validate_source_usage = authorize


def registry_snapshot() -> list[dict[str, Any]]:
    return [{**asdict(item), "mode": item.mode.value, "market_data": item.market_data} for item in _SOURCES]


def source_contract(source: str) -> dict[str, Any]:
    item = source_spec(source)
    return {**asdict(item), "mode": item.mode.value, "market_data": item.market_data}


__all__ = [
    "REGISTRY", "SOURCES", "SOURCE_ALIASES", "SourcePolicyError", "SourceSpec",
    "authorize", "configured_market_data_providers", "get_source", "provider_enabled",
    "registry_snapshot", "source_contract", "source_spec",
    "validate_source_usage",
]
