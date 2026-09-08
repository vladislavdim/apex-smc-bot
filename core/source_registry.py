"""APEX source registry and provenance contract.

The registry is intentionally small and declarative.  It prevents an optional
context adapter from becoming an accidental market-data or execution path and
gives the dashboard one place to show ownership, freshness and fallback.  Gate
is the only source allowed to feed candles, indicators, structure and scanner
decisions.  Binance is execution-only behind Manager V2.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


class SourcePolicyError(ValueError):
    """Raised when a source is used outside its declared purpose."""


@dataclass(frozen=True)
class SourceSpec:
    source: str
    owner: str
    role: str
    mode: str
    market_data: bool
    freshness_seconds: int | None
    budget_key: str
    provenance: str
    fallback: str
    cost: str
    coverage: str
    rate_limit: str


_REGISTRY: tuple[SourceSpec, ...] = (
    SourceSpec(
        "gate", "Gate.io", "candles/price/indicators/MTF/structure/scanner",
        "PRIMARY", True, 120, "gate", "Gate USD-M public market data",
        "fail-closed on stale or missing data", "existing exchange access",
        "configured Gate USD-M symbols", "bounded by local rolling budget and provider limits",
    ),
    SourceSpec(
        "gate_ws", "Gate.io", "order-book/trades microstructure",
        "SHADOW", True, 5, "gate", "Gate WebSocket order book/trades",
        "Gate REST snapshot or no feature", "existing exchange access",
        "explicitly enabled symbols only", "shares Gate budget; no scanner use",
    ),
    SourceSpec(
        "live_market_tape", "Gate.io (optional WS)", "bounded trades/liquidations tape",
        "CONTEXT", False, 120, "gate", "Gate public WebSocket tape; provider fallback is omitted",
        "empty context when disconnected", "existing exchange access",
        "explicitly enabled Gate symbols", "one bounded WS connection; no REST polling",
    ),
    SourceSpec(
        "public_futures", "public exchange fallback", "derivatives context fallback",
        "CONTEXT", False, 120, "custom", "normalized public futures endpoint",
        "omit context when unavailable", "provider cost/limits unknown",
        "provider-dependent", "local custom budget; provider limits must be verified",
    ),
    SourceSpec(
        "crypto_monitor", "public monitor", "OI/funding/liquidation context",
        "CONTEXT", False, 120, "custom", "provider-normalized public derivatives metrics",
        "omit context when unavailable", "provider cost/terms unknown",
        "provider-dependent", "local custom budget; provider limits must be verified",
    ),
    SourceSpec(
        "crypto_whale_tracker", "public tracker", "large-transfer context",
        "SHADOW", False, 900, "custom", "public tracker labels and transfers",
        "omit context when unavailable", "provider cost/terms unknown",
        "provider-dependent", "local custom budget; provider limits must be verified",
    ),
    SourceSpec(
        "deepbluealpha", "DeepBlueAlpha", "smart-money-labelled context",
        "SHADOW", False, 3600, "deepbluealpha", "provider-labelled wallet/flow data",
        "omit context when unavailable", "optional provider access; cost/terms unknown",
        "provider-dependent", "strict local custom allocation",
    ),
    SourceSpec(
        "open_labels_initiative", "Open Labels Initiative", "on-chain label context",
        "SHADOW", False, 86400, "mempool", "public labels; not institutional-flow proof",
        "omit context when unavailable", "public project; coverage/terms vary",
        "provider-dependent", "bounded local allocation",
    ),
    SourceSpec(
        "coinmetrics_community", "Coin Metrics", "network activity context",
        "CONTEXT", False, 21600, "coinmetrics", "Coin Metrics community API",
        "omit context when unavailable", "community endpoint coverage varies",
        "community-supported assets", "bounded local allocation",
    ),
    SourceSpec(
        "deribit_options", "Deribit", "options volatility context",
        "CONTEXT", False, 1800, "deribit", "Deribit public options data",
        "omit context when unavailable", "public endpoint; coverage varies",
        "Deribit options", "bounded local allocation",
    ),
    SourceSpec(
        "pair_registry", "APEX", "symbol/provenance mapping",
        "INTERNAL", False, None, "custom", "local provider-symbol registry",
        "do not request unsupported provider", "internal",
        "configured symbols", "no external request unless adapter refreshes",
    ),
    SourceSpec(
        "coinalyze", "Coinalyze", "open-interest/funding/liquidations",
        "SHADOW", False, 120, "coinalyze", "provider-normalized derivatives aggregates",
        "omit context when unavailable", "free/public key required; verify account terms",
        "provider symbol coverage", "documented provider cap; local cap is stricter",
    ),
    SourceSpec(
        "hyperliquid", "Hyperliquid", "derivatives context",
        "SHADOW", False, 120, "hyperliquid", "Hyperliquid public info endpoint",
        "omit context when unavailable", "public endpoint; verify current limits",
        "Hyperliquid instruments", "local rolling budget plus provider limits",
    ),
    SourceSpec(
        "coinmetrics", "Coin Metrics", "on-chain/reference context",
        "CONTEXT", False, 21600, "coinmetrics", "Coin Metrics community API",
        "omit context when unavailable", "community/public coverage varies",
        "supported community assets", "local rolling budget plus provider limits",
    ),
    SourceSpec(
        "defillama", "DefiLlama", "slow protocol/stablecoin regime context",
        "CONTEXT", False, 21600, "defillama", "DefiLlama public APIs",
        "omit context when unavailable", "public endpoints; attribution/terms apply",
        "supported protocols", "local rolling budget plus provider limits",
    ),
    SourceSpec(
        "deribit", "Deribit", "options volatility context",
        "CONTEXT", False, 1800, "deribit", "Deribit public market data",
        "omit context when unavailable", "public endpoint; instrument coverage varies",
        "Deribit options", "local rolling budget plus provider limits",
    ),
    SourceSpec(
        "mempool", "mempool.space", "Bitcoin on-chain context",
        "SHADOW", False, 900, "mempool", "public Bitcoin mempool API",
        "omit context when unavailable", "public endpoint; Bitcoin only",
        "Bitcoin network", "local rolling budget plus provider limits",
    ),
    SourceSpec(
        "dexscreener", "DEX Screener", "DEX liquidity context",
        "SHADOW", False, 1800, "dexscreener", "public DEX pair/market data",
        "omit context when unavailable", "public endpoint; chain/pair coverage varies",
        "indexed DEX pairs", "local rolling budget plus provider limits",
    ),
    SourceSpec(
        "news", "Fair Economy/CoinDesk/CoinTelegraph/Decrypt", "macro calendar/news risk",
        "CONTEXT", False, 900, "news", "public calendar and RSS feeds",
        "omit news context when unavailable", "public feeds; publisher terms apply",
        "calendar plus crypto headlines", "bounded local rolling budget; no directional prediction",
    ),
    SourceSpec(
        "bls", "U.S. Bureau of Labor Statistics", "official macro actuals",
        "CONTEXT", False, 21600, "bls", "BLS public API v1 published observations",
        "omit official actuals when unavailable", "public API; attribution/terms apply",
        "configured U.S. series", "bounded local rolling budget plus provider limits",
    ),
    SourceSpec(
        "binance", "Binance", "validated execution only",
        "EXECUTION", False, None, "execution", "manager-authorized exchange order state",
        "preserve existing protection and enter reconciliation", "configured account fees",
        "configured futures account", "bounded manager reconciliation; never market-data polling",
    ),
)

REGISTRY: Mapping[str, SourceSpec] = {item.source: item for item in _REGISTRY}
SOURCE_ALIASES: Mapping[str, str] = {
    "coinalyze_shadow": "coinalyze", "coinmetrics_community": "coinmetrics",
    "deribit_options": "deribit", "btc_mempool": "mempool",
    "open_labels_initiative": "open_labels_initiative", "dexscreener": "dexscreener",
}


def get_source(source: str) -> SourceSpec:
    key = str(source or "").strip().lower()
    key = SOURCE_ALIASES.get(key, key)
    try:
        return REGISTRY[key]
    except KeyError as exc:
        raise SourcePolicyError(f"unknown_source:{key or 'empty'}") from exc


def validate_source_usage(source: str, purpose: str) -> bool:
    """Validate a source/purpose pair without making any network call.

    ``market_data`` includes candles, scanner rows, indicators, MTF and
    structure.  ``execution`` is only valid for Binance after Manager V2 has
    validated the command.  Optional sources are context/shadow and therefore
    cannot become a strategy gate through this contract.
    """
    spec = get_source(source)
    requested = str(purpose or "").strip().lower()
    market_purposes = {
        "market_data", "candles", "price", "indicators", "mtf", "structure", "scanner",
    }
    if requested in market_purposes:
        if spec.source != "gate":
            raise SourcePolicyError(f"market_data_must_use_gate:{spec.source}")
        return True
    if requested in {"execution", "order", "validated_execution"}:
        if spec.source != "binance":
            raise SourcePolicyError(f"execution_must_use_binance:{spec.source}")
        return True
    if requested in {"context", "shadow", "microstructure"}:
        if spec.source == "binance":
            raise SourcePolicyError("binance_context_forbidden")
        return True
    raise SourcePolicyError(f"unknown_source_purpose:{requested or 'empty'}")


def registry_snapshot() -> list[dict[str, Any]]:
    """Return a secret-free, deterministic view for Dashboard V2."""
    return [asdict(item) for item in _REGISTRY]


def source_contract(source: str) -> dict[str, Any]:
    """Return the adapter contract used by tests and diagnostics."""
    item = get_source(source)
    return {
        "source": item.source,
        "mode": item.mode,
        "market_data": item.market_data,
        "freshness_seconds": item.freshness_seconds,
        "budget_key": item.budget_key,
        "provenance": item.provenance,
        "fallback": item.fallback,
        "can_influence_entry": item.source == "gate",
        "can_execute": item.source == "binance",
    }


__all__ = [
    "REGISTRY", "SOURCE_ALIASES", "SourcePolicyError", "SourceSpec", "get_source",
    "registry_snapshot", "source_contract", "validate_source_usage",
]
