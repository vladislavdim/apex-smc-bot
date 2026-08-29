"""Aggregate external evidence without calculating trade levels or verdicts."""

from __future__ import annotations

import asyncio
from typing import Any

from . import (btc_mempool, crypto_monitor, defillama, exchange_fallback,
               hyperliquid, live_tape, oli, smart_money, whale_tracker)
from .models import empty_context, number
from .pair_registry import get_pair, refresh_pair_registry


# Maximum age at which a value may influence Groq. Adapters may retain an
# older last-known-good payload for diagnostics, but stale values are excluded
# from normalized fields and voting here.
_MAX_USABLE_AGE = {
    exchange_fallback.SOURCE: 120,
    crypto_monitor.SOURCE: 120,
    whale_tracker.SOURCE: 900,
    smart_money.SOURCE: 3600,
    live_tape.SOURCE: 120, hyperliquid.SOURCE: 120,
    btc_mempool.SOURCE: 900, oli.SOURCE: 86400, defillama.SOURCE: 21600,
}
_COLLECT_TIMEOUT_SECONDS = 10.0


async def _bounded_collect(source: str, awaitable: Any) -> dict[str, Any]:
    """Keep one slow provider from delaying the scanner beyond its budget."""
    try:
        result = await asyncio.wait_for(awaitable, timeout=_COLLECT_TIMEOUT_SECONDS)
        if isinstance(result, dict):
            return result
        return {"source": source, "status": "unavailable", "error": "invalid_response"}
    except Exception as exc:
        return {
            "source": source,
            "status": "unavailable",
            "error": type(exc).__name__,
        }


def _bias(buy: float | None, sell: float | None) -> str:
    if buy is None or sell is None or buy + sell <= 0:
        return "unknown"
    ratio = buy / (buy + sell)
    return "bullish" if ratio > 0.56 else "bearish" if ratio < 0.44 else "neutral"


def _age(result: dict[str, Any]) -> int | None:
    value = result.get("age_seconds")
    try:
        return max(0, int(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def _usable(result: dict[str, Any]) -> bool:
    state = result.get("status")
    if state not in {"fresh", "cached", "stale_fallback"}:
        return False
    age = _age(result)
    maximum = _MAX_USABLE_AGE.get(str(result.get("source")), 120)
    return age is None or age <= maximum


def _status(context: dict[str, Any], result: dict[str, Any]) -> None:
    source = str(result.get("source", "unknown"))
    state = str(result.get("status", "unknown"))
    age = _age(result)
    usable = _usable(result)
    reported = state if usable else (
        f"stale_excluded:{age}s" if state == "stale_fallback" else state
    )
    context["data_quality"]["source_status"][source] = reported
    context["data_quality"]["source_ages"][source] = age
    target = "available_sources" if usable else "failed_sources"
    value = source if usable else f"{source}:{reported}"
    if value not in context["data_quality"][target]:
        context["data_quality"][target].append(value)


def _metadata(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": result.get("source"),
        "status": result.get("status"),
        "age_seconds": _age(result),
    }

def _source_value(target: dict[str, Any], result: dict[str, Any], values: dict[str, Any]) -> None:
    target.setdefault("source_values", {})[str(result.get("source", "unknown"))] = {
        **values, "status": result.get("status"), "age_seconds": _age(result)}


def _apply_futures(context: dict[str, Any], result: dict[str, Any]) -> None:
    if not _usable(result):
        return
    data = result.get("normalized", {})
    if not isinstance(data, dict) or not data:
        return
    meta = _metadata(result)

    oi_values = (data.get("oi"), data.get("oi_1h"), data.get("oi_4h"))
    if any(value is not None for value in oi_values):
        target = context["open_interest"]
        _source_value(target, result, {"value": data.get("oi"), "change_1h_pct": data.get("oi_1h"), "change_4h_pct": data.get("oi_4h")})
        for key, value in (
            ("value", data.get("oi")),
            ("change_1h_pct", data.get("oi_1h")),
            ("change_4h_pct", data.get("oi_4h")),
        ):
            if value is not None:
                target[key] = value
        change = target.get("change_1h_pct")
        target["trend"] = (
            "rising" if change is not None and change > 0
            else "falling" if change is not None and change < 0
            else "flat" if change == 0 else "unknown"
        )
        target.update(meta)

    rate = data.get("funding")
    if rate is not None:
        _source_value(context["funding"], result, {"rate": rate})
        context["funding"].update({
            "rate": rate,
            "extreme": abs(rate) >= 0.001,
            "bias": "crowded_longs" if rate > 0 else "crowded_shorts" if rate < 0 else "neutral",
            **meta,
        })

    long_liq = number(data.get("long_liq"))
    short_liq = number(data.get("short_liq"))
    if long_liq is not None or short_liq is not None:
        long_liq, short_liq = long_liq or 0.0, short_liq or 0.0
        _source_value(context["liquidations"], result, {"long_usd": long_liq, "short_usd": short_liq})
        context["liquidations"].update({
            "long_usd": long_liq,
            "short_usd": short_liq,
            "dominance": (
                "long" if long_liq > short_liq
                else "short" if short_liq > long_liq else "unknown"
            ),
            **meta,
        })

    buy, sell = number(data.get("buy")), number(data.get("sell"))
    if buy is not None or sell is not None:
        _source_value(context["large_orders"], result, {
            "buy_pressure": buy, "sell_pressure": sell,
            "bias": _bias(buy, sell), "method": data.get("order_flow_method"),
        })
        context["large_orders"].update({
            "buy_pressure": buy,
            "sell_pressure": sell,
            "bias": _bias(buy, sell),
            "method": data.get("order_flow_method"),
            **meta,
        })


def _apply_whale(context: dict[str, Any], result: dict[str, Any]) -> None:
    if not _usable(result):
        return
    payload = result.get("payload")
    if not isinstance(payload, dict):
        return
    items = payload.get("items", payload.get("data", payload.get("transactions", [])))
    if not isinstance(items, list):
        return
    inflow = outflow = 0.0
    matched = 0
    for row in items:
        if not isinstance(row, dict):
            continue
        amount = number(row.get("usd_value", row.get("value_usd", 0))) or 0
        direction = str(row.get("direction", "")).lower()
        if direction in {"to_exchange", "inflow"}:
            inflow += amount
            matched += 1
        elif direction in {"from_exchange", "outflow"}:
            outflow += amount
            matched += 1
    total = inflow + outflow
    if total <= 0:
        return
    confidence = min(
        0.85,
        0.25 + abs(outflow - inflow) / total * 0.45 + min(matched, 20) / 100,
    )
    meta = _metadata(result)
    flow_bias = "bearish" if inflow > outflow else "bullish" if outflow > inflow else "neutral"
    context["exchange_flow"].update({
        "inflow_usd": inflow or None,
        "outflow_usd": outflow or None,
        "bias": flow_bias,
        **meta,
    })
    context["whale_activity"].update({
        "buy_usd": outflow or None,
        "sell_usd": inflow or None,
        "bias": _bias(outflow, inflow),
        "confidence": round(confidence, 2),
        **meta,
    })


def _apply_smart_money(context: dict[str, Any], result: dict[str, Any]) -> None:
    if not _usable(result):
        return
    data = result.get("normalized")
    if not isinstance(data, dict):
        return
    buy, sell = number(data.get("buy_usd")), number(data.get("sell_usd"))
    if buy is None or sell is None or buy + sell <= 0:
        return
    total = buy + sell
    count = number(data.get("transaction_count")) or 0
    imbalance = abs(buy - sell) / total
    confidence = min(0.9, imbalance * 0.65 + min(count, 20) / 80)
    context["smart_money"].update({
        "buy_usd": buy,
        "sell_usd": sell,
        "bias": _bias(buy, sell),
        "confidence": round(confidence, 2),
        "top_wallets": data.get("top_wallets") or [],
        "method": data.get("method"),
        **_metadata(result),
    })

def _apply_live_tape(context: dict[str, Any], result: dict[str, Any]) -> None:
    if not _usable(result) or not isinstance(result.get("normalized"), dict): return
    data = result["normalized"]
    buy, sell = number(data.get("buy_usd_60s")), number(data.get("sell_usd_60s"))
    long_liq, short_liq = number(data.get("long_liq_usd_300s")) or 0.0, number(data.get("short_liq_usd_300s")) or 0.0
    context["live_tape"].update({"buy_usd_60s": buy or 0.0, "sell_usd_60s": sell or 0.0,
        "long_liq_usd_300s": long_liq, "short_liq_usd_300s": short_liq,
        "bias": _bias(buy, sell), "sources": sorted((data.get("sources") or {}).keys()),
        "source_values": data.get("sources") or {}, "age_seconds": _age(result), "status": result.get("status")})
    if buy is not None or sell is not None:
        _source_value(context["large_orders"], result, {
            "buy_pressure": buy, "sell_pressure": sell,
            "bias": _bias(buy, sell), "method": "public_websocket_taker_flow_60s",
        })
        context["large_orders"].update({"buy_pressure": buy, "sell_pressure": sell, "bias": _bias(buy, sell), "method": "public_websocket_taker_flow_60s", **_metadata(result)})
    if long_liq or short_liq:
        _source_value(context["liquidations"], result, {"long_usd": long_liq, "short_usd": short_liq})
        context["liquidations"].update({"long_usd": long_liq, "short_usd": short_liq,
            "dominance": "long" if long_liq > short_liq else "short" if short_liq > long_liq else "unknown", **_metadata(result)})
    sources = data.get("sources") or {}
    ois = [number(row.get("oi")) for row in sources.values() if isinstance(row, dict) and number(row.get("oi")) is not None]
    fundings = [number(row.get("funding")) for row in sources.values() if isinstance(row, dict) and number(row.get("funding")) is not None]
    if ois:
        value = sum(ois); _source_value(context["open_interest"], result, {"value": value}); context["open_interest"].update({"value": value, **_metadata(result)})
    if fundings:
        rate = sum(fundings) / len(fundings); _source_value(context["funding"], result, {"rate": rate})
        context["funding"].update({"rate": rate, "extreme": abs(rate) >= 0.001, "bias": "crowded_longs" if rate > 0 else "crowded_shorts" if rate < 0 else "neutral", **_metadata(result)})

def _apply_onchain(context, result):
    if not _usable(result) or not isinstance(result.get("normalized"), dict): return
    data, target, source = result["normalized"], context["onchain_activity"], str(result.get("source"))
    if source == btc_mempool.SOURCE:
        target.update({"btc_large_transfers_usd": data.get("large_transfer_usd"), "btc_large_transfer_count": data.get("large_transfer_count", 0)})
    elif source == oli.SOURCE: target["oli_labels"] = data.get("labels") or []
    if source not in target["sources"]: target["sources"].append(source)
    target.update({"status": result.get("status"), "age_seconds": _age(result)})

def _apply_slow_regime(context, result):
    if _usable(result) and isinstance(result.get("normalized"), dict): context["slow_regime"].update({**result["normalized"], **_metadata(result)})


def _finish(context: dict[str, Any], direction: str | None) -> dict[str, Any]:
    quality = context["data_quality"]
    usable_ages = [
        age for source, age in quality["source_ages"].items()
        if source in quality["available_sources"] and age is not None
    ]
    # The oldest used source is the conservative summary age; exact ages remain
    # available both per field and per source.
    quality["age_seconds"] = max(usable_ages) if usable_ages else None

    # One directional vote per independent source. Exchange flow and whale
    # activity from the same tracker must not be double-counted.
    source_votes: dict[str, str] = {}

    def add_vote(source: str | None, bias: str | None) -> None:
        if not source or bias not in {"bullish", "bearish"}:
            return
        previous = source_votes.get(source)
        if previous and previous != bias:
            conflict = f"CONFLICT: {source} has contradictory normalized fields"
            if conflict not in context["conflicts"]:
                context["conflicts"].append(conflict)
            source_votes.pop(source, None)
        elif source not in source_votes:
            source_votes[source] = bias

    for field_name in ("large_orders", "exchange_flow", "whale_activity", "smart_money"):
        field = context[field_name]
        source_values = field.get("source_values")
        if isinstance(source_values, dict) and source_values:
            for source, values in source_values.items():
                if isinstance(values, dict):
                    add_vote(source, values.get("bias"))
        else:
            add_vote(field.get("source"), field.get("bias"))

    bullish = sum(v == "bullish" for v in source_votes.values())
    bearish = sum(v == "bearish" for v in source_votes.values())
    if bullish and bearish:
        context["external_bias"] = "neutral"
        context["conflicts"].append("CONFLICT: independent external sources disagree")
    elif bullish:
        context["external_bias"] = "bullish"
    elif bearish:
        context["external_bias"] = "bearish"
    else:
        context["external_bias"] = "unknown"

    directional_sources = bullish + bearish
    context["external_confidence"] = round(
        min(1.0, directional_sources / 3)
        if directional_sources >= 2 else (0.25 if directional_sources else 0.0),
        2,
    )
    if direction:
        expected = "bullish" if direction.upper() == "BULLISH" else "bearish"
        if (
            context["external_bias"] not in {"unknown", "neutral", expected}
            and directional_sources >= 2
        ):
            context["conflicts"].append(
                f"CONFLICT: technical {expected} candidate vs external {context['external_bias']} bias"
            )
    quality["available_sources"].sort()
    quality["failed_sources"].sort()
    context["external_data_unavailable"] = not bool(quality["available_sources"])
    return context


async def collect_external_context(symbol: str, direction: str | None = None) -> dict[str, Any]:
    symbol = (symbol or "").upper().replace("/", "")
    context = empty_context(symbol)
    await _bounded_collect("pair_registry", refresh_pair_registry([symbol]))
    pair = get_pair(symbol)
    context["pair_coverage"] = {provider: {"supported": bool(pair.get(f"{provider}_supported")),
        "status": pair.get(f"{provider}_status", "unverified"), "symbol": pair.get(f"{provider}_symbol")}
        for provider in ("gate", "binance", "bybit", "hyperliquid")}
    raw_results = await asyncio.gather(
        _bounded_collect(exchange_fallback.SOURCE, exchange_fallback.collect(symbol)),
        _bounded_collect(crypto_monitor.SOURCE, crypto_monitor.collect(symbol)),
        _bounded_collect(whale_tracker.SOURCE, whale_tracker.collect(symbol)),
        _bounded_collect(smart_money.SOURCE, smart_money.collect(symbol)),
        _bounded_collect(hyperliquid.SOURCE, hyperliquid.collect(symbol)),
        _bounded_collect(live_tape.SOURCE, live_tape.collect(symbol)),
        _bounded_collect(btc_mempool.SOURCE, btc_mempool.collect(symbol)),
        _bounded_collect(oli.SOURCE, oli.collect(symbol)),
        _bounded_collect(defillama.SOURCE, defillama.collect(symbol)),
    )
    normalized_results: list[dict[str, Any]] = []
    for raw in raw_results:
        if isinstance(raw, Exception):
            context["data_quality"]["failed_sources"].append(
                f"internal:{type(raw).__name__}"
            )
            continue
        if raw.get("source") == exchange_fallback.SOURCE:
            result = exchange_fallback.normalize(raw)
        elif raw.get("source") == crypto_monitor.SOURCE:
            result = crypto_monitor.normalize(raw, symbol)
        elif raw.get("source") == smart_money.SOURCE:
            result = smart_money.normalize(raw)
        else:
            result = raw
        normalized_results.append(result)
        _status(context, result)

    for result in normalized_results:
        if result.get("source") == hyperliquid.SOURCE: _apply_futures(context, result)
    for result in normalized_results:
        if result.get("source") == exchange_fallback.SOURCE:
            _apply_futures(context, result)
    for result in normalized_results:
        source = result.get("source")
        if source == crypto_monitor.SOURCE:
            _apply_futures(context, result)
        elif source == whale_tracker.SOURCE:
            _apply_whale(context, result)
        elif source == smart_money.SOURCE:
            _apply_smart_money(context, result)
        elif source == live_tape.SOURCE: _apply_live_tape(context, result)
        elif source in {btc_mempool.SOURCE, oli.SOURCE}: _apply_onchain(context, result)
        elif source == defillama.SOURCE: _apply_slow_regime(context, result)

    context["_source_results"] = normalized_results
    return _finish(context, direction)


def _line(label: str, field: dict[str, Any], keys: tuple[str, ...]) -> str:
    values = ", ".join(f"{key}={field.get(key)}" for key in keys)
    return (
        f"- {label}: {values}; source={field.get('source')}; "
        f"age={field.get('age_seconds')}s; status={field.get('status')}"
    )


def format_external_context(context: dict[str, Any], strategy: str | None = None) -> str:
    quality = context["data_quality"]
    lines = [
        "EXTERNAL MARKET CONTEXT:",
        _line("Open Interest", context["open_interest"], ("value", "change_1h_pct", "change_4h_pct", "trend")),
        _line("Funding", context["funding"], ("rate", "extreme", "bias")),
        _line("Liquidations", context["liquidations"], ("long_usd", "short_usd", "dominance")),
        _line("Large Orders", context["large_orders"], ("buy_pressure", "sell_pressure", "bias", "method")),
        _line("Exchange Flow", context["exchange_flow"], ("inflow_usd", "outflow_usd", "bias")),
        _line("Whale Activity", context["whale_activity"], ("buy_usd", "sell_usd", "bias", "confidence")),
        _line("Smart Money", context["smart_money"], ("buy_usd", "sell_usd", "bias", "confidence", "method")),
        _line("Live Market Tape", context["live_tape"], ("buy_usd_60s", "sell_usd_60s", "long_liq_usd_300s", "short_liq_usd_300s", "bias")),
        f"- On-chain Activity: {context['onchain_activity']}",
        f"- Slow Regime (DefiLlama): {context['slow_regime']}",
        f"- Pair coverage: {context['pair_coverage']}",
        f"- Data age: {quality['age_seconds']} seconds",
        f"- Source quality: available={quality['available_sources']}; failed={quality['failed_sources']}; status={quality['source_status']}",
        f"- Conflicts: {context['conflicts'] or 'none'}",
    ]
    if context.get("external_data_unavailable"):
        lines.append("- Availability: external_data_unavailable")
    lines.append(
        "Instructions: external data is a quality filter, never an independent signal. "
        "Do not invent missing values. Do not approve solely from external bias. "
        "Rising OI confirms only when it agrees with the supplied technical price movement; "
        "rising OI against that movement is a squeeze warning. Funding extremes are warnings, "
        "not automatic bans. Read liquidations by dominant liquidated side and exchange flows "
        "as risk context. Use whale/smart-money evidence only when both fresh and sufficiently "
        "confident. A material technical/external conflict requires valid=false with a clear "
        "logged reason."
    )
    if strategy:
        hints = {
            "MTF": "Check external context against HTF direction.",
            "SWING": "Use OI, liquidations and large orders only as sweep/reversal confirmation.",
            "ZONE": "Treat funding and exchange flow as risk context.",
            "FAST": "Use only fresh OI, funding, liquidations and order-flow; ignore whale/smart-money fields.",
            "WYCKOFF": "Use exchange flow, whale activity and smart-money accumulation only when fresh.",
        }
        lines.append(
            f"- Strategy rule ({strategy.upper()}): "
            f"{hints.get(strategy.upper(), 'Use only as additional context.')}"
        )
    return "\n".join(lines)
