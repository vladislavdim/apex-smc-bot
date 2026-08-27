"""Aggregate external evidence only; it never calculates trade levels or verdicts."""

from __future__ import annotations

import asyncio
from typing import Any

from . import crypto_monitor, exchange_fallback, smart_money, whale_tracker
from .models import empty_context, number


def _bias(buy: float | None, sell: float | None) -> str:
    if buy is None or sell is None or buy + sell <= 0:
        return "unknown"
    ratio = buy / (buy + sell)
    return "bullish" if ratio > 0.56 else "bearish" if ratio < 0.44 else "neutral"


def _status(context: dict[str, Any], result: dict[str, Any]) -> None:
    source, state = result["source"], result.get("status", "unknown")
    context["data_quality"]["source_status"][source] = state
    if state in {"fresh", "cached", "stale_fallback"}:
        context["data_quality"]["available_sources"].append(source)
    else:
        context["data_quality"]["failed_sources"].append(f"{source}:{state}")


def _apply_futures(context: dict[str, Any], result: dict[str, Any]) -> None:
    data = result.get("normalized", {})
    if not data:
        return
    source = result["source"]
    context["open_interest"].update({"value": data.get("oi"), "change_1h_pct": data.get("oi_1h"), "change_4h_pct": data.get("oi_4h"), "source": source})
    change = data.get("oi_1h")
    context["open_interest"]["trend"] = "rising" if change and change > 0 else "falling" if change and change < 0 else "flat" if change == 0 else "unknown"
    rate = data.get("funding")
    context["funding"].update({"rate": rate, "extreme": bool(rate is not None and abs(rate) >= 0.001), "bias": "bearish" if rate and rate > 0 else "bullish" if rate and rate < 0 else "neutral", "source": source})
    long_liq, short_liq = data.get("long_liq", 0), data.get("short_liq", 0)
    context["liquidations"].update({"long_usd": long_liq, "short_usd": short_liq, "dominance": "long" if long_liq > short_liq else "short" if short_liq > long_liq else "unknown", "source": source})
    buy, sell = data.get("buy"), data.get("sell")
    context["large_orders"].update({"buy_pressure": buy, "sell_pressure": sell, "bias": _bias(buy, sell), "source": source})


def _apply_whale(context: dict[str, Any], result: dict[str, Any]) -> None:
    payload = result.get("payload")
    if not isinstance(payload, dict):
        return
    items = payload.get("items", payload.get("data", payload.get("transactions", [])))
    if not isinstance(items, list):
        return
    inflow = outflow = 0.0
    for row in items:
        if not isinstance(row, dict): continue
        amount = number(row.get("usd_value", row.get("value_usd", 0))) or 0
        direction = str(row.get("direction", "")).lower()
        if direction in {"to_exchange", "inflow"}: inflow += amount
        elif direction in {"from_exchange", "outflow"}: outflow += amount
    context["exchange_flow"].update({"inflow_usd": inflow or None, "outflow_usd": outflow or None, "bias": "bearish" if inflow > outflow else "bullish" if outflow > inflow else "neutral", "source": result["source"]})
    context["whale_activity"].update({"buy_usd": outflow or None, "sell_usd": inflow or None, "bias": _bias(outflow, inflow), "confidence": 0.65 if inflow + outflow else 0, "source": result["source"]})


def _apply_smart_money(context: dict[str, Any], result: dict[str, Any]) -> None:
    payload = result.get("payload")
    if not isinstance(payload, dict): return
    index = payload.get("index", payload.get("data", payload))
    if not isinstance(index, dict): return
    score = number(index.get("score", index.get("whale_index")))
    bias = "bullish" if score is not None and score >= 60 else "bearish" if score is not None and score <= 40 else "neutral"
    context["smart_money"].update({"bias": bias, "confidence": round(abs((score or 50) - 50) / 50, 2), "top_wallets": [], "source": result["source"]})


def _finish(context: dict[str, Any], direction: str | None) -> dict[str, Any]:
    ages = [v for v in context["data_quality"]["source_status"].values()]
    # field-level age is deliberately represented by the source status and summary age;
    # source adapters preserve exact cache age in source_events persistence.
    sources = context["data_quality"]["available_sources"]
    context["data_quality"]["age_seconds"] = 0 if sources else None
    votes = [context["large_orders"]["bias"], context["whale_activity"]["bias"], context["smart_money"]["bias"]]
    bullish, bearish = votes.count("bullish"), votes.count("bearish")
    context["external_bias"] = "bullish" if bullish > bearish else "bearish" if bearish > bullish else "neutral" if bullish or bearish else "unknown"
    context["external_confidence"] = round(min(1.0, (bullish + bearish) / 3), 2)
    if bullish and bearish:
        context["conflicts"].append("CONFLICT: independent external sources disagree")
    if direction:
        expected = "bullish" if direction.upper() == "BULLISH" else "bearish"
        if context["external_bias"] not in {"unknown", "neutral", expected} and context["external_confidence"] >= 0.34:
            context["conflicts"].append(f"CONFLICT: technical {expected} candidate vs external {context['external_bias']} bias")
    context["external_data_unavailable"] = not bool(sources)
    return context


async def collect_external_context(symbol: str, direction: str | None = None) -> dict[str, Any]:
    symbol = (symbol or "").upper().replace("/", "")
    context = empty_context(symbol)
    raw_results = await asyncio.gather(
        exchange_fallback.collect(symbol), crypto_monitor.collect(symbol), whale_tracker.collect(symbol), smart_money.collect(symbol),
        return_exceptions=True,
    )
    results = []
    for result in raw_results:
        if isinstance(result, Exception):
            context["data_quality"]["failed_sources"].append(f"internal:{type(result).__name__}")
            continue
        results.append(result)
        _status(context, result)
    for result in results:
        if result["source"] == exchange_fallback.SOURCE:
            _apply_futures(context, exchange_fallback.normalize(result))
        elif result["source"] == crypto_monitor.SOURCE:
            _apply_futures(context, crypto_monitor.normalize(result, symbol))
        elif result["source"] == whale_tracker.SOURCE:
            _apply_whale(context, result)
        elif result["source"] == smart_money.SOURCE:
            _apply_smart_money(context, result)
    context["_source_results"] = results
    return _finish(context, direction)


def format_external_context(context: dict[str, Any], strategy: str | None = None) -> str:
    q = context["data_quality"]
    lines = [
        "EXTERNAL MARKET CONTEXT:",
        f"- Open Interest: {context['open_interest']}",
        f"- Funding: {context['funding']}",
        f"- Liquidations: {context['liquidations']}",
        f"- Large Orders: {context['large_orders']}",
        f"- Exchange Flow: {context['exchange_flow']}",
        f"- Whale Activity: {context['whale_activity']}",
        f"- Smart Money: {context['smart_money']}",
        f"- Data age: {q['age_seconds']} seconds",
        f"- Source quality: available={q['available_sources']}; failed={q['failed_sources']}",
        f"- Conflicts: {context['conflicts'] or 'none'}",
        "Instructions: external data is a quality filter, never an independent signal. Do not invent missing values. Do not approve solely from bullish/bearish external bias. A material technical/external conflict requires valid=false.",
    ]
    if strategy:
        hints = {
            "MTF": "Check external context against HTF direction.", "SWING": "Use OI, liquidations and large orders only as sweep/reversal confirmation.",
            "ZONE": "Treat funding and exchange flow as risk context.", "FAST": "Use only fresh derivatives/order-flow data.",
            "WYCKOFF": "Use exchange flow, whale activity and smart-money accumulation only when fresh.",
        }
        lines.append(f"- Strategy rule ({strategy.upper()}): {hints.get(strategy.upper(), 'Use only as context.')}")
    return "\n".join(lines)
