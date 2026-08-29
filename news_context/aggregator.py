"""Normalize news into risk context; never creates a signal or trade level."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .sources import collect_calendar, collect_headlines
from .official_macro import collect as collect_official_actuals


_CRITICAL = (
    "consumer price index", "core cpi", "cpi", "employment situation",
    "non-farm", "nonfarm", "unemployment rate", "fomc", "federal funds rate",
    "fed chair", "powell", "gross domestic product", "gdp", "core pce",
    "personal consumption expenditures", "producer price index", "ppi",
    "ism manufacturing", "ism services", "pmi", "retail sales",
    "jolts", "jobless claims", "central bank decision",
)
_GLOBAL_NEWS = (
    "bitcoin", "crypto", "federal reserve", "fed ", "interest rate", "inflation",
    "cpi", "gdp", "recession", "tariff", "war", "sanction", "etf", "sec ",
    "hack", "exploit", "bankruptcy", "liquidation",
)


def _event_timestamp(event: dict[str, Any], now: datetime) -> datetime | None:
    date_text, time_text = str(event.get("date", "")).strip(), str(event.get("time", "")).strip()
    if date_text and "T" in date_text:
        try:
            parsed = datetime.fromisoformat(date_text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("America/New_York"))
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    if not date_text or not time_text or time_text.lower() in {"all day", "tentative"}:
        return None
    for date_fmt in ("%m-%d-%Y", "%m/%d/%Y", "%Y-%m-%d"):
        for time_fmt in ("%I:%M%p", "%I%p", "%H:%M"):
            try:
                local = datetime.strptime(f"{date_text} {time_text.replace(' ', '')}", f"{date_fmt} {time_fmt}")
                return local.replace(tzinfo=ZoneInfo("America/New_York")).astimezone(timezone.utc)
            except ValueError:
                continue
    return None


def _is_critical(event: dict[str, Any]) -> bool:
    title = str(event.get("title", "")).lower()
    impact = str(event.get("impact", "")).lower()
    country = str(event.get("country", "")).upper()
    global_scope = not country or country in {"USD", "US", "ALL", "GLOBAL"}
    return global_scope and any(word in title for word in _CRITICAL) and impact not in {"low", "holiday"}


def _phase(minutes: int) -> str:
    if -60 <= minutes <= 30:
        return "RELEASE_WINDOW"
    if 30 < minutes <= 180:
        return "PRE_EVENT"
    if -360 <= minutes < -60:
        return "POST_EVENT"
    return "NORMAL"


def _relevant_headlines(rows: list[dict[str, Any]], symbol: str) -> list[dict[str, Any]]:
    base = symbol.upper().replace("USDT", "").replace("USD", "")
    aliases = {base.lower()}
    if base == "BTC": aliases.add("bitcoin")
    if base == "ETH": aliases.add("ethereum")
    result = []
    for row in rows:
        title = str(row.get("title", "")).lower()
        age = row.get("age_seconds")
        if age is not None and age > 21600:
            continue
        if any(term in title for term in _GLOBAL_NEWS) or any(alias and alias in title for alias in aliases):
            result.append(row)
    return result[:8]


def normalize_news_context(
    symbol: str,
    calendar: list[dict[str, Any]],
    headlines: list[dict[str, Any]],
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    events = []
    for event in calendar:
        if not _is_critical(event):
            continue
        timestamp = _event_timestamp(event, now)
        if timestamp is None:
            continue
        minutes = int((timestamp - now).total_seconds() / 60)
        if -360 <= minutes <= 1440:
            item = dict(event)
            item.update({"timestamp": timestamp.isoformat(), "minutes_until": minutes, "phase": _phase(minutes)})
            events.append(item)
    events.sort(key=lambda row: abs(row["minutes_until"]))
    nearest = events[0] if events else None
    phase = nearest["phase"] if nearest else "NORMAL"
    risk = "HIGH" if nearest and phase == "RELEASE_WINDOW" else "HIGH" if nearest and 0 <= nearest["minutes_until"] <= 90 else "MEDIUM" if nearest else "LOW"
    relevant = _relevant_headlines(headlines, symbol)
    if any(any(word in row["title"].lower() for word in ("hack", "exploit", "bankruptcy", "war")) for row in relevant):
        risk = "HIGH"
    return {
        "symbol": symbol.upper(),
        "timestamp": now.isoformat(),
        "risk_level": risk,
        "phase": phase,
        "nearest_critical_event": nearest,
        "critical_events": events[:5],
        "headlines": relevant,
        "prediction": "not_available_pre_release" if nearest and nearest["minutes_until"] > 0 else "no_directional_prediction",
        "rules": [
            "Scheduled events imply volatility risk, not a known direction.",
            "Do not infer actual/forecast values when absent.",
            "News cannot create a trade without a valid technical candidate.",
        ],
        "news_data_unavailable": not bool(calendar or headlines),
        "data_quality": {"available_sources": [], "failed_sources": [], "age_seconds": None},
    }


async def collect_news_context(symbol: str) -> dict[str, Any]:
    raw = await asyncio.gather(
        collect_calendar(), collect_headlines(), collect_official_actuals(),
        return_exceptions=True,
    )
    calendar: list[dict[str, Any]] = []
    headlines: list[dict[str, Any]] = []
    statuses: list[tuple[str, str, int | None]] = []
    failed: list[str] = []
    official_actuals: dict[str, Any] = {}
    if isinstance(raw[0], Exception):
        failed.append(f"macro_calendar:{type(raw[0]).__name__}")
    else:
        calendar, status, age = raw[0]
        statuses.append(("macro_calendar", status, age))
    if isinstance(raw[1], Exception):
        failed.append(f"crypto_rss:{type(raw[1]).__name__}")
    else:
        headlines, status, age = raw[1]
        statuses.append(("crypto_rss", status, age))
    if isinstance(raw[2], Exception):
        failed.append(f"bls_official_actuals:{type(raw[2]).__name__}")
    elif isinstance(raw[2], dict) and raw[2].get("status") in {"fresh", "cached", "stale_fallback"}:
        official_actuals = raw[2].get("actuals", {})
        statuses.append(("bls_official_actuals", raw[2].get("status"), raw[2].get("age_seconds")))
    elif not isinstance(raw[2], dict):
        failed.append("bls_official_actuals:invalid_response")
    else:
        failed.append(f"bls_official_actuals:{raw[2].get('status', 'unavailable')}")
    context = normalize_news_context(symbol, calendar, headlines)
    context["official_macro_actuals"] = official_actuals
    context["data_quality"] = {
        "available_sources": [name for name, _, _ in statuses],
        "failed_sources": failed,
        "age_seconds": max((age or 0 for _, _, age in statuses), default=None),
        "source_status": {name: status for name, status, _ in statuses},
    }
    context["news_data_unavailable"] = not bool(statuses)
    return context


def format_news_context(context: dict[str, Any]) -> str:
    event = context.get("nearest_critical_event") or {}
    headlines = [f"{row.get('source')}: {row.get('title')}" for row in context.get("headlines", [])[:5]]
    quality = context.get("data_quality", {})
    return "\n".join([
        "NEWS RISK CONTEXT:",
        f"- Risk level: {context.get('risk_level', 'UNKNOWN')}",
        f"- Phase: {context.get('phase', 'NORMAL')}",
        f"- Critical event: {event.get('title', 'none')}",
        f"- Minutes until event: {event.get('minutes_until', 'n/a')}",
        f"- Forecast / previous / actual: {event.get('forecast') or 'unknown'} / {event.get('previous') or 'unknown'} / {event.get('actual') or 'unknown'}",
        f"- Official published macro actuals (BLS): {context.get('official_macro_actuals') or 'unavailable'}",
        f"- Headlines: {headlines or 'none'}",
        f"- Prediction: {context.get('prediction')}",
        f"- Data quality: available={quality.get('available_sources', [])}; failed={quality.get('failed_sources', [])}; age={quality.get('age_seconds')}",
        "Instructions: scheduled macro news is volatility risk, not a directional prediction. Official BLS observations may be delayed or revised and describe published history, not the next release. Never invent the release value or market reaction. News is an additional quality filter and cannot independently create a trade.",
    ])
