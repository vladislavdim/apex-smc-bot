"""Bounded public news/calendar readers used by the news-risk aggregator."""

from __future__ import annotations

import asyncio
import json
import os
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.request import Request, urlopen

from external_sources.cache import cache


CALENDAR_URL = os.environ.get(
    "MACRO_CALENDAR_URL",
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
)
RSS_SOURCES = (
    ("https://cointelegraph.com/rss", "CoinTelegraph"),
    ("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk"),
    ("https://decrypt.co/feed", "Decrypt"),
)


def _fetch_text_sync(url: str, timeout: float = 7.0) -> str:
    req = Request(url, headers={"User-Agent": "APEX-SMC-Bot/news-context"})
    with urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


async def fetch_text(url: str, retries: int = 2) -> str:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return await asyncio.to_thread(_fetch_text_sync, url)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                await asyncio.sleep(0.25 * (2**attempt) + random.uniform(0, 0.1))
    raise RuntimeError(type(last_error).__name__ if last_error else "request_failed")


def _child_text(node: ET.Element, *names: str) -> str:
    wanted = {name.lower() for name in names}
    for child in list(node):
        tag = child.tag.rsplit("}", 1)[-1].lower()
        if tag in wanted and child.text:
            return child.text.strip()
    return ""


def parse_calendar_xml(raw: str) -> list[dict[str, Any]]:
    root = ET.fromstring(raw)
    events: list[dict[str, Any]] = []
    for node in root.iter():
        if node.tag.rsplit("}", 1)[-1].lower() not in {"event", "item"}:
            continue
        title = _child_text(node, "title", "event")
        if not title:
            continue
        events.append({
            "title": title,
            "country": _child_text(node, "country"),
            "date": _child_text(node, "date"),
            "time": _child_text(node, "time"),
            "impact": _child_text(node, "impact"),
            "forecast": _child_text(node, "forecast"),
            "previous": _child_text(node, "previous"),
            "actual": _child_text(node, "actual"),
            "url": _child_text(node, "url", "link"),
            "source": "ForexFactory",
        })
    return events


def parse_calendar_payload(raw: str) -> list[dict[str, Any]]:
    """Accept the Fair Economy JSON feed and the legacy ForexFactory XML feed."""
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return parse_calendar_xml(raw)
    rows = payload if isinstance(payload, list) else payload.get("events", []) if isinstance(payload, dict) else []
    events = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("title"):
            continue
        events.append({
            "title": str(row.get("title", "")), "country": str(row.get("country", "")),
            "date": str(row.get("date", "")), "time": str(row.get("time", "")),
            "impact": str(row.get("impact", "")), "forecast": str(row.get("forecast", "") or ""),
            "previous": str(row.get("previous", "") or ""), "actual": str(row.get("actual", "") or ""),
            "url": str(row.get("url", "")), "source": "ForexFactory/FairEconomy",
        })
    return events


def parse_rss_xml(raw: str, source: str) -> list[dict[str, Any]]:
    root = ET.fromstring(raw)
    items: list[dict[str, Any]] = []
    for node in root.iter():
        if node.tag.rsplit("}", 1)[-1].lower() not in {"item", "entry"}:
            continue
        title = _child_text(node, "title")
        if not title:
            continue
        published = _child_text(node, "pubdate", "published", "updated")
        age_seconds = None
        if published:
            try:
                dt = parsedate_to_datetime(published)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age_seconds = max(0, int((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()))
            except Exception:
                pass
        items.append({
            "title": title[:300],
            "url": _child_text(node, "link")[:500],
            "published": published[:80],
            "age_seconds": age_seconds,
            "source": source,
        })
    return items


async def collect_calendar() -> tuple[list[dict[str, Any]], str, int | None]:
    async def _load() -> list[dict[str, Any]]:
        return parse_calendar_payload(await fetch_text(CALENDAR_URL))

    return await cache.get_or_fetch("news:macro-calendar", 900, 21600, _load)


async def collect_headlines() -> tuple[list[dict[str, Any]], str, int | None]:
    async def _load() -> list[dict[str, Any]]:
        responses = await asyncio.gather(
            *(fetch_text(url) for url, _ in RSS_SOURCES), return_exceptions=True
        )
        rows: list[dict[str, Any]] = []
        for response, (_, source) in zip(responses, RSS_SOURCES):
            if isinstance(response, Exception):
                continue
            try:
                rows.extend(parse_rss_xml(response, source))
            except Exception:
                continue
        if not rows:
            raise ValueError("empty_headlines")
        rows.sort(key=lambda row: row.get("age_seconds") if row.get("age_seconds") is not None else 10**12)
        return rows[:60]

    return await cache.get_or_fetch("news:crypto-rss", 300, 1800, _load)
