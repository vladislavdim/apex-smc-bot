"""Bounded, read-only macro context adapters."""

from __future__ import annotations

import logging
import time


_fear_greed_history_cache: dict | None = None
_fear_greed_history_time = 0.0
_dxy_cache: dict = {}
_dxy_cache_time = 0.0
_economic_cache: str | None = None
_economic_cache_time = 0.0


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def get_fg_history() -> dict | None:
    global _fear_greed_history_cache, _fear_greed_history_time
    if time.time() - _fear_greed_history_time < 3600 and _fear_greed_history_cache:
        return _fear_greed_history_cache
    try:
        response = _request_get(
            "https://api.alternative.me/fng/?limit=7&format=json", timeout=8,
        )
        data = response.json().get("data", [])
        if not data:
            return None
        values = [int(item["value"]) for item in data]
        result = {
            "values": values,
            "avg7": round(sum(values) / len(values), 1),
            "trend": "IMPROVING" if values[0] > values[-1] else "WORSENING",
            "current": values[0],
        }
        _fear_greed_history_cache = result
        _fear_greed_history_time = time.time()
        return result
    except Exception as exc:
        logging.debug("F&G history: %s", exc)
        return None


def get_dxy_signal() -> dict | None:
    global _dxy_cache, _dxy_cache_time
    if time.time() - _dxy_cache_time < 3600 and _dxy_cache:
        return _dxy_cache
    try:
        response = _request_get(
            "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=5d",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
        )
        results = (response.json().get("chart") or {}).get("result") or []
        if not results:
            logging.debug("DXY: пустой ответ от Yahoo Finance")
            return None
        quote = (results[0].get("indicators") or {}).get("quote") or [{}]
        closes = [value for value in (quote[0] if quote else {}).get("close") or [] if value is not None]
        if len(closes) < 2:
            return None
        change = (closes[-1] - closes[-3]) / closes[-3] * 100 if len(closes) >= 3 else 0
        _dxy_cache = {
            "value": round(closes[-1], 2),
            "change": round(change, 2),
            "signal": "STRONG" if change > 0.3 else "WEAK" if change < -0.3 else "NEUTRAL",
        }
        _dxy_cache_time = time.time()
        return _dxy_cache
    except Exception as exc:
        logging.debug("DXY: %s", exc)
        return None


def get_upcoming_events() -> str:
    global _economic_cache, _economic_cache_time
    if time.time() - _economic_cache_time < 1800 and _economic_cache is not None:
        return _economic_cache
    high_impact = (
        "Federal Reserve", "Fed", "CPI", "NFP", "Non-Farm", "GDP",
        "Interest Rate", "Inflation", "FOMC", "Powell", "SEC", "ECB",
    )
    try:
        items = []
        try:
            response = _request_get(
                "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
            )
            if response.status_code == 200:
                items = response.json()
        except Exception as exc:
            logging.debug("get_upcoming_events fetch: %s", exc)
        warnings = []
        for item in items:
            title = item.get("title", "")
            if any(keyword.lower() in title.lower() for keyword in high_impact):
                warnings.append(f"{item.get('date', '')}: {title[:60]}")
        _economic_cache = " | ".join(warnings[:2]) if warnings else ""
        _economic_cache_time = time.time()
        return _economic_cache
    except Exception as exc:
        logging.debug("get_upcoming_events: %s", exc)
        return ""


__all__ = ["get_dxy_signal", "get_fg_history", "get_upcoming_events"]
