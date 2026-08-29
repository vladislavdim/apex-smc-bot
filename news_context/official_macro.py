"""Published U.S. macro actuals from the keyless BLS v1 API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from external_sources.cache import cache
from external_sources.http_client import http_client
from external_sources.models import number


SOURCE = "bls_official_actuals"
_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
_SERIES = {
    "CUUR0000SA0": "cpi_all_urban",
    "LNS14000000": "unemployment_rate",
    "CES0000000001": "nonfarm_payrolls",
    "CES0500000003": "average_hourly_earnings",
}


def _month_key(row: dict[str, Any]) -> tuple[int, int]:
    period = str(row.get("period", ""))
    month = int(period[1:]) if period.startswith("M") and period[1:].isdigit() else 0
    try:
        year = int(row.get("year", 0))
    except (TypeError, ValueError):
        year = 0
    return year, month


def _footnotes(row: dict[str, Any]) -> list[str]:
    values = row.get("footnotes", [])
    return [str(item.get("text")) for item in values if isinstance(item, dict) and item.get("text")]


def normalize(payload: dict[str, Any]) -> dict[str, Any]:
    series = payload.get("Results", {}).get("series", []) if isinstance(payload, dict) else []
    output: dict[str, Any] = {}
    for item in series if isinstance(series, list) else []:
        if not isinstance(item, dict):
            continue
        series_id = str(item.get("seriesID", ""))
        name = _SERIES.get(series_id)
        if not name:
            continue
        rows = [row for row in item.get("data", []) if isinstance(row, dict) and str(row.get("period", "")).startswith("M") and row.get("period") != "M13"]
        rows.sort(key=_month_key)
        if not rows:
            continue
        latest, previous = rows[-1], rows[-2] if len(rows) >= 2 else {}
        value, old = number(latest.get("value")), number(previous.get("value"))
        year, month = _month_key(latest)
        year_ago = next((row for row in rows if _month_key(row) == (year - 1, month)), None)
        year_ago_value = number(year_ago.get("value")) if year_ago else None
        output[name] = {
            "series_id": series_id, "value": value,
            "observation": f"{year:04d}-{month:02d}" if year and month else None,
            "previous": old,
            "change_1m": round(value - old, 4) if value is not None and old is not None else None,
            "change_1m_pct": round((value - old) / old * 100, 4) if value is not None and old else None,
            "change_12m_pct": round((value - year_ago_value) / year_ago_value * 100, 4) if value is not None and year_ago_value else None,
            "footnotes": _footnotes(latest),
        }
    return output


async def collect() -> dict[str, Any]:
    async def fetch() -> Any:
        year = datetime.now(timezone.utc).year
        return await http_client.post_json(_URL, {
            "seriesid": list(_SERIES),
            "startyear": str(year - 1),
            "endyear": str(year),
        })

    try:
        payload, status, age = await cache.get_or_fetch(SOURCE, 21600, 172800, fetch)
        if not isinstance(payload, dict) or payload.get("status") not in {None, "REQUEST_SUCCEEDED"}:
            raise ValueError("bls_response_failed")
        normalized = normalize(payload)
        if not normalized:
            raise ValueError("empty_bls_actuals")
        return {"source": SOURCE, "status": status, "age_seconds": age, "actuals": normalized}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__}
