"""Open Labels Initiative metadata for explicitly configured addresses."""
from __future__ import annotations
import json
import os
from .cache import cache
from .http_client import http_client

SOURCE = "open_labels_initiative"
_BASE = os.environ.get("OLI_API_URL", "https://api.openlabelsinitiative.org").rstrip("/")

def _addresses(symbol: str) -> list[str]:
    try:
        mapping = json.loads(os.environ.get("OLI_TRACKED_ADDRESSES_JSON", "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    values = mapping.get(symbol.upper().replace("/", ""), []) if isinstance(mapping, dict) else []
    return [str(value).strip() for value in values if str(value).strip()] if isinstance(values, list) else []

async def collect(symbol: str) -> dict:
    addresses = _addresses(symbol)
    if not addresses:
        return {"source": SOURCE, "status": "not_configured", "symbol": symbol}
    api_key = os.environ.get("OLI_API_KEY", "").strip()
    headers = {"x-api-key": api_key} if api_key else None
    async def fetch():
        rows = []
        for address in addresses[:5]:
            endpoint = f"{_BASE}/labels" if api_key else f"{_BASE}/attestations"
            params = {"address": address} if api_key else {"recipient": address}
            rows.append({"address": address, "payload": await http_client.get_json(endpoint, params, headers)})
        return rows
    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:{symbol}:{','.join(addresses)}", 3600, 86400, fetch)
        labels: list[str] = []
        for item in payload:
            data = item.get("payload") if isinstance(item, dict) else None
            candidates = (data.get("attestations", data.get("labels", data.get("results", data.get("data", data.get("items", []))))) if isinstance(data, dict) else data)
            for row in candidates if isinstance(candidates, list) else []:
                if isinstance(row, dict):
                    value = row.get("label") or row.get("tag") or row.get("name") or row.get("tags_json")
                    if value and str(value) not in labels:
                        labels.append(str(value)[:120])
        return {"source": SOURCE, "status": status, "age_seconds": age, "symbol": symbol,
                "normalized": {"labels": labels[:20], "address_count": len(addresses)}}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}
