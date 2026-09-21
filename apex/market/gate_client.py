"""Small, injectable Gate REST adapter for the canonical V3 market layer."""

from __future__ import annotations

from dataclasses import dataclass
import json
import threading
import time
from math import isfinite
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .source_registry import authorize


GATE_API_BASE = "https://api.gateio.ws/api/v4"


class GateTransportError(RuntimeError):
    pass


JsonGet = Callable[[str, Mapping[str, Any], float], Any]


def _stdlib_get(url: str, params: Mapping[str, Any], timeout: float) -> Any:
    query = urlencode({key: value for key, value in params.items() if value is not None})
    request = Request(f"{url}?{query}" if query else url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed Gate base URL
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise GateTransportError(f"gate_request_failed:{type(exc).__name__}") from exc


def gate_contract(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper().replace("-", "_").replace("/", "_")
    if "_" not in normalized and normalized.endswith("USDT"):
        normalized = f"{normalized[:-4]}_USDT"
    if not normalized or not normalized.endswith("_USDT"):
        raise ValueError(f"unsupported_gate_contract:{symbol}")
    return normalized


def normalize_gate_candles(payload: Any) -> list[dict[str, float]]:
    """Normalize Gate's ``[t,v,c,h,l,o,...]`` and mapping response shapes."""
    if not isinstance(payload, Iterable) or isinstance(payload, (str, bytes, Mapping)):
        return []
    output: list[dict[str, float]] = []
    for row in payload:
        try:
            if isinstance(row, Mapping):
                values = {
                    "open_time": float(row.get("t", row.get("time", row.get("timestamp")))),
                    "open": float(row.get("o", row.get("open"))),
                    "high": float(row.get("h", row.get("high"))),
                    "low": float(row.get("l", row.get("low"))),
                    "close": float(row.get("c", row.get("close"))),
                    "volume": float(row.get("v", row.get("volume", 0.0))),
                }
            elif isinstance(row, (list, tuple)) and len(row) >= 6:
                values = {
                    "open_time": float(row[0]), "volume": float(row[1]),
                    "close": float(row[2]), "high": float(row[3]),
                    "low": float(row[4]), "open": float(row[5]),
                }
            else:
                continue
        except (TypeError, ValueError):
            continue
        if all(isfinite(value) for value in values.values()):
            output.append(values)
    return sorted(output, key=lambda row: row["open_time"])


@dataclass(frozen=True)
class GateResponse:
    endpoint: str
    received_at: int
    payload: Any


class GateMarketClient:
    """Public market calls only; this adapter has no trading credentials."""

    def __init__(
        self,
        *,
        get_json: JsonGet = _stdlib_get,
        base_url: str = GATE_API_BASE,
        timeout: float = 8.0,
        clock: Callable[[], float] = time.time,
        cache_ttls: Mapping[str, float] | None = None,
    ) -> None:
        self._get_json = get_json
        self._base_url = base_url.rstrip("/")
        self._timeout = max(0.1, float(timeout))
        self._clock = clock
        self._cache_ttls = {
            "/futures/usdt/candlesticks": 30.0,
            "/futures/usdt/contract_stats": 60.0,
            "/futures/usdt/funding_rate": 300.0,
            "/futures/usdt/trades": 5.0,
            "/futures/usdt/order_book": 2.0,
            **dict(cache_ttls or {}),
        }
        self._cache: dict[tuple[str, tuple[tuple[str, str], ...]], GateResponse] = {}
        self._lock_guard = threading.Lock()
        self._key_locks: dict[tuple[str, tuple[tuple[str, str], ...]], threading.Lock] = {}

    def _get(
        self, endpoint: str, params: Mapping[str, Any], *, purpose: str, source: str = "gate",
    ) -> GateResponse:
        authorize(source, purpose)
        key = (endpoint, tuple(sorted((str(name), str(value)) for name, value in params.items())))
        with self._lock_guard:
            key_lock = self._key_locks.setdefault(key, threading.Lock())
        with key_lock:
            now = int(self._clock())
            cached = self._cache.get(key)
            ttl = max(0.0, float(self._cache_ttls.get(endpoint, 0.0)))
            if cached is not None and now - cached.received_at <= ttl:
                return cached
            payload = self._get_json(f"{self._base_url}{endpoint}", params, self._timeout)
            response = GateResponse(endpoint, int(self._clock()), payload)
            self._cache[key] = response
            return response

    def clear_cache(self) -> None:
        with self._lock_guard:
            self._cache.clear()

    def candles(self, symbol: str, interval: str, *, limit: int = 1000) -> GateResponse:
        return self._get(
            "/futures/usdt/candlesticks",
            {"contract": gate_contract(symbol), "interval": interval, "limit": min(2000, max(2, int(limit)))},
            purpose="candles",
        )

    def contract_stats(self, symbol: str, *, interval: str = "1h", limit: int = 2) -> GateResponse:
        return self._get(
            "/futures/usdt/contract_stats",
            {"contract": gate_contract(symbol), "interval": interval, "limit": min(100, max(1, int(limit)))},
            purpose="context", source="gate_derivatives",
        )

    def funding_history(self, symbol: str, *, limit: int = 2) -> GateResponse:
        return self._get(
            "/futures/usdt/funding_rate",
            {"contract": gate_contract(symbol), "limit": min(1000, max(1, int(limit)))},
            purpose="context", source="gate_derivatives",
        )

    def public_trades(self, symbol: str, *, limit: int = 200) -> GateResponse:
        return self._get(
            "/futures/usdt/trades",
            {"contract": gate_contract(symbol), "limit": min(1000, max(1, int(limit)))},
            purpose="context", source="gate_ws",
        )

    def order_book(self, symbol: str, *, limit: int = 20) -> GateResponse:
        return self._get(
            "/futures/usdt/order_book",
            {"contract": gate_contract(symbol), "limit": min(100, max(1, int(limit))), "with_id": "true"},
            purpose="context", source="gate_ws",
        )


__all__ = [
    "GATE_API_BASE", "GateMarketClient", "GateResponse", "GateTransportError",
    "gate_contract", "normalize_gate_candles",
]
