"""Small process-local guard for CoinGecko public API calls.

Keeps endpoint-specific TTL caches and a shared request spacing/cooldown without
changing the payloads returned to callers.  This module deliberately contains no
trading logic.
"""

import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

import requests


_LOCK = threading.RLock()
_CACHE: Dict[str, Tuple[float, Any]] = {}
_LAST_REQUEST_AT = 0.0
_COOLDOWN_UNTIL = 0.0
_MIN_REQUEST_SPACING = 2.5
_DEFAULT_429_COOLDOWN = 60.0


def _cached(key: str, ttl: float) -> Optional[Any]:
    now = time.monotonic()
    with _LOCK:
        item = _CACHE.get(key)
        if item and now - item[0] <= ttl:
            return item[1]
    return None


def _store(key: str, value: Any) -> None:
    with _LOCK:
        _CACHE[key] = (time.monotonic(), value)


def _retry_after_seconds(response: requests.Response) -> float:
    raw = response.headers.get("Retry-After")
    try:
        return max(float(raw), _DEFAULT_429_COOLDOWN) if raw else _DEFAULT_429_COOLDOWN
    except (TypeError, ValueError):
        return _DEFAULT_429_COOLDOWN


def get_json(
    key: str,
    url: str,
    *,
    ttl: float,
    timeout: float,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    validator: Optional[Callable[[Any], bool]] = None,
) -> Any:
    """Return fresh cached JSON or perform one rate-limited CoinGecko request.

    Raises requests-compatible errors to preserve each caller's existing
    fallback/error handling.  A 429 starts a shared cooldown; no retry storm is
    generated.
    """
    global _LAST_REQUEST_AT, _COOLDOWN_UNTIL

    hit = _cached(key, ttl)
    if hit is not None:
        return hit

    with _LOCK:
        # Re-check after waiting for another caller that may have populated cache.
        hit = _cached(key, ttl)
        if hit is not None:
            return hit

        now = time.monotonic()
        if now < _COOLDOWN_UNTIL:
            raise requests.HTTPError("CoinGecko cooldown active after HTTP 429")

        wait = _MIN_REQUEST_SPACING - (now - _LAST_REQUEST_AT)
        if wait > 0:
            time.sleep(wait)

        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        _LAST_REQUEST_AT = time.monotonic()
        if response.status_code == 429:
            _COOLDOWN_UNTIL = _LAST_REQUEST_AT + _retry_after_seconds(response)
            response.raise_for_status()
        response.raise_for_status()
        payload = response.json()
        if validator is not None and not validator(payload):
            raise ValueError(f"invalid CoinGecko payload for {key}")
        _store(key, payload)
        return payload
