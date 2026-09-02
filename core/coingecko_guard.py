"""Transparent process-local protection for CoinGecko public API calls.

The guard changes no trading logic and preserves the normal ``requests.get``
response interface.  Only api.coingecko.com GET calls are coordinated.
"""

import copy
import threading
import time
from typing import Dict, Tuple
from urllib.parse import urlparse

import requests

_LOCK = threading.RLock()
_CACHE: Dict[str, Tuple[float, requests.Response]] = {}
_LAST_REQUEST_AT = 0.0
_COOLDOWN_UNTIL = 0.0
_MIN_REQUEST_SPACING = 2.5
_DEFAULT_429_COOLDOWN = 60.0
_ORIGINAL_GET = requests.get
_INSTALLED = False


def _ttl_for(url: str) -> float:
    path = urlparse(url).path.rstrip("/")
    if path.endswith("/global"):
        return 3600.0
    if path.endswith("/search/trending"):
        return 1800.0
    if path.endswith("/coins/markets"):
        return 900.0
    if path.endswith("/simple/price"):
        return 20.0
    return 60.0


def _cache_key(url: str, params) -> str:
    if not params:
        return url
    try:
        items = sorted((str(k), str(v)) for k, v in params.items())
    except AttributeError:
        return f"{url}|{params!r}"
    return f"{url}|{items!r}"


def _is_coingecko(url) -> bool:
    try:
        return urlparse(str(url)).hostname == "api.coingecko.com"
    except Exception:
        return False


def _retry_after(response: requests.Response) -> float:
    raw = response.headers.get("Retry-After")
    try:
        return max(float(raw), _DEFAULT_429_COOLDOWN) if raw else _DEFAULT_429_COOLDOWN
    except (TypeError, ValueError):
        return _DEFAULT_429_COOLDOWN


def guarded_get(url, *args, **kwargs):
    """Drop-in ``requests.get`` wrapper for CoinGecko only."""
    global _LAST_REQUEST_AT, _COOLDOWN_UNTIL
    if not _is_coingecko(url):
        return _ORIGINAL_GET(url, *args, **kwargs)

    ttl = _ttl_for(str(url))
    key = _cache_key(str(url), kwargs.get("params"))
    now = time.monotonic()
    with _LOCK:
        cached = _CACHE.get(key)
        if cached and now - cached[0] <= ttl:
            return copy.copy(cached[1])

        if now < _COOLDOWN_UNTIL:
            # Preserve existing caller fallbacks by returning the last successful
            # response for this exact request when available, even if its normal
            # TTL expired.  If none exists, make no synthetic payload.
            if cached:
                return copy.copy(cached[1])
            response = requests.Response()
            response.status_code = 429
            response.url = str(url)
            response.reason = "CoinGecko cooldown active"
            return response

        wait = _MIN_REQUEST_SPACING - (now - _LAST_REQUEST_AT)
        if wait > 0:
            time.sleep(wait)

        response = _ORIGINAL_GET(url, *args, **kwargs)
        _LAST_REQUEST_AT = time.monotonic()
        if response.status_code == 429:
            _COOLDOWN_UNTIL = _LAST_REQUEST_AT + _retry_after(response)
            if cached:
                return copy.copy(cached[1])
            return response

        if 200 <= response.status_code < 300:
            _CACHE[key] = (_LAST_REQUEST_AT, copy.copy(response))
        return response


def install() -> None:
    """Install once for the current Python process."""
    global _INSTALLED
    with _LOCK:
        if _INSTALLED:
            return
        requests.get = guarded_get
        _INSTALLED = True
