"""Transparent process-local request de-duplication for CoinGecko.

The guard changes no trading logic and preserves the normal ``requests.get``
response interface. Only api.coingecko.com GET calls are coordinated.

Design goals:
- endpoint-specific TTL cache;
- single-flight per identical request so concurrent callers do not duplicate HTTP;
- no cooldowns, no global blocking window and no artificial sleep;
- on HTTP 429, reuse the last successful response for that exact request when
  available, otherwise return the original 429 so existing caller fallbacks work.
"""

import copy
import threading
import time
from typing import Dict, Tuple
from urllib.parse import urlparse

import requests

_CACHE_LOCK = threading.RLock()
_CACHE: Dict[str, Tuple[float, requests.Response]] = {}
_KEY_LOCKS: Dict[str, threading.Lock] = {}
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


def _fresh_cached(key: str, ttl: float):
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached and now - cached[0] <= ttl:
            return copy.copy(cached[1])
    return None


def _last_cached(key: str):
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        return copy.copy(cached[1]) if cached else None


def _key_lock(key: str) -> threading.Lock:
    with _CACHE_LOCK:
        lock = _KEY_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _KEY_LOCKS[key] = lock
        return lock


def guarded_get(url, *args, **kwargs):
    """Drop-in ``requests.get`` wrapper for CoinGecko only."""
    if not _is_coingecko(url):
        return _ORIGINAL_GET(url, *args, **kwargs)

    url_text = str(url)
    ttl = _ttl_for(url_text)
    key = _cache_key(url_text, kwargs.get("params"))

    cached = _fresh_cached(key, ttl)
    if cached is not None:
        return cached

    # Single-flight: only identical requests wait for each other. Different
    # CoinGecko endpoints/params remain independent and are never put on a
    # global cooldown or delay.
    with _key_lock(key):
        cached = _fresh_cached(key, ttl)
        if cached is not None:
            return cached

        response = _ORIGINAL_GET(url, *args, **kwargs)
        if response.status_code == 429:
            stale = _last_cached(key)
            return stale if stale is not None else response

        if 200 <= response.status_code < 300:
            with _CACHE_LOCK:
                _CACHE[key] = (time.monotonic(), copy.copy(response))
        return response


def install() -> None:
    """Install once for the current Python process."""
    global _INSTALLED
    with _CACHE_LOCK:
        if _INSTALLED:
            return
        requests.get = guarded_get
        _INSTALLED = True
