"""Bounded Gate USD-M ticker and liquid-universe provider."""

from __future__ import annotations

import logging
import time

from core.pair_universe import (
    DEFAULT_UNIVERSE_SIZE, FALLBACK_COMMON_PAIRS, select_gate_pairs,
)


_pairs_cache: list[str] = []
_pairs_cache_time = 0.0
_price_cache: dict[str, dict] = {}
_last_price_update = 0.0


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def get_top_pairs(limit: int = DEFAULT_UNIVERSE_SIZE) -> list[str]:
    global _pairs_cache, _pairs_cache_time
    if time.time() - _pairs_cache_time < 3600 and _pairs_cache:
        return _pairs_cache[:limit]
    target = max(1, min(int(limit), DEFAULT_UNIVERSE_SIZE))
    try:
        response = _request_get(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers", timeout=7,
            headers={"User-Agent": "APEX-SMC/1.0"},
        )
        response.raise_for_status()
        discovered = select_gate_pairs(
            response.json(), limit=DEFAULT_UNIVERSE_SIZE,
        )
        if not discovered:
            raise RuntimeError("no eligible Gate perpetuals")
        _pairs_cache = discovered
        logging.info("[PairUniverse] %s liquid Gate perpetuals", len(_pairs_cache))
    except Exception as exc:
        _pairs_cache = list(dict.fromkeys(FALLBACK_COMMON_PAIRS))[:DEFAULT_UNIVERSE_SIZE]
        logging.warning(
            "[PairUniverse] live refresh unavailable, using %s-pair fallback: %s",
            len(_pairs_cache), exc,
        )
    _pairs_cache_time = time.time()
    return _pairs_cache[:target]


def get_live_prices() -> dict[str, dict]:
    global _price_cache, _last_price_update
    if time.time() - _last_price_update < 20 and _price_cache:
        return _price_cache
    try:
        response = _request_get(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers",
            headers={"User-Agent": "APEX-SMC/1.0"}, timeout=10,
        )
        response.raise_for_status()
        tickers = response.json()
        if isinstance(tickers, list) and tickers:
            market = {}
            for ticker in tickers:
                symbol = str(ticker.get("contract", "")).replace("_", "").upper()
                if not symbol.endswith("USDT"):
                    continue
                try:
                    price = float(ticker.get("last") or ticker.get("mark_price") or 0)
                    change = float(ticker.get("change_percentage") or 0)
                    volume = float(
                        ticker.get("volume_24h_quote")
                        or ticker.get("volume_24h_settle") or 0
                    )
                    if price > 0:
                        market[symbol] = {
                            "price": price, "change": round(change, 2),
                            "volume": volume,
                        }
                except Exception:
                    pass
            if market:
                _price_cache = market
                _last_price_update = time.time()
                logging.info("Цены: Gate Futures (%s пар)", len(market))
                return _price_cache
    except Exception as exc:
        logging.warning("Gate Futures prices unavailable: %s", exc)
    logging.error("Gate Futures prices unavailable; using last cache")
    return _price_cache if _price_cache else {}


def get_all_market_pairs() -> list[str]:
    """Return the same Gate analysis universe used by scheduled scanners."""
    return get_top_pairs(DEFAULT_UNIVERSE_SIZE)


__all__ = ["get_all_market_pairs", "get_live_prices", "get_top_pairs"]
