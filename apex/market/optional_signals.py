"""Optional liquidation, on-chain sentiment and whale-transfer context."""

from __future__ import annotations

import logging
import re
import time

from apex.config.settings import IntegrationSettings


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def _request_post(url: str, **kwargs):
    import requests
    return requests.post(url, **kwargs)


class OptionalSignalProviders:
    _SANTIMENT_SLUGS = {
        "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "SOLUSDT": "solana",
        "BNBUSDT": "binance-coin", "XRPUSDT": "ripple", "ADAUSDT": "cardano",
        "AVAXUSDT": "avalanche", "DOTUSDT": "polkadot", "LINKUSDT": "chainlink",
        "LTCUSDT": "litecoin", "ATOMUSDT": "cosmos", "NEARUSDT": "near-protocol",
        "INJUSDT": "injective-protocol", "SUIUSDT": "sui", "ARBUSDT": "arbitrum",
    }

    def __init__(self, settings: IntegrationSettings):
        self._settings = settings
        self._liquidation_cache: dict[str, dict] = {}
        self._liquidation_cache_time = 0.0
        self._sentiment_cache: dict[str, dict] = {}
        self._sentiment_cache_time = 0.0
        self._whale_cache: list[str] = []
        self._whale_cache_time = 0.0

    def get_liquidations(self, symbol: str) -> dict | None:
        if time.time() - self._liquidation_cache_time < 1800:
            cached = self._liquidation_cache.get(symbol)
            if cached is not None:
                return cached
        api_key = self._settings.coinglass_api_key
        if not api_key:
            return None
        try:
            response = _request_get(
                "https://open-api.coinglass.com/public/v2/liquidation_ex",
                headers={"coinglassSecret": api_key},
                params={"symbol": symbol.replace("USDT", ""), "interval": "1h"},
                timeout=10,
            )
            payload = response.json()
            if payload.get("code") != "0" or not payload.get("data"):
                return None
            item = payload["data"][0] if isinstance(payload["data"], list) else payload["data"]
            long_liquidations = float(item.get("longLiquidationUsd", 0))
            short_liquidations = float(item.get("shortLiquidationUsd", 0))
            result = {
                "long_liq_usd": long_liquidations,
                "short_liq_usd": short_liquidations,
                "total_usd": long_liquidations + short_liquidations,
                "bias": (
                    "LONGS_WIPED" if long_liquidations > short_liquidations * 1.5
                    else "SHORTS_WIPED" if short_liquidations > long_liquidations * 1.5
                    else "BALANCED"
                ),
            }
            self._liquidation_cache[symbol] = result
            self._liquidation_cache_time = time.time()
            return result
        except Exception as exc:
            logging.debug("CoinGlass %s: %s", symbol, exc)
            return None

    def get_santiment_data(self, symbol: str) -> dict | None:
        if time.time() - self._sentiment_cache_time < 3600:
            cached = self._sentiment_cache.get(symbol)
            if cached is not None:
                return cached
        api_key = self._settings.santiment_api_key
        slug = self._SANTIMENT_SLUGS.get(symbol)
        if not api_key or not slug:
            return None
        query = '''{ getMetric(metric: "sentiment_balance_total") {
            timeseriesData(slug: "%s", from: "utc_now-1d", to: "utc_now", interval: "1h") {
                datetime value } } }''' % slug
        try:
            response = _request_post(
                "https://api.santiment.net/graphql", json={"query": query},
                headers={"Authorization": f"Apikey {api_key}"}, timeout=10,
            )
            timeseries = (
                response.json().get("data", {}).get("getMetric", {})
                .get("timeseriesData", [])
            )
            values = [row["value"] for row in timeseries if row.get("value") is not None]
            if not values:
                return None
            average = sum(values) / len(values)
            result = {
                "sentiment": round(average, 3),
                "signal": (
                    "BULLISH" if average > 0.1
                    else "BEARISH" if average < -0.1 else "NEUTRAL"
                ),
            }
            self._sentiment_cache[symbol] = result
            self._sentiment_cache_time = time.time()
            return result
        except Exception as exc:
            logging.debug("Santiment %s: %s", symbol, exc)
            return None

    def get_whale_alerts(self) -> list[str]:
        if time.time() - self._whale_cache_time < 900 and self._whale_cache:
            return self._whale_cache
        try:
            response = _request_get(
                "https://whale-alert.io/feed",
                headers={"User-Agent": "Mozilla/5.0"}, timeout=8,
            )
            items = re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', response.text)
            alerts = [item for item in items[1:11] if any(
                word in item.lower()
                for word in ("bitcoin", "ethereum", "transfer", "exchange", "moved")
            )]
            self._whale_cache = alerts[:5]
            self._whale_cache_time = time.time()
            return self._whale_cache
        except Exception as exc:
            logging.debug("Whale Alert: %s", exc)
            return []


__all__ = ["OptionalSignalProviders"]
