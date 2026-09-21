"""Keyed optional context adapters with no entry or execution authority."""

from __future__ import annotations

import logging

from apex.config.settings import IntegrationSettings


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


class OptionalContextProviders:
    def __init__(self, settings: IntegrationSettings):
        self._settings = settings

    def get_twelvedata_candles(
        self, symbol: str, interval: str = "1h", limit: int = 200,
    ) -> list[dict]:
        api_key = self._settings.twelvedata_api_key
        if not api_key:
            return []
        try:
            interval_map = {
                "1m": "1min", "5m": "5min", "15m": "15min",
                "30m": "30min", "1h": "1h", "4h": "4h", "1d": "1day",
            }
            base = symbol.replace("USDT", "").replace("BUSD", "")
            response = _request_get(
                "https://api.twelvedata.com/time_series",
                params={
                    "symbol": f"{base}/USD",
                    "interval": interval_map.get(interval, "1h"),
                    "outputsize": limit,
                    "apikey": api_key,
                },
                headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
            )
            data = response.json()
            if data.get("status") == "error" or "values" not in data:
                return []
            return [{
                "open": float(value["open"]), "high": float(value["high"]),
                "low": float(value["low"]), "close": float(value["close"]),
                "volume": float(value.get("volume", 0)),
            } for value in reversed(data["values"])]
        except Exception as exc:
            logging.debug("TwelveData %s: %s", symbol, exc)
            return []

    def get_mobula_price(self, symbol: str) -> dict:
        api_key = self._settings.mobula_api_key
        if not api_key:
            return {}
        try:
            base = symbol.replace("USDT", "").replace("BUSD", "")
            response = _request_get(
                "https://api.mobula.io/api/1/market/data",
                params={"asset": base},
                headers={"Authorization": api_key, "User-Agent": "Mozilla/5.0"},
                timeout=8,
            )
            if response.status_code != 200:
                return {}
            data = response.json().get("data", {})
            return {
                "price": data.get("price", 0), "volume_24h": data.get("volume", 0),
                "change_24h": data.get("price_change_24h", 0), "source": "mobula",
            }
        except Exception as exc:
            logging.debug("Mobula %s: %s", symbol, exc)
            return {}

    def get_coinalyze_data(self, symbol: str) -> dict:
        api_key = self._settings.coinalyze_api_key
        if not api_key:
            return {}
        try:
            base = symbol.replace("USDT", "")
            response = _request_get(
                "https://api.coinalyze.net/v1/open-interest",
                params={"symbols": f"{base}USDT_PERP.A", "api_key": api_key},
                headers={"User-Agent": "Mozilla/5.0"}, timeout=8,
            )
            payload = response.json()
            if response.status_code != 200 or not payload:
                return {}
            data = payload[0] if isinstance(payload, list) else {}
            return {
                "open_interest": data.get("open_interest_usd", 0),
                "oi_change_24h": data.get(
                    "open_interest_usd_change_24h_percent", 0,
                ),
                "source": "coinalyze",
            }
        except Exception as exc:
            logging.debug("Coinalyze %s: %s", symbol, exc)
            return {}

    def get_lunarcrush_data(self, symbol: str) -> dict:
        api_key = self._settings.lunarcrush_api_key
        if not api_key:
            return {}
        try:
            base = symbol.replace("USDT", "").replace("BUSD", "").lower()
            response = _request_get(
                f"https://lunarcrush.com/api4/public/coins/{base}/v1",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "User-Agent": "Mozilla/5.0",
                },
                timeout=10,
            )
            if response.status_code != 200:
                return {}
            data = response.json().get("data", {})
            galaxy_score = data.get("galaxy_score", 0)
            sentiment = data.get("sentiment", 50)
            signal = (
                "BULLISH" if galaxy_score > 60 and sentiment > 60
                else "BEARISH" if galaxy_score < 30 else "NEUTRAL"
            )
            return {
                "galaxy_score": galaxy_score, "sentiment": sentiment,
                "alt_rank": data.get("alt_rank", 999), "signal": signal,
                "source": "lunarcrush",
            }
        except Exception as exc:
            logging.debug("LunarCrush %s: %s", symbol, exc)
            return {}


__all__ = ["OptionalContextProviders"]
