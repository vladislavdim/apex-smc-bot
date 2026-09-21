"""Optional public price and candle providers used by the compatibility UI."""

from __future__ import annotations

import logging
import time


YAHOO_SYMBOLS = {
    "BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "SOLUSDT": "SOL-USD",
    "BNBUSDT": "BNB-USD", "XRPUSDT": "XRP-USD", "DOGEUSDT": "DOGE-USD",
    "AVAXUSDT": "AVAX-USD", "LINKUSDT": "LINK-USD", "ADAUSDT": "ADA-USD",
    "DOTUSDT": "DOT-USD", "MATICUSDT": "MATIC-USD", "LTCUSDT": "LTC-USD",
    "ATOMUSDT": "ATOM-USD", "TRXUSDT": "TRX-USD", "XLMUSDT": "XLM-USD",
}

CRYPTOCOMPARE_SYMS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "AVAX", "LINK", "ADA",
    "DOT", "MATIC", "LTC", "ATOM", "TRX", "XLM", "NEAR", "ARB", "OP",
    "UNI", "PEPE", "SHIB", "TON", "SUI", "INJ", "APT", "WIF", "RENDER",
    "FET", "STX", "HBAR",
]

_yahoo_cache: dict[str, dict] = {}
_yahoo_cache_time = 0.0
_cryptocompare_cache: dict[str, dict] = {}
_cryptocompare_cache_time = 0.0
_messari_cache: dict[str, dict] = {}
_messari_cache_time = 0.0


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def get_yahoo_finance_prices() -> dict[str, dict]:
    global _yahoo_cache, _yahoo_cache_time
    if time.time() - _yahoo_cache_time < 60 and _yahoo_cache:
        return _yahoo_cache
    try:
        response = _request_get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={
                "symbols": " ".join(YAHOO_SYMBOLS.values()),
                "fields": "regularMarketPrice,regularMarketChangePercent",
            },
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
        )
        reverse = {value: key for key, value in YAHOO_SYMBOLS.items()}
        result = {}
        for item in response.json().get("quoteResponse", {}).get("result", []):
            symbol = reverse.get(item.get("symbol", ""))
            if symbol and item.get("regularMarketPrice"):
                result[symbol] = {
                    "price": float(item["regularMarketPrice"]),
                    "change": round(float(item.get("regularMarketChangePercent", 0)), 2),
                    "source": "Yahoo",
                }
        if result:
            _yahoo_cache = result
            _yahoo_cache_time = time.time()
            logging.info("Yahoo Finance: %s монет", len(result))
        return result
    except Exception as exc:
        logging.warning("Yahoo Finance: %s", exc)
        return {}


def get_cryptocompare_prices() -> dict[str, dict]:
    global _cryptocompare_cache, _cryptocompare_cache_time
    if time.time() - _cryptocompare_cache_time < 60 and _cryptocompare_cache:
        return _cryptocompare_cache
    try:
        response = _request_get(
            "https://min-api.cryptocompare.com/data/pricemultifull",
            params={"fsyms": ",".join(CRYPTOCOMPARE_SYMS), "tsyms": "USD"},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
        )
        result = {}
        for symbol, value in response.json().get("RAW", {}).items():
            usd = value.get("USD", {})
            if usd.get("PRICE"):
                result[f"{symbol}USDT"] = {
                    "price": float(usd["PRICE"]),
                    "change": round(float(usd.get("CHANGEPCT24HOUR", 0)), 2),
                    "volume": float(usd.get("VOLUME24HOURTO", 0)),
                    "source": "CryptoCompare",
                }
        if result:
            _cryptocompare_cache = result
            _cryptocompare_cache_time = time.time()
            logging.info("CryptoCompare: %s монет", len(result))
        return result
    except Exception as exc:
        logging.warning("CryptoCompare: %s", exc)
        return {}


def get_cryptocompare_candles(symbol: str, interval: str = "1h", limit: int = 200) -> list[dict]:
    try:
        base = symbol.replace("USDT", "").replace("BUSD", "")
        endpoint = {
            "1m": "histominute", "3m": "histominute", "5m": "histominute",
            "15m": "histominute", "30m": "histominute", "1h": "histohour",
            "2h": "histohour", "4h": "histohour", "1d": "histoday",
            "3d": "histoday", "1w": "histoday", "1M": "histoday",
        }.get(interval, "histohour")
        aggregate = {
            "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
            "1h": 1, "2h": 2, "4h": 4, "1d": 1, "3d": 3, "1w": 7,
            "1M": 30,
        }.get(interval, 1)
        response = _request_get(
            f"https://min-api.cryptocompare.com/data/{endpoint}",
            params={
                "fsym": base, "tsym": "USD", "limit": min(limit + 20, 2000),
                "aggregate": aggregate,
            },
            headers={"User-Agent": "Mozilla/5.0"}, timeout=12,
        )
        data = response.json().get("Data", [])
        if isinstance(data, dict):
            data = data.get("Data", [])
        if not data or len(data) < 5:
            return []
        candles = [{
            "open": float(row["open"]), "high": float(row["high"]),
            "low": float(row["low"]), "close": float(row["close"]),
            "volume": float(row.get("volumeto") or row.get("volumefrom") or 0),
        } for row in data if row.get("close") and float(row.get("close", 0)) > 0]
        return candles[-limit:]
    except Exception as exc:
        logging.warning("CryptoCompare candles %s: %s", symbol, exc)
        return []


def get_messari_data(symbol: str) -> dict | None:
    global _messari_cache_time
    if symbol in _messari_cache and time.time() - _messari_cache_time < 3600:
        return _messari_cache[symbol]
    try:
        base = symbol.replace("USDT", "").lower()
        response = _request_get(
            f"https://data.messari.io/api/v1/assets/{base}/metrics",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
        )
        data = response.json().get("data", {})
        market = data.get("market_data", {})
        roi = data.get("roi_data", {})
        development = data.get("developer_activity", {})
        result = {
            "price": market.get("price_usd"),
            "volume_24h": market.get("volume_last_24_hours"),
            "market_cap": market.get("real_volume_last_24_hours"),
            "change_1h": market.get("percent_change_usd_last_1_hour"),
            "change_24h": market.get("percent_change_usd_last_24_hours"),
            "change_7d": market.get("percent_change_usd_last_7_days"),
            "roi_7d": roi.get("percent_change_last_1_week"),
            "github_commits": development.get("commit_count_4_weeks"),
            "source": "Messari",
        }
        if result["price"]:
            _messari_cache[symbol] = result
            _messari_cache_time = time.time()
        return result
    except Exception as exc:
        logging.warning("Messari %s: %s", symbol, exc)
        return None


__all__ = [
    "get_cryptocompare_candles", "get_cryptocompare_prices",
    "get_messari_data", "get_yahoo_finance_prices",
]
