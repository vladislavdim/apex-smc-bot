"""
market_data.py - Исправленный модуль получения рыночных данных
Решает проблемы: retry, timeout, fallback, проверка пустых данных
"""

import requests
import json
import time
import logging
import asyncio
from typing import List, Dict, Optional, Union
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Rate limiting
class RateLimiter:
    def __init__(self, calls_per_second: int = 10):
        self.calls_per_second = calls_per_second
        self.calls = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            # Удаляем старые вызовы (старше 1 секунды)
            self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            if len(self.calls) >= self.calls_per_second:
                # Нужно подождать
                sleep_time = 1.0 - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    self.calls = []
            
            self.calls.append(now)

# Глобальные rate limiters для разных API
binance_limiter = RateLimiter(calls_per_second=10)
coingecko_limiter = RateLimiter(calls_per_second=5)
general_limiter = RateLimiter(calls_per_second=20)

# API endpoints
BINANCE_F = "https://fapi.binance.com"
BINANCE = "https://api.binance.com"
CRYPTOCOMPARE = "https://min-api.cryptocompare.com"

def safe_request(url: str, method: str = "GET", params: Dict = None, 
                headers: Dict = None, timeout: int = 10, 
                max_retries: int = 3, backoff_factor: float = 1.0) -> Optional[Dict]:
    """
    Безопасный HTTP запрос с retry и exponential backoff
    """
    headers = headers or {"User-Agent": "Mozilla/5.0"}
    params = params or {}
    
    for attempt in range(max_retries):
        try:
            # Rate limiting
            if "binance" in url.lower():
                binance_limiter.wait_if_needed()
            elif "coingecko" in url.lower():
                coingecko_limiter.wait_if_needed()
            else:
                general_limiter.wait_if_needed()
            
            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                timeout=timeout
            )
            
            response.raise_for_status()
            
            # Проверка на пустой ответ
            if not response.text.strip():
                logging.warning(f"Empty response from {url}")
                return None
            
            # Валидация JSON
            try:
                data = response.json()
                return data
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error from {url}: {e}")
                if attempt == max_retries - 1:
                    return None
                continue
                
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout on attempt {attempt + 1} for {url}")
        except requests.exceptions.ConnectionError:
            logging.warning(f"Connection error on attempt {attempt + 1} for {url}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                # Rate limit exceeded
                wait_time = backoff_factor * (2 ** attempt) + 5
                logging.warning(f"Rate limit hit, waiting {wait_time}s")
                time.sleep(wait_time)
                continue
            elif e.response.status_code >= 500:
                # Server error, retry
                logging.warning(f"Server error {e.response.status_code} on attempt {attempt + 1}")
            else:
                # Client error, don't retry
                logging.error(f"Client error {e.response.status_code} for {url}")
                return None
        except Exception as e:
            logging.error(f"Unexpected error on attempt {attempt + 1} for {url}: {e}")
        
        # Exponential backoff
        if attempt < max_retries - 1:
            wait_time = backoff_factor * (2 ** attempt)
            time.sleep(wait_time)
    
    return None

def validate_candles(candles: List[Dict]) -> bool:
    """
    Валидация свечей на корректность данных
    """
    if not candles or not isinstance(candles, list):
        return False
    
    required_fields = ["open", "high", "low", "close", "volume"]
    
    for candle in candles[:5]:  # Проверяем первые 5 свечей
        if not isinstance(candle, dict):
            return False
        
        # Проверяем обязательные поля
        for field in required_fields:
            if field not in candle:
                return False
            try:
                value = float(candle[field])
                if value < 0 and field != "volume":  # Объём может быть 0
                    return False
            except (ValueError, TypeError):
                return False
        
        # Проверяем логику OHLC
        try:
            o, h, l, c = map(float, [candle["open"], candle["high"], 
                                    candle["low"], candle["close"]])
            if not (l <= o <= h and l <= c <= h):
                return False
        except (ValueError, TypeError):
            return False
    
    return True

def normalize_candles(raw_candles: List, source: str = "binance") -> List[Dict]:
    """
    Нормализация свечей из разных форматов в единый
    """
    if not raw_candles:
        return []
    
    normalized = []
    
    if source == "binance":
        # Binance format: [timestamp, open, high, low, close, volume, ...]
        for candle in raw_candles:
            try:
                normalized.append({
                    "timestamp": int(candle[0]),
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5])
                })
            except (IndexError, ValueError, TypeError):
                continue
    
    elif source == "cryptocompare":
        # CryptoCompare format: {"volumefrom": ..., "volumeto": ..., "open": ..., ...}
        for candle in raw_candles:
            try:
                normalized.append({
                    "timestamp": int(candle.get("time", 0)),
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "volume": float(candle.get("volumeto", 0))
                })
            except (KeyError, ValueError, TypeError):
                continue
    
    elif source == "coingecko_ohlc":
        # CoinGecko OHLC format: [timestamp, open, high, low, close]
        for candle in raw_candles:
            try:
                normalized.append({
                    "timestamp": int(candle[0]),
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": 0.0  # CoinGecko OHLC не включает объём
                })
            except (IndexError, ValueError, TypeError):
                continue
    
    return normalized

def get_candles_binance(symbol: str, interval: str, limit: int = 200, 
                       is_futures: bool = True) -> Optional[List[Dict]]:
    """
    Получение свечей с Binance с retry и fallback
    """
    # Конвертация интервала
    interval_map = {
        "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
        "1h": "1h", "4h": "4h", "1d": "1d", "1w": "1w"
    }
    bi = interval_map.get(interval, interval)
    
    # Пробуем сначала Futures, потом Spot
    endpoints = [
        (f"{BINANCE_F}/fapi/v1/klines", "Binance Futures"),
        (f"{BINANCE}/api/v3/klines", "Binance Spot")
    ] if is_futures else [(f"{BINANCE}/api/v3/klines", "Binance Spot")]
    
    for endpoint, name in endpoints:
        try:
            data = safe_request(
                url=endpoint,
                params={
                    "symbol": symbol,
                    "interval": bi,
                    "limit": min(limit, 1500)  # Binance limit
                },
                timeout=10,
                max_retries=3
            )
            
            if data and isinstance(data, list) and len(data) > 0:
                candles = normalize_candles(data, "binance")
                if validate_candles(candles):
                    logging.info(f"Got {len(candles)} candles from {name} for {symbol}")
                    return candles[-limit:]  # Возвращаем последние N свечей
                    
        except Exception as e:
            logging.warning(f"Failed to get candles from {name}: {e}")
            continue
    
    return None

def get_candles_cryptocompare(symbol: str, interval: str, limit: int = 200) -> Optional[List[Dict]]:
    """
    Fallback источник: CryptoCompare
    """
    # Конвертация интервала
    interval_map = {
        "1m": ("minute", 1), "5m": ("minute", 5), "15m": ("minute", 15),
        "30m": ("minute", 30), "1h": ("hour", 1), "4h": ("hour", 4),
        "1d": ("day", 1)
    }
    
    ep, agg = interval_map.get(interval, ("hour", 1))
    
    # Конвертация символа
    base = symbol.replace("USDT", "").replace("BUSD", "")
    
    try:
        data = safe_request(
            url=f"{CRYPTOCOMPARE}/data/v2/histo{ep}",
            params={
                "fsym": base,
                "tsym": "USD",
                "limit": min(limit, 2000),
                "aggregate": agg
            },
            timeout=15,
            max_retries=2
        )
        
        if data and isinstance(data, dict) and "Data" in data:
            candles = normalize_candles(data["Data"], "cryptocompare")
            if validate_candles(candles):
                logging.info(f"Got {len(candles)} candles from CryptoCompare for {symbol}")
                return candles[-limit:]
                
    except Exception as e:
        logging.warning(f"Failed to get candles from CryptoCompare: {e}")
    
    return None

def get_candles_coingecko(symbol: str, interval: str, limit: int = 200) -> Optional[List[Dict]]:
    """
    Fallback источник: CoinGecko
    """
    # Конвертация символа в CoinGecko ID
    coin_map = {
        "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "SOLUSDT": "solana",
        "BNBUSDT": "binancecoin", "XRPUSDT": "ripple", "ADAUSDT": "cardano",
        "DOGEUSDT": "dogecoin", "AVAXUSDT": "avalanche-2", "DOTUSDT": "polkadot",
        "LINKUSDT": "chainlink", "MATICUSDT": "matic-network", "UNIUSDT": "uniswap"
    }
    
    cg_id = coin_map.get(symbol)
    if not cg_id:
        return None
    
    # Конвертация интервала в дни
    days_map = {
        "1m": 1, "5m": 1, "15m": 1, "30m": 1,
        "1h": 7, "4h": 30, "1d": 365, "1w": 365
    }
    days = days_map.get(interval, 7)
    
    try:
        data = safe_request(
            url=f"https://api.coingecko.com/api/v3/coins/{cg_id}/ohlc",
            params={
                "vs_currency": "usd",
                "days": days
            },
            timeout=15,
            max_retries=2
        )
        
        if data and isinstance(data, list) and len(data) > 0:
            candles = normalize_candles(data, "coingecko_ohlc")
            if validate_candles(candles):
                logging.info(f"Got {len(candles)} candles from CoinGecko for {symbol}")
                return candles[-limit:]
                
    except Exception as e:
        logging.warning(f"Failed to get candles from CoinGecko: {e}")
    
    return None

def get_candles_smart(symbol: str, interval: str = "1h", limit: int = 200) -> Dict[str, Union[List[Dict], str, int]]:
    """
    Умное получение свечей с множественными источниками и fallback
    """
    result = {
        "candles": [],
        "source": "unknown",
        "quality": "low",
        "error": None
    }
    
    # Пробуем источники в порядке приоритета
    sources = [
        (get_candles_binance, "Binance Futures", {"is_futures": True}),
        (get_candles_binance, "Binance Spot", {"is_futures": False}),
        (get_candles_cryptocompare, "CryptoCompare", {}),
        (get_candles_coingecko, "CoinGecko", {})
    ]
    
    for get_func, source_name, kwargs in sources:
        try:
            candles = get_func(symbol, interval, limit, **kwargs)
            if candles and len(candles) >= min(limit // 2, 20):  # Минимум половина запрошенных
                result["candles"] = candles
                result["source"] = source_name
                result["quality"] = "high" if len(candles) >= limit * 0.9 else "medium"
                logging.info(f"Successfully got {len(candles)} candles from {source_name}")
                return result
        except Exception as e:
            logging.warning(f"Source {source_name} failed: {e}")
            continue
    
    # Если все источники failed
    result["error"] = f"All sources failed for {symbol}/{interval}"
    logging.error(result["error"])
    return result

def get_multiple_candles(symbols: List[str], interval: str = "1h", 
                         limit: int = 200, max_workers: int = 5) -> Dict[str, Dict]:
    """
    Параллельное получение свечей для множества символов
    """
    results = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Создаём future для каждого символа
        future_to_symbol = {
            executor.submit(get_candles_smart, symbol, interval, limit): symbol
            for symbol in symbols
        }
        
        # Собираем результаты
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                result = future.result(timeout=30)
                results[symbol] = result
                
                # Логируем статус
                if result["candles"]:
                    logging.debug(f"✅ {symbol}: {len(result['candles'])} candles from {result['source']}")
                else:
                    logging.warning(f"❌ {symbol}: {result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                logging.error(f"❌ {symbol}: Exception - {e}")
                results[symbol] = {
                    "candles": [],
                    "source": "error",
                    "quality": "none",
                    "error": str(e)
                }
    
    return results

def get_top_pairs(limit: int = 50) -> List[str]:
    """
    Получение топ пар с Binance с retry
    """
    try:
        # Пробуем сначала CryptoCompare (более стабильный)
        data = safe_request(
            url=f"{CRYPTOCOMPARE}/data/top/mktcapfull",
            params={"limit": limit, "tsym": "USD"},
            timeout=10
        )
        
        if data and isinstance(data, dict) and "Data" in data:
            pairs = []
            for coin in data["Data"]:
                symbol = coin.get("CoinInfo", {}).get("Name", "")
                if symbol:
                    pairs.append(f"{symbol}USDT")
            
            if len(pairs) >= limit:
                logging.info(f"Got {len(pairs)} pairs from CryptoCompare")
                return pairs[:limit]
    
    except Exception as e:
        logging.warning(f"CryptoCompare top pairs failed: {e}")
    
    # Fallback на Binance
    try:
        data = safe_request(
            url=f"{BINANCE_F}/fapi/v1/ticker/24hr",
            timeout=10
        )
        
        if data and isinstance(data, list):
            # Сортируем по объёму
            sorted_data = sorted(data, key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            pairs = [item["symbol"] for item in sorted_data if item["symbol"].endswith("USDT")]
            
            logging.info(f"Got {len(pairs)} pairs from Binance Futures")
            return pairs[:limit]
            
    except Exception as e:
        logging.error(f"Binance top pairs failed: {e}")
    
    # Ultimate fallback
    return [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
        "MATICUSDT", "UNIUSDT", "LTCUSDT", "BCHUSDT", "FILUSDT"
    ]

# Кэш для свечей (в памяти)
_candle_cache = {}
_cache_lock = threading.Lock()
_cache_ttl = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1d": 86400
}

def get_candles_cached(symbol: str, interval: str = "1h", limit: int = 200, 
                      use_cache: bool = True) -> Dict[str, Union[List[Dict], str, int]]:
    """
    Получение свечей с кэшированием
    """
    cache_key = f"{symbol}_{interval}"
    now = time.time()
    ttl = _cache_ttl.get(interval, 3600)
    
    # Проверяем кэш
    if use_cache:
        with _cache_lock:
            if cache_key in _candle_cache:
                cached_data, cached_time = _candle_cache[cache_key]
                if now - cached_time < ttl:
                    # Возвращаем из кэша, но обрезаем до нужного лимита
                    cached_result = {
                        "candles": cached_data[-limit:],
                        "source": "cache",
                        "quality": "high"
                    }
                    logging.debug(f"Cache hit for {cache_key}")
                    return cached_result
    
    # Получаем свежие данные
    result = get_candles_smart(symbol, interval, limit)
    
    # Сохраняем в кэш если успешно
    if use_cache and result["candles"]:
        with _cache_lock:
            _candle_cache[cache_key] = (result["candles"], now)
            
            # Очищаем старые записи из кэша
            keys_to_remove = []
            for key, (_, timestamp) in _candle_cache.items():
                if now - timestamp > ttl * 2:  # Храним в 2 раза дольше TTL
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del _candle_cache[key]
    
    return result

def clear_cache():
    """Очистка кэша свечей"""
    with _cache_lock:
        _candle_cache.clear()
    logging.info("Candle cache cleared")

def get_cache_stats() -> Dict[str, int]:
    """Статистика кэша"""
    with _cache_lock:
        return {
            "cached_symbols": len(_candle_cache),
            "cache_keys": list(_candle_cache.keys())
        }
