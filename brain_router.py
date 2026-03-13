"""
brain_router.py — Умный диспетчер данных APEX
================================================
Не меняет основные файлы. Работает как прослойка между bot.py и источниками.
Groq учится какие источники работают, строит workarounds, накапливает опыт.

Использование в bot.py:
    from brain_router import router
    candles = router.candles(symbol, interval, limit)
    context = router.signal_context(symbol, direction, tf, confluence, regime)
"""

import os, sqlite3, time, logging, requests, json, threading
from datetime import datetime, timedelta
from typing import Optional

# ── WAL патч ──
_orig_connect_br = sqlite3.connect
def _wal_connect_br(db, timeout=15, **kw):
    conn = _orig_connect_br(db, timeout=timeout, **kw)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=8000")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn
sqlite3.connect = _wal_connect_br

# ──────────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ──────────────────────────────────────────────────────────────
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_BASE_DIR, "brain.db")

GROQ_KEY = os.environ.get("GROQ_API_KEY", "")
TWELVEDATA_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
MOBULA_KEY = os.environ.get("MOBULA_API_KEY", "")
COINALYZE_KEY = os.environ.get("COINALYZE_API_KEY", "")
LUNARCRUSH_KEY = os.environ.get("LUNARCRUSH_API_KEY", "")

BINANCE = "https://api.binance.com"
BINANCE_F = "https://fapi.binance.com"
BINANCE_INTERVALS = {
    "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
    "1h":"1h","2h":"2h","4h":"4h","1d":"1d","1w":"1w","1M":"1M"
}
CC_INTERVALS = {
    "1m":("histominute",1),"3m":("histominute",3),"5m":("histominute",5),
    "15m":("histominute",15),"30m":("histominute",30),"1h":("histohour",1),
    "2h":("histohour",2),"4h":("histohour",4),"1d":("histoday",1),
    "1w":("histoday",7)
}
TD_INTERVALS = {
    "1m":"1min","5m":"5min","15m":"15min","30m":"30min",
    "1h":"1h","4h":"4h","1d":"1day"
}

COINGECKO_IDS = {
    "BTCUSDT":"bitcoin","ETHUSDT":"ethereum","SOLUSDT":"solana",
    "BNBUSDT":"binancecoin","XRPUSDT":"ripple","DOGEUSDT":"dogecoin",
    "AVAXUSDT":"avalanche-2","LINKUSDT":"chainlink","TONUSDT":"the-open-network",
    "ADAUSDT":"cardano","DOTUSDT":"polkadot","MATICUSDT":"matic-network",
    "UNIUSDT":"uniswap","LTCUSDT":"litecoin","ATOMUSDT":"cosmos",
    "APTUSDT":"aptos","ARBUSDT":"arbitrum","OPUSDT":"optimism",
    "SHIBUSDT":"shiba-inu","XLMUSDT":"stellar","WLDUSDT":"worldcoin-wld",
    "INJUSDT":"injective-protocol","SUIUSDT":"sui","TIAUSDT":"celestia",
    "FETUSDT":"fetch-ai","RENDERUSDT":"render-token","NEARUSDT":"near",
    "FTMUSDT":"fantom","ALGOUSDT":"algorand","SANDUSDT":"the-sandbox",
}

# ──────────────────────────────────────────────────────────────
# ИНИЦИАЛИЗАЦИЯ БД
# ──────────────────────────────────────────────────────────────
def _init_router_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS router_source_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, interval TEXT, source TEXT,
                success_count INTEGER DEFAULT 0,
                fail_count INTEGER DEFAULT 0,
                last_success REAL DEFAULT 0,
                last_fail REAL DEFAULT 0,
                avg_latency REAL DEFAULT 0,
                notes TEXT DEFAULT '',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS router_workarounds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                error_pattern TEXT,
                solution TEXT,
                source_skip TEXT DEFAULT '',
                confidence REAL DEFAULT 0.5,
                uses INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS router_synthetic_tf (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, target_tf TEXT, base_tf TEXT,
                quality REAL DEFAULT 0.7,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS router_knowledge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT, key TEXT, value TEXT,
                confidence REAL DEFAULT 0.5,
                expires_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS router_signal_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, direction TEXT, grade TEXT,
                tf TEXT, regime TEXT, confluence INTEGER,
                entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
                result TEXT DEFAULT 'pending',
                rr REAL DEFAULT 0,
                hours_held REAL DEFAULT 0,
                groq_insight TEXT DEFAULT '',
                error_reason TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS router_market_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, hour_utc INTEGER, day_of_week INTEGER,
                win_count INTEGER DEFAULT 0, loss_count INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS router_pair_intelligence (
                symbol TEXT PRIMARY KEY,
                best_tf TEXT DEFAULT '1h',
                best_session TEXT DEFAULT 'london',
                avg_volatility REAL DEFAULT 0,
                btc_beta REAL DEFAULT 1.0,
                typical_move_pct REAL DEFAULT 2.0,
                last_pump_pattern TEXT DEFAULT '',
                accumulation_score REAL DEFAULT 0,
                notes TEXT DEFAULT '',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS router_groq_insights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                content TEXT,
                confidence REAL DEFAULT 0.5,
                used_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS router_contradiction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, factors TEXT, verdict TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS router_seasonality (
                month INTEGER PRIMARY KEY,
                btc_avg_return REAL DEFAULT 0,
                alt_avg_return REAL DEFAULT 0,
                notes TEXT DEFAULT ''
            );
        """)
        # Вставляем историческую сезонность BTC (данные по месяцам)
        seasonality = [
            (1, 8.5, 12.0, "Январь — обычно бычий после декабря"),
            (2, 14.0, 18.0, "Февраль — сильный месяц"),
            (3, 5.0, 8.0, "Март — неоднозначный"),
            (4, 12.0, 15.0, "Апрель — pre-halving рост"),
            (5, -5.0, -3.0, "Май — 'sell in May'"),
            (6, -10.0, -8.0, "Июнь — слабый месяц"),
            (7, 3.0, 5.0, "Июль — летнее восстановление"),
            (8, -2.0, -1.0, "Август — нейтральный"),
            (9, -8.0, -6.0, "Сентябрь — исторически худший"),
            (10, 18.0, 22.0, "Октябрь — Uptober, сильный рост"),
            (11, 25.0, 30.0, "Ноябрь — традиционно самый бычий"),
            (12, 5.0, 8.0, "Декабрь — фиксация прибыли"),
        ]
        for row in seasonality:
            conn.execute(
                "INSERT OR IGNORE INTO router_seasonality VALUES (?,?,?,?)", row
            )
        conn.commit()
        conn.close()
        logging.info("[Router] БД инициализирована")
    except Exception as e:
        logging.error(f"[Router] init DB: {e}")

# ──────────────────────────────────────────────────────────────
# GROQ КЛИЕНТ (lazy)
# ──────────────────────────────────────────────────────────────
_groq_client = None
_groq_lock = threading.Lock()
_groq_tokens_today = 0
_groq_tokens_reset = time.time()

def _groq(prompt: str, max_tokens: int = 300, system: str = "") -> str:
    global _groq_client, _groq_tokens_today, _groq_tokens_reset
    if not GROQ_KEY:
        return ""
    with _groq_lock:
        if _groq_client is None:
            from groq import Groq
            _groq_client = Groq(api_key=GROQ_KEY)
        # Сброс счётчика раз в сутки
        if time.time() - _groq_tokens_reset > 86400:
            _groq_tokens_today = 0
            _groq_tokens_reset = time.time()
        # Лимит 80k токенов в день из роутера
        if _groq_tokens_today > 80000:
            return ""
    models = ["llama-3.1-8b-instant", "llama-3.1-70b-specdec", "gemma2-9b-it"]
    for model in models:
        for attempt in range(2):
            try:
                msgs = []
                if system:
                    msgs.append({"role": "system", "content": system})
                msgs.append({"role": "user", "content": prompt})
                resp = _groq_client.chat.completions.create(
                    model=model, messages=msgs, max_tokens=max_tokens,
                    temperature=0.3
                )
                result = resp.choices[0].message.content.strip()
                with _groq_lock:
                    _groq_tokens_today += max_tokens
                return result
            except Exception as e:
                err = str(e)
                if "429" in err:
                    time.sleep(20 * (attempt + 1))
                    continue
                if "model" in err.lower():
                    break
                return ""
    return ""

# ──────────────────────────────────────────────────────────────
# ЗАПИСЬ НАДЁЖНОСТИ ИСТОЧНИКОВ
# ──────────────────────────────────────────────────────────────
def _record_source(symbol: str, interval: str, source: str,
                   success: bool, latency: float = 0.0, note: str = ""):
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT id, success_count, fail_count, avg_latency FROM router_source_map "
            "WHERE symbol=? AND interval=? AND source=?",
            (symbol, interval, source)
        ).fetchone()
        now = time.time()
        if row:
            sc = row[1] + (1 if success else 0)
            fc = row[2] + (0 if success else 1)
            al = (row[3] * 0.8 + latency * 0.2) if latency > 0 else row[3]
            conn.execute(
                "UPDATE router_source_map SET success_count=?, fail_count=?, "
                "avg_latency=?, last_success=?, last_fail=?, notes=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=?",
                (sc, fc, al,
                 now if success else row[3],
                 now if not success else 0,
                 note, row[0])
            )
        else:
            conn.execute(
                "INSERT INTO router_source_map "
                "(symbol, interval, source, success_count, fail_count, avg_latency, "
                "last_success, last_fail, notes) VALUES (?,?,?,?,?,?,?,?,?)",
                (symbol, interval, source,
                 1 if success else 0, 0 if success else 1,
                 latency, now if success else 0,
                 now if not success else 0, note)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.debug(f"[Router] _record_source: {e}")

def _get_best_sources(symbol: str, interval: str) -> list:
    """Возвращает список источников отсортированных по надёжности для этой пары/TF"""
    defaults = [
        "cryptocompare", "binance_futures", "binance_spot",
        "twelvedata", "coingecko", "mexc", "kraken", "synthetic"
    ]
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT source, success_count, fail_count, avg_latency FROM router_source_map "
            "WHERE symbol=? AND interval=?",
            (symbol, interval)
        ).fetchall()
        conn.close()
        if not rows:
            return defaults
        scored = {}
        for src, sc, fc, lat in rows:
            total = sc + fc
            wr = sc / total if total > 0 else 0.5
            speed = 1.0 - min(lat / 10.0, 1.0) if lat > 0 else 0.5
            scored[src] = wr * 0.7 + speed * 0.3
        # Добавляем источники которых нет в БД с дефолтным скором
        for s in defaults:
            if s not in scored:
                scored[s] = 0.5
        return sorted(scored, key=lambda x: scored[x], reverse=True)
    except Exception as e:
        logging.debug(f"[Router] _get_best_sources: {e}")
        return defaults

# ──────────────────────────────────────────────────────────────
# ФЕТЧЕРЫ СВЕЧЕЙ
# ──────────────────────────────────────────────────────────────
def _fetch_cryptocompare(symbol: str, interval: str, limit: int) -> list:
    base = symbol.replace("USDT","").replace("BUSD","").replace("PERP","")
    ep, agg = CC_INTERVALS.get(interval, ("histohour", 1))
    if ep == "histominute":
        r = requests.get(
            f"https://min-api.cryptocompare.com/data/v2/{ep}",
            params={"fsym": base, "tsym": "USD", "limit": limit, "aggregate": agg},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10
        )
        raw = r.json()
        data = raw.get("Data", {}).get("Data", []) or raw.get("Data", [])
    else:
        r = requests.get(
            f"https://min-api.cryptocompare.com/data/{ep}",
            params={"fsym": base, "tsym": "USD", "limit": limit, "aggregate": agg},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10
        )
        raw = r.json()
        data = raw.get("Data", {}).get("Data", []) or raw.get("Data", [])
    if not data:
        raise ValueError(f"CC empty for {symbol} {interval}")
    candles = [{"open": float(c["open"]), "high": float(c["high"]),
                "low": float(c["low"]), "close": float(c["close"]),
                "volume": float(c.get("volumeto", 0))}
               for c in data if c.get("close", 0) > 0]
    return candles[-limit:]

def _fetch_binance_futures(symbol: str, interval: str, limit: int) -> list:
    bi = BINANCE_INTERVALS.get(interval, interval)
    r = requests.get(f"{BINANCE_F}/fapi/v1/klines",
        params={"symbol": symbol, "interval": bi, "limit": limit},
        headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    data = r.json()
    if not isinstance(data, list) or len(data) < 5:
        raise ValueError(f"BF empty {symbol}")
    return [{"open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]),
             "volume": float(c[5])} for c in data]

def _fetch_binance_spot(symbol: str, interval: str, limit: int) -> list:
    bi = BINANCE_INTERVALS.get(interval, interval)
    r = requests.get(f"{BINANCE}/api/v3/klines",
        params={"symbol": symbol, "interval": bi, "limit": limit},
        headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    data = r.json()
    if not isinstance(data, list) or len(data) < 5:
        raise ValueError(f"BS empty {symbol}")
    return [{"open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]),
             "volume": float(c[5])} for c in data]

def _fetch_twelvedata(symbol: str, interval: str, limit: int) -> list:
    if not TWELVEDATA_KEY:
        raise ValueError("No TwelveData key")
    base = symbol.replace("USDT","").replace("BUSD","")
    td_int = TD_INTERVALS.get(interval, "1h")
    r = requests.get("https://api.twelvedata.com/time_series",
        params={"symbol": f"{base}/USD", "interval": td_int,
                "outputsize": limit, "apikey": TWELVEDATA_KEY},
        timeout=12)
    data = r.json()
    values = data.get("values", [])
    if not values:
        raise ValueError(f"TD empty {symbol}")
    candles = [{"open": float(v["open"]), "high": float(v["high"]),
                "low": float(v["low"]), "close": float(v["close"]),
                "volume": float(v.get("volume", 0))}
               for v in reversed(values)]
    return candles

def _fetch_coingecko(symbol: str, interval: str, limit: int) -> list:
    cg_id = COINGECKO_IDS.get(symbol)
    if not cg_id:
        raise ValueError(f"No CG ID for {symbol}")
    days_map = {"1m":1,"5m":1,"15m":1,"30m":1,"1h":7,"4h":30,"1d":90,"1w":365}
    days = days_map.get(interval, 7)
    r = requests.get(f"https://api.coingecko.com/api/v3/coins/{cg_id}/ohlc",
        params={"vs_currency": "usd", "days": days},
        headers={"User-Agent": "Mozilla/5.0"}, timeout=14)
    data = r.json()
    if not isinstance(data, list) or len(data) < 5:
        raise ValueError(f"CG empty {symbol}")
    candles = [{"open": float(c[1]), "high": float(c[2]),
                "low": float(c[3]), "close": float(c[4]),
                "volume": 0.0} for c in data]
    return candles[-limit:]

def _fetch_mexc(symbol: str, interval: str, limit: int) -> list:
    mx_map = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
              "1h":"60m","4h":"4h","1d":"1d"}
    mx_int = mx_map.get(interval, "60m")
    r = requests.get("https://api.mexc.com/api/v3/klines",
        params={"symbol": symbol, "interval": mx_int, "limit": limit},
        timeout=10)
    data = r.json()
    if not isinstance(data, list) or len(data) < 5:
        raise ValueError(f"MEXC empty {symbol}")
    return [{"open": float(c[1]), "high": float(c[2]),
             "low": float(c[3]), "close": float(c[4]),
             "volume": float(c[5])} for c in data]

def _build_synthetic_tf(symbol: str, target_tf: str, limit: int) -> list:
    """Строим старший TF из более мелкого — например 4h из 1h"""
    tf_build_map = {
        "15m": ("5m", 3), "30m": ("15m", 2),
        "1h": ("15m", 4), "2h": ("1h", 2),
        "4h": ("1h", 4), "1d": ("4h", 6),
    }
    if target_tf not in tf_build_map:
        raise ValueError(f"Cannot build synthetic {target_tf}")
    base_tf, ratio = tf_build_map[target_tf]
    base_candles = _smart_fetch(symbol, base_tf, limit * ratio)
    if len(base_candles) < ratio:
        raise ValueError("Not enough base candles for synthetic")
    result = []
    for i in range(0, len(base_candles) - ratio + 1, ratio):
        chunk = base_candles[i:i+ratio]
        if len(chunk) < ratio:
            break
        result.append({
            "open": chunk[0]["open"],
            "high": max(c["high"] for c in chunk),
            "low": min(c["low"] for c in chunk),
            "close": chunk[-1]["close"],
            "volume": sum(c.get("volume", 0) for c in chunk),
            "_synthetic": True
        })
    return result[-limit:]

# ──────────────────────────────────────────────────────────────
# УМНЫЙ ФЕТЧ — главная логика
# ──────────────────────────────────────────────────────────────
_candle_cache: dict = {}
_cache_ttl = {"1m":30,"3m":60,"5m":60,"15m":120,"30m":180,
              "1h":300,"2h":360,"4h":600,"1d":1800}

def _smart_fetch(symbol: str, interval: str, limit: int) -> list:
    """Пробует источники в порядке надёжности, записывает результат"""
    ck = f"{symbol}_{interval}_{limit}"
    ttl = _cache_ttl.get(interval, 300)
    if ck in _candle_cache:
        cached, ts = _candle_cache[ck]
        if time.time() - ts < ttl and len(cached) >= min(20, limit):
            return cached

    sources_order = _get_best_sources(symbol, interval)
    fetchers = {
        "cryptocompare": _fetch_cryptocompare,
        "binance_futures": _fetch_binance_futures,
        "binance_spot": _fetch_binance_spot,
        "twelvedata": _fetch_twelvedata,
        "coingecko": _fetch_coingecko,
        "mexc": _fetch_mexc,
    }

    errors = []
    for src in sources_order:
        if src == "synthetic":
            continue
        fn = fetchers.get(src)
        if not fn:
            continue
        t0 = time.time()
        try:
            candles = fn(symbol, interval, limit)
            if candles and len(candles) >= min(20, limit):
                latency = time.time() - t0
                _record_source(symbol, interval, src, True, latency)
                _candle_cache[ck] = (candles, time.time())
                logging.info(f"[Router] {symbol} {interval} ← {src} ({len(candles)}св, {latency:.1f}с)")
                return candles
        except Exception as e:
            latency = time.time() - t0
            err_msg = str(e)
            _record_source(symbol, interval, src, False, latency, err_msg[:100])
            errors.append(f"{src}: {err_msg[:60]}")
            logging.debug(f"[Router] {src} failed {symbol} {interval}: {err_msg[:80]}")

    # Последний резерв — синтетические свечи
    try:
        candles = _build_synthetic_tf(symbol, interval, limit)
        if candles and len(candles) >= 10:
            _record_source(symbol, interval, "synthetic", True)
            _candle_cache[ck] = (candles, time.time())
            logging.info(f"[Router] {symbol} {interval} ← synthetic ({len(candles)}св)")
            return candles
    except Exception as e:
        errors.append(f"synthetic: {e}")

    # Все источники упали — Groq анализирует и записывает workaround
    if errors:
        _groq_analyze_candle_failure(symbol, interval, errors)

    logging.error(f"[Router] Нет свечей {symbol} {interval} | ошибки: {errors}")
    return []

def _groq_analyze_candle_failure(symbol: str, interval: str, errors: list):
    """Groq анализирует почему нет свечей и записывает решение"""
    try:
        errors_text = "\n".join(errors[:5])
        prompt = (
            f"Анализируй ошибки получения свечей {symbol} {interval}:\n{errors_text}\n\n"
            "Ответь JSON одной строкой: "
            "{\"reason\": \"...\", \"skip_sources\": [\"...\"], \"try_instead\": \"...\", \"note\": \"...\"}\n"
            "skip_sources — какие источники пропустить для этой пары, "
            "try_instead — что попробовать вместо них (mexc/kraken/gate/coingecko)"
        )
        result = _groq(prompt, max_tokens=150)
        if not result:
            return
        # Чистим JSON
        result = result.strip()
        if "```" in result:
            result = result.split("```")[1].replace("json","").strip()
        data = json.loads(result)
        skip = data.get("skip_sources", [])
        note = data.get("note", "")
        # Записываем workaround в БД
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO router_workarounds "
            "(error_pattern, solution, source_skip, confidence) VALUES (?,?,?,?)",
            (f"{symbol}_{interval}", data.get("reason",""), ",".join(skip), 0.7)
        )
        # Понижаем надёжность плохих источников для этой пары
        for src in skip:
            _record_source(symbol, interval, src, False, 0, "groq_skip")
        conn.commit()
        conn.close()
        logging.info(f"[Router] Groq workaround для {symbol} {interval}: пропустить {skip}")
    except Exception as e:
        logging.debug(f"[Router] _groq_analyze_candle_failure: {e}")

# ──────────────────────────────────────────────────────────────
# ДОПОЛНИТЕЛЬНЫЕ ДАННЫЕ
# ──────────────────────────────────────────────────────────────
_oi_cache: dict = {}
_funding_cache: dict = {}
_lunarcrush_cache: dict = {}

def get_open_interest(symbol: str) -> dict:
    """OI с Coinalyze или Binance"""
    if symbol in _oi_cache:
        if time.time() - _oi_cache[symbol][1] < 300:
            return _oi_cache[symbol][0]
    result = {"oi": 0, "oi_change_4h": 0, "signal": "NEUTRAL"}
    # Coinalyze
    if COINALYZE_KEY:
        try:
            r = requests.get("https://api.coinalyze.net/v1/open-interest",
                params={"symbols": symbol, "api_key": COINALYZE_KEY},
                timeout=8)
            data = r.json()
            if isinstance(data, list) and data:
                oi = data[0].get("openInterestUsd", 0)
                oi_prev = data[0].get("openInterestUsdPrev4h", oi)
                change = ((oi - oi_prev) / oi_prev * 100) if oi_prev else 0
                sig = "RISING" if change > 5 else "FALLING" if change < -5 else "NEUTRAL"
                result = {"oi": oi, "oi_change_4h": change, "signal": sig}
                _oi_cache[symbol] = (result, time.time())
                return result
        except Exception as e:
            logging.debug(f"[Router] OI Coinalyze {symbol}: {e}")
    # Binance Futures fallback
    try:
        r = requests.get(f"{BINANCE_F}/fapi/v1/openInterest",
            params={"symbol": symbol}, timeout=8)
        oi = float(r.json().get("openInterest", 0))
        result = {"oi": oi, "oi_change_4h": 0, "signal": "NEUTRAL"}
        _oi_cache[symbol] = (result, time.time())
    except Exception as e:
        logging.debug(f"[Router] OI Binance {symbol}: {e}")
    return result

def get_funding_rate(symbol: str) -> dict:
    """Funding rate с Binance Futures"""
    if symbol in _funding_cache:
        if time.time() - _funding_cache[symbol][1] < 600:
            return _funding_cache[symbol][0]
    result = {"rate": 0, "signal": "NEUTRAL", "warning": ""}
    try:
        r = requests.get(f"{BINANCE_F}/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1}, timeout=8)
        data = r.json()
        if isinstance(data, list) and data:
            rate = float(data[0].get("fundingRate", 0)) * 100
            if rate > 0.1:
                sig, warn = "HIGH_LONG", "⚠️ Высокий фандинг — лонги перегреты"
            elif rate < -0.1:
                sig, warn = "HIGH_SHORT", "⚠️ Отрицательный фандинг — шорты перегреты"
            else:
                sig, warn = "NEUTRAL", ""
            result = {"rate": rate, "signal": sig, "warning": warn}
            _funding_cache[symbol] = (result, time.time())
    except Exception as e:
        logging.debug(f"[Router] Funding {symbol}: {e}")
    return result

def get_lunarcrush_score(symbol: str) -> dict:
    """Galaxy Score и социальный объём с LunarCrush"""
    base = symbol.replace("USDT","").replace("BUSD","").lower()
    if base in _lunarcrush_cache:
        if time.time() - _lunarcrush_cache[base][1] < 3600:
            return _lunarcrush_cache[base][0]
    result = {"galaxy_score": 0, "social_volume": 0, "signal": "NEUTRAL"}
    if not LUNARCRUSH_KEY:
        return result
    try:
        r = requests.get(f"https://lunarcrush.com/api4/public/coins/{base}/v1",
            headers={"Authorization": f"Bearer {LUNARCRUSH_KEY}"}, timeout=10)
        data = r.json().get("data", {})
        gs = data.get("galaxy_score", 0)
        sv = data.get("social_volume_24h", 0)
        sig = "BULLISH" if gs > 60 else "BEARISH" if gs < 30 else "NEUTRAL"
        result = {"galaxy_score": gs, "social_volume": sv, "signal": sig}
        _lunarcrush_cache[base] = (result, time.time())
    except Exception as e:
        logging.debug(f"[Router] LunarCrush {symbol}: {e}")
    return result

def get_seasonality_context() -> dict:
    """Текущая сезонность BTC по месяцам"""
    try:
        month = datetime.now().month
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT btc_avg_return, alt_avg_return, notes FROM router_seasonality WHERE month=?",
            (month,)
        ).fetchone()
        conn.close()
        if row:
            btc_ret, alt_ret, notes = row
            bias = "BULLISH" if btc_ret > 3 else "BEARISH" if btc_ret < -3 else "NEUTRAL"
            return {"month": month, "btc_return": btc_ret,
                    "alt_return": alt_ret, "bias": bias, "notes": notes}
    except Exception as e:
        logging.debug(f"[Router] seasonality: {e}")
    return {"month": 0, "btc_return": 0, "alt_return": 0, "bias": "NEUTRAL", "notes": ""}

def get_session_context() -> dict:
    """Текущая торговая сессия"""
    hour = datetime.utcnow().hour
    if 0 <= hour < 8:
        session, quality = "asian", "низкая"
    elif 8 <= hour < 12:
        session, quality = "london_open", "высокая"
    elif 12 <= hour < 17:
        session, quality = "london_ny_overlap", "максимальная"
    elif 17 <= hour < 21:
        session, quality = "ny", "высокая"
    else:
        session, quality = "ny_close", "средняя"
    return {"session": session, "hour_utc": hour, "quality": quality,
            "day_of_week": datetime.utcnow().weekday()}

def get_pair_best_hours(symbol: str) -> dict:
    """Лучшие часы для сигналов по этой паре (из истории)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT hour_utc, win_count, loss_count FROM router_market_memory "
            "WHERE symbol=? ORDER BY (win_count * 1.0 / (win_count+loss_count+0.1)) DESC LIMIT 3",
            (symbol,)
        ).fetchall()
        conn.close()
        if rows:
            best = [r[0] for r in rows if r[1]+r[2] > 2]
            return {"best_hours": best, "has_history": True}
    except Exception as e:
        logging.debug(f"[Router] pair_best_hours {symbol}: {e}")
    return {"best_hours": [], "has_history": False}

def record_signal_outcome(symbol: str, direction: str, tf: str,
                           result: str, hour_utc: int):
    """Запоминаем результат сигнала по часу — учим бота лучшему времени входа"""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT id, win_count, loss_count FROM router_market_memory "
            "WHERE symbol=? AND hour_utc=?", (symbol, hour_utc)
        ).fetchone()
        if row:
            wc = row[1] + (1 if result in ("tp1","tp2","tp3","win") else 0)
            lc = row[2] + (1 if result in ("sl","loss") else 0)
            conn.execute("UPDATE router_market_memory SET win_count=?, loss_count=?, "
                        "updated_at=CURRENT_TIMESTAMP WHERE id=?", (wc, lc, row[0]))
        else:
            wc = 1 if result in ("tp1","tp2","tp3","win") else 0
            lc = 1 if result in ("sl","loss") else 0
            conn.execute(
                "INSERT INTO router_market_memory "
                "(symbol, hour_utc, day_of_week, win_count, loss_count) VALUES (?,?,?,?,?)",
                (symbol, hour_utc, datetime.utcnow().weekday(), wc, lc)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.debug(f"[Router] record_signal_outcome: {e}")

# ──────────────────────────────────────────────────────────────
# ДЕТЕКТОР НАКОПЛЕНИЙ (Wyckoff)
# ──────────────────────────────────────────────────────────────
def detect_accumulation(symbol: str) -> dict:
    """
    Wyckoff накопление: узкий диапазон + низкий объём + BB сжатие
    Возвращает score 0-100 и описание фазы
    """
    result = {"score": 0, "phase": "UNKNOWN", "signals": [], "symbol": symbol}
    try:
        candles = _smart_fetch(symbol, "1h", 48)
        if len(candles) < 20:
            return result

        closes = [c["close"] for c in candles]
        volumes = [c.get("volume", 0) for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

        price = closes[-1]
        score = 0
        signals = []

        # 1. Узкий диапазон за 12ч (последние 12 свечей)
        recent_high = max(highs[-12:])
        recent_low = min(lows[-12:])
        range_pct = (recent_high - recent_low) / recent_low * 100 if recent_low > 0 else 100
        if range_pct < 3.0:
            score += 20
            signals.append(f"📦 Узкий диапазон {range_pct:.1f}% за 12ч")
        elif range_pct < 5.0:
            score += 10
            signals.append(f"📦 Диапазон {range_pct:.1f}%")

        # 2. Объём ниже среднего (накопление тихое)
        avg_vol = sum(volumes[:-12]) / max(len(volumes[:-12]), 1) if volumes else 0
        recent_vol = sum(volumes[-12:]) / 12 if volumes else 0
        if avg_vol > 0 and recent_vol < avg_vol * 0.7:
            score += 20
            signals.append("🔇 Тихий объём (накопление)")
        elif avg_vol > 0 and recent_vol < avg_vol * 0.9:
            score += 10

        # 3. Bollinger Bands сжатие
        if len(closes) >= 20:
            ma20 = sum(closes[-20:]) / 20
            std = (sum((c - ma20)**2 for c in closes[-20:]) / 20) ** 0.5
            bb_width = (std * 4) / ma20 * 100 if ma20 > 0 else 100
            if bb_width < 3.0:
                score += 25
                signals.append(f"🎯 BB сжатие {bb_width:.1f}% — взрыв близко")
            elif bb_width < 5.0:
                score += 12

        # 4. Маленькие тела свечей (неопределённость)
        recent_bodies = [abs(c["close"] - c["open"]) / c["open"] * 100
                        for c in candles[-8:] if c["open"] > 0]
        avg_body = sum(recent_bodies) / len(recent_bodies) if recent_bodies else 5
        if avg_body < 0.5:
            score += 15
            signals.append("🕯 Маленькие тела — неопределённость")
        elif avg_body < 1.0:
            score += 8

        # 5. OI растёт при стоячей цене — крупный игрок набирает
        oi_data = get_open_interest(symbol)
        if oi_data["oi_change_4h"] > 5 and range_pct < 5:
            score += 20
            signals.append(f"🐋 OI +{oi_data['oi_change_4h']:.1f}% при боковике — накопление")

        # Определяем фазу Wyckoff
        if score >= 70:
            phase = "WYCKOFF_ACCUMULATION"
        elif score >= 50:
            phase = "CONSOLIDATION"
        elif score >= 30:
            phase = "POSSIBLE_ACCUMULATION"
        else:
            phase = "NO_PATTERN"

        result = {"score": score, "phase": phase, "signals": signals,
                  "symbol": symbol, "range_pct": range_pct}

        # Обновляем intelligence для пары
        _update_pair_intelligence(symbol, accumulation_score=score)

    except Exception as e:
        logging.debug(f"[Router] detect_accumulation {symbol}: {e}")
    return result

def _update_pair_intelligence(symbol: str, **kwargs):
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT symbol FROM router_pair_intelligence WHERE symbol=?", (symbol,)
        ).fetchone()
        if row:
            for k, v in kwargs.items():
                conn.execute(f"UPDATE router_pair_intelligence SET {k}=?, "
                            "updated_at=CURRENT_TIMESTAMP WHERE symbol=?", (v, symbol))
        else:
            conn.execute(
                "INSERT INTO router_pair_intelligence (symbol) VALUES (?)", (symbol,)
            )
            for k, v in kwargs.items():
                conn.execute(f"UPDATE router_pair_intelligence SET {k}=? WHERE symbol=?",
                            (v, symbol))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.debug(f"[Router] _update_pair_intelligence: {e}")

# ──────────────────────────────────────────────────────────────
# CONTRADICTION DETECTOR
# ──────────────────────────────────────────────────────────────
def detect_contradictions(symbol: str, direction: str,
                           fg_value: int, funding_signal: str,
                           dxy_signal: str, smc_grade: str) -> dict:
    """
    Находит противоречия между факторами.
    Например: SMC говорит LONG но F&G = 92 (Extreme Greed)
    """
    conflicts = []
    warnings = []

    # F&G vs направление
    if direction == "BULLISH" and fg_value > 85:
        conflicts.append("SMC LONG но F&G Extreme Greed — рынок перегрет")
    if direction == "BEARISH" and fg_value < 15:
        conflicts.append("SMC SHORT но F&G Extreme Fear — возможен отскок")

    # Funding vs направление
    if direction == "BULLISH" and funding_signal == "HIGH_LONG":
        conflicts.append("LONG сигнал но фандинг перегрет — риск ликвидации лонгов")
    if direction == "BEARISH" and funding_signal == "HIGH_SHORT":
        conflicts.append("SHORT сигнал но шорты перегреты — риск шорт-сквиза")

    # DXY vs крипта
    if direction == "BULLISH" and dxy_signal == "STRONG":
        warnings.append("Растущий доллар давит на крипту — лонг под вопросом")

    # Сезонность
    season = get_seasonality_context()
    if direction == "BULLISH" and season["bias"] == "BEARISH":
        warnings.append(f"Сезонность против лонга ({season['notes']})")
    if direction == "BEARISH" and season["bias"] == "BULLISH":
        warnings.append(f"Сезонность против шорта ({season['notes']})")

    # День недели
    dow = datetime.utcnow().weekday()
    if dow == 0:  # Понедельник
        warnings.append("Понедельник — больше ложных пробоев, осторожно")
    if dow == 4:  # Пятница
        warnings.append("Пятница — часто фиксация прибыли перед выходными")

    severity = "HIGH" if len(conflicts) >= 2 else "MEDIUM" if conflicts else "LOW"
    verdict = "⚠️ КОНФЛИКТ ФАКТОРОВ" if conflicts else ("⚡ Предупреждения" if warnings else "✅ Факторы согласованы")

    result = {
        "conflicts": conflicts, "warnings": warnings,
        "severity": severity, "verdict": verdict,
        "has_conflicts": bool(conflicts)
    }

    # Сохраняем в лог
    if conflicts or warnings:
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT INTO router_contradiction_log (symbol, factors, verdict) VALUES (?,?,?)",
                (symbol, json.dumps(conflicts + warnings), verdict)
            )
            conn.commit()
            conn.close()
        except:
            pass

    return result

# ──────────────────────────────────────────────────────────────
# GROQ КОНТЕКСТ ДЛЯ СИГНАЛА
# ──────────────────────────────────────────────────────────────
def build_signal_context(symbol: str, direction: str, grade: str,
                          confluence: int, regime: str, tf: str) -> str:
    """
    Собирает полный контекст для сигнала:
    OI, Funding, LunarCrush, сессия, сезонность, накопление, противоречия.
    Groq даёт финальный инсайт.
    """
    parts = []

    # OI
    oi = get_open_interest(symbol)
    if oi["oi_change_4h"] != 0:
        parts.append(f"OI: {oi['signal']} ({oi['oi_change_4h']:+.1f}%)")

    # Funding
    funding = get_funding_rate(symbol)
    if funding["warning"]:
        parts.append(funding["warning"])

    # LunarCrush
    lc = get_lunarcrush_score(symbol)
    if lc["galaxy_score"] > 0:
        parts.append(f"Galaxy Score: {lc['galaxy_score']} ({lc['signal']})")

    # Сессия
    sess = get_session_context()
    parts.append(f"Сессия: {sess['session']} (ликвидность: {sess['quality']})")

    # Лучшие часы
    best_h = get_pair_best_hours(symbol)
    if best_h["has_history"] and best_h["best_hours"]:
        h_str = ", ".join(f"{h}:00 UTC" for h in best_h["best_hours"])
        cur = sess["hour_utc"]
        if cur in best_h["best_hours"]:
            parts.append(f"✅ Сейчас {cur}:00 — лучшее время для {symbol}")
        else:
            parts.append(f"⏰ Лучшие часы для {symbol}: {h_str}")

    # Сезонность
    season = get_seasonality_context()
    if season["bias"] != "NEUTRAL":
        emoji = "🟢" if season["bias"] == "BULLISH" else "🔴"
        parts.append(f"{emoji} Сезонность: {season['notes']}")

    # Накопление
    if grade in ("МЕГА ТОП", "ТОП СДЕЛКА"):
        accum = detect_accumulation(symbol)
        if accum["score"] >= 50:
            parts.append(f"📦 Накопление {accum['score']}/100: {', '.join(accum['signals'][:2])}")

    context_str = "\n".join(parts) if parts else ""

    # Groq финальный инсайт
    if GROQ_KEY and grade in ("МЕГА ТОП", "ТОП СДЕЛКА"):
        try:
            prompt = (
                f"Сигнал: {symbol} {direction} {grade} {tf}\n"
                f"Confluence: {confluence}, Режим: {regime}\n"
                f"Контекст:\n{context_str}\n\n"
                "Дай 1-2 предложения: почему сейчас и на что смотреть. "
                "Будь конкретным, без воды."
            )
            insight = _groq(prompt, max_tokens=120,
                           system="Ты опытный крипто трейдер. Отвечай кратко и по делу.")
            if insight:
                context_str += f"\n\n💬 Инсайт: {insight}"
        except Exception as e:
            logging.debug(f"[Router] signal insight: {e}")

    return context_str

# ──────────────────────────────────────────────────────────────
# GROQ САМООБУЧЕНИЕ
# ──────────────────────────────────────────────────────────────
def groq_learn_from_result(symbol: str, direction: str, grade: str,
                            tf: str, result: str, confluence: int,
                            entry: float, sl: float, tp1: float,
                            regime: str, hour_utc: int):
    """
    После закрытия сигнала — Groq анализирует и записывает урок.
    """
    # Записываем в память по часам
    record_signal_outcome(symbol, direction, tf, result, hour_utc)

    # Groq урок
    if not GROQ_KEY:
        return
    is_win = result in ("tp1", "tp2", "tp3", "win")
    rr = abs(tp1 - entry) / abs(entry - sl) if sl != entry else 0

    def _learn():
        try:
            prompt = (
                f"Сигнал закрыт: {symbol} {direction} {grade} {tf}\n"
                f"Результат: {'ПОБЕДА' if is_win else 'СТОП'} | "
                f"Confluence: {confluence} | Режим: {regime} | "
                f"R:R: {rr:.1f} | Час UTC: {hour_utc}\n\n"
                "Ответь JSON: {\"lesson\": \"...\", \"rule\": \"...\", "
                "\"avoid_next_time\": \"...\", \"confidence\": 0.0-1.0}\n"
                "lesson — что случилось, rule — правило на будущее, "
                "avoid_next_time — что избегать если похожая ситуация"
            )
            resp = _groq(prompt, max_tokens=200,
                        system="Ты аналитик трейдинга. Отвечай только JSON.")
            if not resp:
                return
            resp = resp.strip()
            if "```" in resp:
                resp = resp.split("```")[1].replace("json","").strip()
            data = json.loads(resp)
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT INTO router_groq_insights (category, content, confidence) VALUES (?,?,?)",
                (f"trade_lesson_{symbol}",
                 f"{'WIN' if is_win else 'LOSS'} | {data.get('lesson','')} | "
                 f"Rule: {data.get('rule','')} | Avoid: {data.get('avoid_next_time','')}",
                 data.get("confidence", 0.6))
            )
            conn.commit()
            conn.close()
            logging.info(f"[Router] Groq урок сохранён для {symbol} {result}")
        except Exception as e:
            logging.debug(f"[Router] groq_learn_from_result: {e}")

    threading.Thread(target=_learn, daemon=True).start()

def groq_daily_strategy_review():
    """
    Ежедневный обзор — Groq смотрит на все результаты и строит стратегию.
    Запускать раз в день в 05:00.
    """
    if not GROQ_KEY:
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        # Собираем статистику за 7 дней
        rows = conn.execute(
            "SELECT symbol, grade, tf, regime, result, COUNT(*) as cnt "
            "FROM router_signal_history "
            "WHERE created_at > datetime('now', '-7 days') "
            "GROUP BY symbol, grade, tf, result ORDER BY cnt DESC LIMIT 30"
        ).fetchall()
        # Последние инсайты
        lessons = conn.execute(
            "SELECT content FROM router_groq_insights "
            "WHERE category LIKE 'trade_lesson_%' "
            "ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        conn.close()

        if not rows and not lessons:
            return

        stats_text = "\n".join(
            f"{r[0]} {r[1]} {r[2]} {r[3]}: {r[4]} × {r[5]}" for r in rows
        )
        lessons_text = "\n".join(l[0] for l in lessons[:10])

        prompt = (
            f"Анализ торговли за 7 дней:\n{stats_text}\n\n"
            f"Последние уроки:\n{lessons_text}\n\n"
            "Составь краткую торговую стратегию (5-7 пунктов): "
            "какие монеты торговать, какие TF, какой режим рынка, "
            "что избегать. Конкретно."
        )
        strategy = _groq(prompt, max_tokens=400,
                        system="Ты опытный трейдер SMC. Пиши конкретные правила.")
        if strategy:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT INTO router_groq_insights (category, content, confidence) VALUES (?,?,?)",
                ("daily_strategy", strategy, 0.8)
            )
            conn.commit()
            conn.close()
            logging.info("[Router] Ежедневная стратегия обновлена")
    except Exception as e:
        logging.error(f"[Router] groq_daily_strategy_review: {e}")

def get_daily_strategy() -> str:
    """Возвращает последнюю дневную стратегию"""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT content, created_at FROM router_groq_insights "
            "WHERE category='daily_strategy' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            return f"📊 Стратегия ({row[1][:10]}):\n{row[0]}"
    except Exception as e:
        logging.debug(f"[Router] get_daily_strategy: {e}")
    return "Стратегия ещё не сформирована — нужно накопить историю сделок"

def get_groq_insights_summary() -> str:
    """Краткая сводка последних инсайтов Groq для меню Мозга"""
    try:
        conn = sqlite3.connect(DB_PATH)
        lessons = conn.execute(
            "SELECT content, created_at FROM router_groq_insights "
            "WHERE category LIKE 'trade_lesson_%' "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        workarounds = conn.execute(
            "SELECT error_pattern, solution, uses FROM router_workarounds "
            "ORDER BY uses DESC LIMIT 3"
        ).fetchall()
        conn.close()

        lines = ["<b>🧠 Роутер — накопленный опыт:</b>\n"]

        if lessons:
            lines.append("<b>Последние уроки Groq:</b>")
            for l in lessons:
                lines.append(f"• {l[0][:100]}...")
        else:
            lines.append("Уроков пока нет — нужны закрытые сделки")

        if workarounds:
            lines.append("\n<b>Workarounds (обходы ошибок):</b>")
            for w in workarounds:
                lines.append(f"• {w[0]}: {w[1][:80]} (использован {w[2]}р)")

        return "\n".join(lines)
    except Exception as e:
        return f"Ошибка чтения инсайтов: {e}"

def get_source_reliability_text() -> str:
    """Таблица надёжности источников для меню"""
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT source, SUM(success_count), SUM(fail_count), AVG(avg_latency) "
            "FROM router_source_map GROUP BY source "
            "ORDER BY (SUM(success_count)*1.0/(SUM(success_count)+SUM(fail_count)+0.1)) DESC"
        ).fetchall()
        conn.close()
        if not rows:
            return "Данных пока нет — накапливаются по мере работы"
        lines = ["<b>📡 Надёжность источников данных:</b>\n"]
        for src, sc, fc, lat in rows:
            total = sc + fc
            wr = sc / total * 100 if total > 0 else 0
            bar = "█" * int(wr/10) + "░" * (10 - int(wr/10))
            lines.append(f"{src}: {bar} {wr:.0f}% ({sc}✅/{fc}❌, {lat:.1f}с)")
        return "\n".join(lines)
    except Exception as e:
        return f"Ошибка: {e}"

# ──────────────────────────────────────────────────────────────
# ГЛАВНЫЙ ИНТЕРФЕЙС — Router класс
# ──────────────────────────────────────────────────────────────
class BrainRouter:
    """
    Единая точка входа для bot.py.
    Использование:
        from brain_router import router
        candles = router.candles("BTCUSDT", "1h", 200)
        ctx = router.signal_context("BTCUSDT", "BULLISH", "1h", 75, "TRENDING", "ТОП СДЕЛКА")
    """

    def __init__(self):
        _init_router_db()
        logging.info("[Router] BrainRouter инициализирован")

    def candles(self, symbol: str, interval: str = "1h", limit: int = 200) -> list:
        """Умное получение свечей с автоматическим обходом недоступных источников"""
        return _smart_fetch(symbol, interval, limit)

    def signal_context(self, symbol: str, direction: str, tf: str,
                       confluence: int, regime: str, grade: str) -> str:
        """Полный контекст для сигнала: OI, Funding, сессия, противоречия, инсайт"""
        return build_signal_context(symbol, direction, grade, confluence, regime, tf)

    def accumulation(self, symbol: str) -> dict:
        """Wyckoff детектор накоплений"""
        return detect_accumulation(symbol)

    def contradictions(self, symbol: str, direction: str,
                       fg: int, funding: str, dxy: str, grade: str) -> dict:
        """Детектор противоречий между факторами"""
        return detect_contradictions(symbol, direction, fg, funding, dxy, grade)

    def learn(self, symbol: str, direction: str, grade: str, tf: str,
              result: str, confluence: int, entry: float, sl: float,
              tp1: float, regime: str):
        """Обучение на результате сделки"""
        hour = datetime.utcnow().hour
        groq_learn_from_result(symbol, direction, grade, tf, result,
                               confluence, entry, sl, tp1, regime, hour)

    def daily_review(self):
        """Ежедневный обзор стратегии"""
        groq_daily_strategy_review()

    def strategy(self) -> str:
        """Текущая дневная стратегия"""
        return get_daily_strategy()

    def insights(self) -> str:
        """Сводка инсайтов для меню Мозга"""
        return get_groq_insights_summary()

    def source_stats(self) -> str:
        """Статистика источников"""
        return get_source_reliability_text()

    def oi(self, symbol: str) -> dict:
        return get_open_interest(symbol)

    def funding(self, symbol: str) -> dict:
        return get_funding_rate(symbol)

    def social(self, symbol: str) -> dict:
        return get_lunarcrush_score(symbol)

    def session(self) -> dict:
        return get_session_context()

    def seasonality(self) -> dict:
        return get_seasonality_context()


# Singleton
router = BrainRouter()
