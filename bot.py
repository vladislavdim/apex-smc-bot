import asyncio
import logging
import os
import requests
import sqlite3
import threading
import time
import json
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

from groq import Groq
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
GROQ_KEY = os.environ.get("GROQ_API_KEY")
TAVILY_KEY = os.environ.get("TAVILY_API_KEY", "")

bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)
groq_client = Groq(api_key=GROQ_KEY)

# ===== HEALTH SERVER =====

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def run_server():
    """Fallback health check - only used if webhook not configured"""
    server = HTTPServer(("0.0.0.0", 10000), HealthHandler)
    server.serve_forever()

# ===== DATABASE =====

def init_db():
    # Добавляем проверку на наличие свечей для ETHUSDT 1h
    if not groq_client.get_candles(symbol='ETHUSDT', timeframe='1h'):
        logging.error('Нет свечей для ETHUSDT 1h')
    conn = sqlite3.connect("brain.db")
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, direction TEXT, signal_type TEXT,
        entry REAL, tp1 REAL, tp2 REAL, tp3 REAL, sl REAL,
        timeframe TEXT, estimated_hours INTEGER, grade TEXT,
        result TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        closed_at TEXT)""")

    c.execute("""CREATE TABLE IF NOT EXISTS knowledge (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT, content TEXT, source TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    c.execute("""CREATE TABLE IF NOT EXISTS user_memory (
        user_id INTEGER PRIMARY KEY,
        name TEXT, profile TEXT, preferences TEXT,
        coins_mentioned TEXT, deposit REAL DEFAULT 0,
        risk_percent REAL DEFAULT 1.0,
        total_messages INTEGER DEFAULT 0,
        first_seen TEXT, last_seen TEXT)""")

    c.execute("""CREATE TABLE IF NOT EXISTS chat_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, role TEXT, content TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    c.execute("""CREATE TABLE IF NOT EXISTS news_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT, content TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    c.execute("""CREATE TABLE IF NOT EXISTS signal_learning (
        symbol TEXT PRIMARY KEY,
        total INTEGER DEFAULT 0,
        wins INTEGER DEFAULT 0,
        losses INTEGER DEFAULT 0,
        avg_hours_to_tp REAL DEFAULT 0,
        best_timeframe TEXT,
        worst_timeframe TEXT,
        win_rate REAL DEFAULT 0,
        last_analysis TEXT)""")

    # Дневник сделок пользователя
    c.execute("""CREATE TABLE IF NOT EXISTS journal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, symbol TEXT, direction TEXT,
        entry REAL, exit_price REAL, result TEXT,
        note TEXT, pnl_percent REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # Алерты на пробой уровней
    c.execute("""CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, symbol TEXT,
        price_level REAL, direction TEXT,
        triggered INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # ===== ОШИБКИ БОТА =====
    c.execute("""CREATE TABLE IF NOT EXISTS bot_errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id INTEGER,
        symbol TEXT,
        direction TEXT,
        entry REAL,
        sl REAL,
        result TEXT,
        error_type TEXT,
        error_description TEXT,
        ai_analysis TEXT,
        ai_lesson TEXT,
        ai_next_time TEXT,
        fixed INTEGER DEFAULT 0,
        fix_description TEXT,
        hours_in_trade REAL,
        market_context TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        fixed_at TEXT)""")

    # Счётчик повторных ошибок
    c.execute("""CREATE TABLE IF NOT EXISTS error_patterns (
        error_type TEXT PRIMARY KEY,
        count INTEGER DEFAULT 1,
        last_seen TEXT,
        rule_added TEXT,
        active INTEGER DEFAULT 1)""")

    # ===== САМООБУЧЕНИЕ =====
    # Живые правила стратегии — бот сам пишет и обновляет
    c.execute("""CREATE TABLE IF NOT EXISTS self_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT,
        rule TEXT,
        confidence REAL DEFAULT 0.5,
        confirmed_by INTEGER DEFAULT 0,
        contradicted_by INTEGER DEFAULT 0,
        source TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # Наблюдения — что бот заметил о рынке
    c.execute("""CREATE TABLE IF NOT EXISTS observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        observation TEXT,
        context TEXT,
        outcome TEXT,
        confirmed INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # Модель рынка — текущее понимание бота о каждой монете
    c.execute("""CREATE TABLE IF NOT EXISTS market_model (
        symbol TEXT PRIMARY KEY,
        trend TEXT,
        key_levels TEXT,
        behavior_notes TEXT,
        best_setup TEXT,
        avoid_conditions TEXT,
        last_updated TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # Версия мозга — лог эволюции
    c.execute("""CREATE TABLE IF NOT EXISTS brain_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT,
        description TEXT,
        impact TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    conn.commit()
    conn.close()

# ===== USER MEMORY =====

def get_user_memory(user_id):
    try:
        conn = sqlite3.connect("brain.db")
        row = conn.execute(
            "SELECT name, profile, preferences, coins_mentioned, total_messages, deposit, risk_percent FROM user_memory WHERE user_id=?",
            (user_id,)
        ).fetchone()
        conn.close()
        if row:
            return {
                "name": row[0] or "", "profile": row[1] or "",
                "preferences": row[2] or "", "coins": row[3] or "",
                "messages": row[4] or 0, "deposit": row[5] or 0,
                "risk": row[6] or 1.0
            }
        return {"name": "", "profile": "", "preferences": "", "coins": "", "messages": 0, "deposit": 0, "risk": 1.0}
    except:
        return {"name": "", "profile": "", "preferences": "", "coins": "", "messages": 0, "deposit": 0, "risk": 1.0}

def update_user_memory(user_id, name="", profile=None, preferences=None, coins=None, deposit=None, risk=None):
    try:
        conn = sqlite3.connect("brain.db")
        now = datetime.now().isoformat()
        existing = conn.execute("SELECT user_id FROM user_memory WHERE user_id=?", (user_id,)).fetchone()
        if existing:
            updates = ["total_messages = total_messages + 1", "last_seen = ?"]
            params = [now]
            if name:
                updates.append("name = ?"); params.append(name)
            if profile:
                updates.append("profile = ?"); params.append(profile)
            if preferences:
                updates.append("preferences = ?"); params.append(preferences)
            if coins:
                updates.append("coins_mentioned = ?"); params.append(coins)
            if deposit is not None:
                updates.append("deposit = ?"); params.append(deposit)
            if risk is not None:
                updates.append("risk_percent = ?"); params.append(risk)
            params.append(user_id)
            conn.execute(f"UPDATE user_memory SET {', '.join(updates)} WHERE user_id=?", params)
        else:
            conn.execute(
                "INSERT INTO user_memory VALUES (?,?,?,?,?,?,?,?,?,?)",
                (user_id, name, profile or "", preferences or "", coins or "", deposit or 0, 1.0, 0, now, now)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Memory error: {e}")

def save_chat_log(user_id, role, content):
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute("INSERT INTO chat_log VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)", (user_id, role, content[:2000]))
        conn.commit()
        conn.close()
    except:
        pass

def get_chat_history(user_id, limit=15):
    try:
        conn = sqlite3.connect("brain.db")
        rows = conn.execute(
            "SELECT role, content FROM chat_log WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit)
        ).fetchall()
        conn.close()
        return list(reversed(rows))
    except:
        return []

def extract_and_save_profile(user_id, user_name, message, ai_response):
    try:
        mem = get_user_memory(user_id)
        prompt = f"""Извлеки факты о трейдере из сообщения. Верни только JSON:
Текущий профиль: {mem["profile"] or "пустой"}
Сообщение: {message}
{{"profile": "1-2 предложения о стиле торговли", "coins": "монеты через запятую", "preferences": "таймфрейм, стиль, риск"}}"""
        r = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200
        )
        text = r.choices[0].message.content.strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            data = json.loads(text[start:end])
            update_user_memory(user_id, name=user_name,
                               profile=data.get("profile"),
                               coins=data.get("coins"),
                               preferences=data.get("preferences"))
    except Exception as e:
        logging.error(f"Profile extract error: {e}")
        update_user_memory(user_id, name=user_name)

# ===== BINANCE DATA =====

BINANCE = "https://api.binance.com"
BINANCE_F = "https://fapi.binance.com"
BYBIT_URL = "https://api.bybit.com/v5/market/kline"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers"
BYBIT_INTERVALS = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15",
    "30m": "30", "1h": "60", "2h": "120", "4h": "240", "1d": "D"
}

# Динамический кэш топ-50 пар
pairs_cache = []
pairs_cache_time = 0
price_cache = {}
last_price_update = 0
candle_cache = {}  # {symbol_interval: (candles, timestamp)}

def get_top_pairs(limit=50):
    """Топ-N пар по объёму: Bybit → Binance Futures → Binance Spot"""
    global pairs_cache, pairs_cache_time
    if time.time() - pairs_cache_time < 3600 and pairs_cache:
        return pairs_cache

    # 1. Bybit
    try:
        r = requests.get(BYBIT_TICKERS, params={"category": "linear"}, timeout=10)
        data = r.json()["result"]["list"]
        data.sort(key=lambda x: float(x.get("turnover24h", 0)), reverse=True)
        top = [t["symbol"] for t in data if str(t.get("symbol","")).endswith("USDT")][:limit]
        if top:
            pairs_cache = top
            pairs_cache_time = time.time()
            logging.info(f"Пары Bybit: {len(top)} шт, топ: {top[:3]}")
            return pairs_cache
    except Exception as e:
        logging.warning(f"Bybit tickers: {e}")

    # 2. Binance Futures / Spot
    for url in [f"{BINANCE_F}/fapi/v1/ticker/24hr", f"{BINANCE}/api/v3/ticker/24hr"]:
        try:
            r = requests.get(url, timeout=10)
            data = r.json()
            if isinstance(data, list):
                data.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
                top = [t["symbol"] for t in data if str(t.get("symbol","")).endswith("USDT")][:limit]
                if top:
                    pairs_cache = top
                    pairs_cache_time = time.time()
                    return pairs_cache
        except:
            continue

    logging.warning("get_top_pairs: используем fallback список")
    return pairs_cache if pairs_cache else PAIRS

# Обратная совместимость
PAIRS = ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT",
         "TONUSDT","DOGEUSDT","AVAXUSDT","LINKUSDT","ARBUSDT"]

def get_live_prices():
    global price_cache, last_price_update
    if time.time() - last_price_update < 20 and price_cache:
        return price_cache

    # 1. Binance Futures
    try:
        r = requests.get(f"{BINANCE_F}/fapi/v1/ticker/24hr", timeout=10)
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            market = {}
            for t in data:
                if isinstance(t, dict) and str(t.get("symbol","")).endswith("USDT"):
                    market[t["symbol"]] = {
                        "price": float(t["lastPrice"]),
                        "change": float(t["priceChangePercent"]),
                        "volume": float(t.get("quoteVolume", 0))
                    }
            if market:
                price_cache = market
                last_price_update = time.time()
                logging.info(f"Цены: Binance Futures ({len(market)} пар)")
                return price_cache
    except Exception as e:
        logging.warning(f"Binance Futures prices: {e}")

    # 2. Binance Spot
    try:
        r = requests.get(f"{BINANCE}/api/v3/ticker/24hr", timeout=10)
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            market = {}
            for t in data:
                if isinstance(t, dict) and str(t.get("symbol","")).endswith("USDT"):
                    market[t["symbol"]] = {
                        "price": float(t["lastPrice"]),
                        "change": float(t["priceChangePercent"]),
                        "volume": float(t.get("quoteVolume", 0))
                    }
            if market:
                price_cache = market
                last_price_update = time.time()
                logging.info(f"Цены: Binance Spot ({len(market)} пар)")
                return price_cache
    except Exception as e:
        logging.warning(f"Binance Spot prices: {e}")

    # 3. CoinGecko (бесплатный, без ключа)
    try:
        ids = "bitcoin,ethereum,solana,binancecoin,ripple,dogecoin,avalanche-2,chainlink,toncoin"
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": ids, "vs_currencies": "usd", "include_24hr_change": "true", "include_24hr_vol": "true"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json()
        symbol_map = {
            "bitcoin": "BTCUSDT", "ethereum": "ETHUSDT", "solana": "SOLUSDT",
            "binancecoin": "BNBUSDT", "ripple": "XRPUSDT", "dogecoin": "DOGEUSDT",
            "avalanche-2": "AVAXUSDT", "chainlink": "LINKUSDT", "toncoin": "TONUSDT"
        }
        market = {}
        for cg_id, symbol in symbol_map.items():
            if cg_id in data:
                d = data[cg_id]
                market[symbol] = {
                    "price": float(d.get("usd", 0)),
                    "change": float(d.get("usd_24h_change", 0)),
                    "volume": float(d.get("usd_24h_vol", 0))
                }
        if market:
            price_cache = market
            last_price_update = time.time()
            logging.info(f"Цены: CoinGecko ({len(market)} монет)")
            return price_cache
    except Exception as e:
        logging.warning(f"CoinGecko prices: {e}")

    # 4. Kraken как последний fallback
    try:
        pairs_kraken = {"XBTUSD": "BTCUSDT", "ETHUSD": "ETHUSDT", "SOLUSD": "SOLUSDT",
                        "XRPUSD": "XRPUSDT", "DOGEUSD": "DOGEUSDT", "AVAXUSD": "AVAXUSDT",
                        "LINKUSD": "LINKUSDT", "BNBUSD": "BNBUSDT"}
        market = {}
        r = requests.get(
            "https://api.kraken.com/0/public/Ticker",
            params={"pair": ",".join(pairs_kraken.keys())},
            timeout=10
        )
        data = r.json().get("result", {})
        for kraken_pair, our_symbol in pairs_kraken.items():
            if kraken_pair in data:
                d = data[kraken_pair]
                market[our_symbol] = {
                    "price": float(d["c"][0]),
                    "change": 0,
                    "volume": float(d["v"][1])
                }
        if market:
            price_cache = market
            last_price_update = time.time()
            logging.info(f"Цены: Kraken ({len(market)} монет)")
            return price_cache
    except Exception as e:
        logging.warning(f"Kraken prices: {e}")

    logging.error("Все источники цен недоступны")
    return price_cache if price_cache else {}

# ===== МОНИТОРИНГ НАКОПЛЕНИЙ ПЕРЕД ПАМПОМ =====

def detect_accumulation(symbol):
    """
    Wyckoff Accumulation + Volume Analysis:
    - Боковик (низкая волатильность) + сжатие цены
    - Объём ниже среднего (накопление в тишине)
    - Резкий всплеск объёма на последних свечах (кит заходит)
    - Крупные bid ордера в стакане
    Возвращает score 0-100 и детали
    """
    try:
        candles_1h = get_candles(symbol, "1h", 48)
        candles_15m = get_candles(symbol, "15m", 96)

        if len(candles_1h) < 24 or len(candles_15m) < 48:
            return None

        score = 0
        signals = []

        # 1. БОКОВИК — цена в узком диапазоне последние 12 свечей
        last_12 = candles_1h[-12:]
        high_max = max(c["high"] for c in last_12)
        low_min = min(c["low"] for c in last_12)
        price_now = candles_1h[-1]["close"]
        range_pct = (high_max - low_min) / low_min * 100

        if range_pct < 5:
            score += 25
            signals.append(f"✅ Боковик {range_pct:.1f}% за 12ч (накопление)")
        elif range_pct < 8:
            score += 15
            signals.append(f"⚡️ Диапазон {range_pct:.1f}% за 12ч (сжатие)")

        # 2. ОБЪЁМ — среднее vs последние 3 свечи
        all_vols = [c["volume"] for c in candles_1h[:-3]]
        avg_vol = sum(all_vols) / len(all_vols) if all_vols else 1
        recent_vols = [c["volume"] for c in candles_1h[-3:]]
        avg_recent = sum(recent_vols) / len(recent_vols)

        vol_ratio = avg_recent / avg_vol if avg_vol > 0 else 1

        if vol_ratio < 0.6:
            score += 20
            signals.append(f"✅ Объём в {1/vol_ratio:.1f}x ниже среднего (тихое накопление)")
        elif vol_ratio > 2.0:
            score += 20
            signals.append(f"🔥 Всплеск объёма x{vol_ratio:.1f} (кит заходит!)")

        # 3. СВЕЧИ — серия маленьких тел (нерешительность = накопление)
        small_candles = 0
        for c in last_12:
            body = abs(c["close"] - c["open"])
            full_range = c["high"] - c["low"] if c["high"] != c["low"] else 0.001
            if body / full_range < 0.3:
                small_candles += 1

        if small_candles >= 7:
            score += 20
            signals.append(f"✅ {small_candles}/12 свечей с маленьким телом (боковик)")

        # 4. СТАКАН — давление покупателей
        ob = get_orderbook(symbol)
        if ob:
            bid_ask_ratio = ob["bids"] / ob["asks"] if ob["asks"] > 0 else 1
            if bid_ask_ratio > 1.5:
                score += 20
                signals.append(f"✅ Биды x{bid_ask_ratio:.1f} больше асков (кит покупает)")
            elif bid_ask_ratio > 1.2:
                score += 10
                signals.append(f"⚡️ Биды немного давят (bid/ask {bid_ask_ratio:.1f})")

        # 5. BOLLINGER BANDS — сжатие волатильности
        closes = [c["close"] for c in candles_1h[-20:]]
        avg_close = sum(closes) / len(closes)
        std = (sum((x - avg_close) ** 2 for x in closes) / len(closes)) ** 0.5
        bb_width = (std * 2) / avg_close * 100

        if bb_width < 3:
            score += 15
            signals.append(f"✅ BB сжатие {bb_width:.1f}% (взрыв близко!)")
        elif bb_width < 5:
            score += 8
            signals.append(f"⚡️ BB ширина {bb_width:.1f}% (сжимается)")

        if not signals or score < 30:
            return None

        return {
            "symbol": symbol,
            "score": min(score, 100),
            "price": price_now,
            "range_pct": range_pct,
            "vol_ratio": vol_ratio,
            "bb_width": bb_width,
            "signals": signals
        }

    except Exception as e:
        logging.error(f"Accumulation detect error {symbol}: {e}")
        return None


def format_accumulation(acc):
    """Форматируем сигнал накопления"""
    score = acc["score"]

    if score >= 80:
        grade = "🔥🔥🔥 МЕГА НАКОПЛЕНИЕ"
        grade_note = "Высокая вероятность памп"
    elif score >= 60:
        grade = "🔥🔥 СИЛЬНОЕ НАКОПЛЕНИЕ"
        grade_note = "Следи внимательно"
    else:
        grade = "🔥 НАКОПЛЕНИЕ"
        grade_note = "Ранняя стадия"

    signals_text = "\n".join(acc["signals"])
    p = acc["price"]
    ps = f"${p:,.4f}" if p < 1 else f"${p:,.3f}" if p < 100 else f"${p:,.2f}"

    return (
        f"{'━'*26}\n"
        f"{grade}\n"
        f"📦 <b>{acc['symbol']}</b> | {grade_note}\n"
        f"{'━'*26}\n\n"
        f"💰 Цена: <code>{ps}</code>\n"
        f"📊 Скор накопления: <b>{score}/100</b>\n\n"
        f"<b>Признаки:</b>\n{signals_text}\n\n"
        f"💡 <i>Войти можно при пробое диапазона с объёмом</i>\n"
        f"{'━'*26}"
    )

def format_market():
    market = get_live_prices()
    if not market:
        return "Данные недоступны"
    lines = []
    for pair, d in market.items():
        emoji = "🟢" if d["change"] >= 0 else "🔴"
        p = d["price"]
        ps = f"${p:,.2f}" if p >= 1000 else f"${p:.3f}" if p >= 1 else f"${p:.6f}"
        lines.append(f"{emoji} {pair.replace('USDT','')}: {ps} ({d['change']:+.2f}%)")
    return "\n".join(lines)

COINGECKO_IDS = {
    "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "SOLUSDT": "solana",
    "BNBUSDT": "binancecoin", "XRPUSDT": "ripple", "DOGEUSDT": "dogecoin",
    "AVAXUSDT": "avalanche-2", "LINKUSDT": "chainlink", "TONUSDT": "toncoin",
    "ARBUSDT": "arbitrum", "SUIUSDT": "sui", "NEARUSDT": "near",
    "INJUSDT": "injective-protocol", "APTUSDT": "aptos",
    "DOTUSDT": "polkadot", "ADAUSDT": "cardano", "MATICUSDT": "matic-network",
    "LTCUSDT": "litecoin", "ATOMUSDT": "cosmos", "UNIUSDT": "uniswap",
    "OPUSDT": "optimism", "STXUSDT": "blockstack",
    "RENDERUSDT": "render-token", "FETUSDT": "fetch-ai", "WIFUSDT": "dogwifcoin",
    "PEPEUSDT": "pepe", "SHIBUSDT": "shiba-inu", "TRXUSDT": "tron",
    "XLMUSDT": "stellar", "HBARUSDT": "hedera-hashgraph",
}

# Псевдонимы монет для распознавания в тексте
SYMBOL_ALIASES = {
    "btc": "BTCUSDT", "биткоин": "BTCUSDT", "бтк": "BTCUSDT", "bitcoin": "BTCUSDT",
    "eth": "ETHUSDT", "эфир": "ETHUSDT", "эфириум": "ETHUSDT", "ethereum": "ETHUSDT",
    "sol": "SOLUSDT", "соль": "SOLUSDT", "солана": "SOLUSDT", "solana": "SOLUSDT",
    "bnb": "BNBUSDT", "бнб": "BNBUSDT",
    "xrp": "XRPUSDT", "рипл": "XRPUSDT", "ripple": "XRPUSDT",
    "doge": "DOGEUSDT", "додж": "DOGEUSDT", "dogecoin": "DOGEUSDT",
    "avax": "AVAXUSDT", "авакс": "AVAXUSDT",
    "link": "LINKUSDT", "линк": "LINKUSDT",
    "ton": "TONUSDT", "тон": "TONUSDT",
    "arb": "ARBUSDT", "арб": "ARBUSDT",
    "sui": "SUIUSDT", "dot": "DOTUSDT", "полкадот": "DOTUSDT",
    "ada": "ADAUSDT", "кардано": "ADAUSDT",
    "matic": "MATICUSDT", "матик": "MATICUSDT",
    "ltc": "LTCUSDT", "лайткоин": "LTCUSDT",
    "atom": "ATOMUSDT", "near": "NEARUSDT",
    "pepe": "PEPEUSDT", "пепе": "PEPEUSDT",
    "shib": "SHIBUSDT", "шиб": "SHIBUSDT",
    "trx": "TRXUSDT", "трон": "TRXUSDT",
    "wif": "WIFUSDT", "render": "RENDERUSDT", "fet": "FETUSDT",
    "inj": "INJUSDT", "apt": "APTUSDT", "op": "OPUSDT",
}

def get_candles(symbol, interval="1h", limit=200):
    """Свечи: кэш → Bybit (основной) → Binance Futures → Binance Spot → CoinGecko"""
    global candle_cache
    cache_key = f"{symbol}_{interval}"

    if cache_key in candle_cache:
        cached, ts = candle_cache[cache_key]
        if time.time() - ts < 120 and len(cached) >= 20:
            return cached

    # 1. Bybit — работает с Render без блокировок
    try:
        bybit_int = BYBIT_INTERVALS.get(interval, "60")
        r = requests.get(BYBIT_URL, params={
            "category": "linear", "symbol": symbol,
            "interval": bybit_int, "limit": limit
        }, timeout=10)
        data = r.json()
        if data.get("retCode") == 0:
            raw = data["result"]["list"]
            candles = [{"open": float(c[1]), "high": float(c[2]),
                        "low": float(c[3]), "close": float(c[4]),
                        "volume": float(c[5])} for c in reversed(raw)]
            if len(candles) >= 20:
                logging.info(f"Свечи Bybit: {symbol} {interval} {len(candles)}шт")
                candle_cache[cache_key] = (candles, time.time())
                return candles
    except Exception as e:
        logging.warning(f"Bybit klines {symbol} {interval}: {e}")

    # 2. Binance Futures
    try:
        r = requests.get(f"{BINANCE_F}/fapi/v1/klines", params={
            "symbol": symbol, "interval": interval, "limit": limit
        }, timeout=8)
        data = r.json()
        if isinstance(data, list) and len(data) > 5 and isinstance(data[0], list):
            candles = [{"open": float(c[1]), "high": float(c[2]),
                        "low": float(c[3]), "close": float(c[4]),
                        "volume": float(c[5])} for c in data]
            candle_cache[cache_key] = (candles, time.time())
            return candles
    except Exception as e:
        logging.warning(f"Binance Futures {symbol}: {e}")

    # 3. Binance Spot
    try:
        r = requests.get(f"{BINANCE}/api/v3/klines", params={
            "symbol": symbol, "interval": interval, "limit": limit
        }, timeout=8)
        data = r.json()
        if isinstance(data, list) and len(data) > 5 and isinstance(data[0], list):
            candles = [{"open": float(c[1]), "high": float(c[2]),
                        "low": float(c[3]), "close": float(c[4]),
                        "volume": float(c[5])} for c in data]
            candle_cache[cache_key] = (candles, time.time())
            return candles
    except Exception as e:
        logging.warning(f"Binance Spot {symbol}: {e}")

    # 4. CoinGecko — последний резерв
    cg_id = COINGECKO_IDS.get(symbol)
    if cg_id:
        try:
            days_map = {"5m": 1, "15m": 1, "1h": 7, "4h": 30, "1d": 90}
            days = days_map.get(interval, 7)
            r = requests.get(
                f"https://api.coingecko.com/api/v3/coins/{cg_id}/ohlc",
                params={"vs_currency": "usd", "days": days},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=12
            )
            data = r.json()
            if isinstance(data, list) and len(data) > 5:
                candles = [{"open": float(c[1]), "high": float(c[2]),
                            "low": float(c[3]), "close": float(c[4]),
                            "volume": 0.0} for c in data[-limit:]]
                candle_cache[cache_key] = (candles, time.time())
                return candles
        except Exception as e:
            logging.warning(f"CoinGecko {symbol}: {e}")

    # 5. CryptoCompare — дополнительный источник свечей
    cc_candles = get_cryptocompare_candles(symbol, interval, limit)
    if cc_candles and len(cc_candles) >= 20:
        candle_cache[cache_key] = (cc_candles, time.time())
        return cc_candles

    logging.error(f"Нет свечей для {symbol} {interval}")
    return []


def get_orderbook(symbol):
    try:
        r = requests.get(f"{BINANCE}/api/v3/depth", params={"symbol": symbol, "limit": 20}, timeout=8)
        d = r.json()
        bids = sum(float(b[1]) for b in d["bids"])
        asks = sum(float(a[1]) for a in d["asks"])
        return {"bids": bids, "asks": asks, "bias": "BUY" if bids > asks else "SELL"}
    except:
        return None

# ===== SMC ENGINE =====

def find_swings(candles, lookback=5):
    highs, lows = [], []
    for i in range(lookback, len(candles) - lookback):
        wh = [c["high"] for c in candles[i-lookback:i+lookback+1]]
        wl = [c["low"] for c in candles[i-lookback:i+lookback+1]]
        if candles[i]["high"] == max(wh):
            highs.append((i, candles[i]["high"]))
        if candles[i]["low"] == min(wl):
            lows.append((i, candles[i]["low"]))
    return highs, lows

def classify_swings(highs, lows):
    result = []
    for i, (idx, price) in enumerate(highs):
        kind = "HH" if i == 0 or price > highs[i-1][1] else "LH"
        result.append({"idx": idx, "price": price, "kind": kind})
    for i, (idx, price) in enumerate(lows):
        kind = "HL" if i == 0 or price > lows[i-1][1] else "LL"
        result.append({"idx": idx, "price": price, "kind": kind})
    return sorted(result, key=lambda x: x["idx"])

def detect_events(candles, classified):
    """Определяем направление тренда по структуре свингов — не требуем точного пробоя прямо сейчас"""
    events = []
    if not classified:
        return events

    highs = [s for s in classified if s["kind"] in ("HH", "LH")]
    lows = [s for s in classified if s["kind"] in ("HL", "LL")]
    last_close = candles[-1]["close"]

    # Бычья структура: больше HH чем LH — восходящий тренд
    hh_count = sum(1 for s in classified if s["kind"] == "HH")
    lh_count = sum(1 for s in classified if s["kind"] == "LH")
    hl_count = sum(1 for s in classified if s["kind"] == "HL")
    ll_count = sum(1 for s in classified if s["kind"] == "LL")

    # CHoCH/BOS — точный пробой свинга
    if highs and last_close > highs[-1]["price"]:
        etype = "CHoCH" if highs[-1]["kind"] == "LH" else "BOS"
        events.append({"type": etype, "direction": "BULLISH", "level": highs[-1]["price"]})
    if lows and last_close < lows[-1]["price"]:
        etype = "CHoCH" if lows[-1]["kind"] == "HL" else "BOS"
        events.append({"type": etype, "direction": "BEARISH", "level": lows[-1]["price"]})

    # Если нет точного пробоя — определяем по структуре свингов
    if not events:
        if hh_count >= 2 and hl_count >= 1:
            events.append({"type": "TREND", "direction": "BULLISH", "level": 0})
        elif ll_count >= 2 and lh_count >= 1:
            events.append({"type": "TREND", "direction": "BEARISH", "level": 0})
        elif hh_count > ll_count:
            events.append({"type": "BIAS", "direction": "BULLISH", "level": 0})
        elif ll_count > hh_count:
            events.append({"type": "BIAS", "direction": "BEARISH", "level": 0})

    return events

def find_ob(candles, direction):
    for i in range(len(candles) - 2, max(0, len(candles) - 25), -1):
        c = candles[i]
        if direction == "BULLISH" and c["close"] < c["open"]:
            return {"top": max(c["open"], c["close"]), "bottom": min(c["open"], c["close"])}
        if direction == "BEARISH" and c["close"] > c["open"]:
            return {"top": max(c["open"], c["close"]), "bottom": min(c["open"], c["close"])}
    return None

def find_fvg(candles, direction):
    for i in range(len(candles) - 3, max(1, len(candles) - 20), -1):
        if direction == "BULLISH" and candles[i+1]["low"] > candles[i-1]["high"]:
            return {"top": candles[i+1]["low"], "bottom": candles[i-1]["high"]}
        if direction == "BEARISH" and candles[i+1]["high"] < candles[i-1]["low"]:
            return {"top": candles[i-1]["low"], "bottom": candles[i+1]["high"]}
    return None

def smc_on_tf(symbol, interval):
    """SMC анализ на одном таймфрейме — возвращает направление или None"""
    candles = get_candles(symbol, interval, 150)
    if len(candles) < 20:
        return None
    highs, lows = find_swings(candles)
    classified = classify_swings(highs, lows)
    events = detect_events(candles, classified)
    if not events:
        return None
    return events[0]["direction"]  # BULLISH / BEARISH

# ===== МУЛЬТИТАЙМФРЕЙМНЫЙ АНАЛИЗ =====

TF_LABELS = {
    "5m": "5 мин",
    "15m": "15 мин",
    "1h": "1 час",
    "4h": "4 часа",
    "1d": "1 день"
}

TF_HOURS = {
    "5m": 1,
    "15m": 4,
    "1h": 12,
    "4h": 48,
    "1d": 120
}

def multi_tf_analysis(symbol, timeframes=None):
    """
    Анализ по нескольким таймфреймам.
    Возвращает направление, список совпадений и GRADE сигнала.
    """
    if timeframes is None:
        timeframes = ["15m", "1h", "4h"]

    results = {}
    for tf in timeframes:
        direction = smc_on_tf(symbol, tf)
        results[tf] = direction

    bullish = [tf for tf, d in results.items() if d == "BULLISH"]
    bearish = [tf for tf, d in results.items() if d == "BEARISH"]

    total = len(timeframes)
    if len(bullish) == total:
        direction = "BULLISH"
        matched = bullish
    elif len(bearish) == total:
        direction = "BEARISH"
        matched = bearish
    elif len(bullish) > len(bearish):
        direction = "BULLISH"
        matched = bullish
    elif len(bearish) > len(bullish):
        direction = "BEARISH"
        matched = bearish
    else:
        return None  # Нет ясности

    match_count = len(matched)

    # GRADE системы
    if match_count == total and total >= 3:
        grade = "МЕГА ТОП"
        grade_emoji = "🔥🔥🔥"
        stars = "⭐⭐⭐⭐⭐"
    elif match_count >= 3:
        grade = "ТОП СДЕЛКА"
        grade_emoji = "🔥🔥"
        stars = "⭐⭐⭐⭐"
    elif match_count == 2:
        grade = "ХОРОШАЯ"
        grade_emoji = "✅"
        stars = "⭐⭐⭐"
    else:
        grade = "СЛАБАЯ"
        grade_emoji = "⚠️"
        stars = "⭐⭐"

    tf_status = ""
    for tf in timeframes:
        d = results.get(tf)
        if d == "BULLISH":
            icon = "🟢"
        elif d == "BEARISH":
            icon = "🔴"
        else:
            icon = "⚪️"
        tf_status += f"{icon} {TF_LABELS.get(tf, tf)}: {d or 'нет сигнала'}\n"

    return {
        "direction": direction,
        "matched": matched,
        "match_count": match_count,
        "total": total,
        "grade": grade,
        "grade_emoji": grade_emoji,
        "stars": stars,
        "tf_status": tf_status,
        "results": results
    }

# ===== FEAR & GREED INDEX =====

fg_cache = {}
fg_cache_time = 0

def get_fear_greed():
    global fg_cache, fg_cache_time
    if time.time() - fg_cache_time < 3600 and fg_cache:
        return fg_cache
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8)
        data = r.json()["data"][0]
        fg_cache = {
            "value": int(data["value"]),
            "label": data["value_classification"],
            "updated": data["timestamp"]
        }
        fg_cache_time = time.time()
        return fg_cache
    except:
        return None

# ===== FUNDING RATE =====

def get_funding_rate(symbol):
    try:
        r = requests.get(
            f"{BINANCE_F}/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": 1},
            timeout=8
        )
        data = r.json()
        if data:
            return float(data[-1]["fundingRate"]) * 100
        return None
    except:
        return None

# ===== OPEN INTEREST =====

def get_open_interest(symbol):
    try:
        # Текущий OI
        r1 = requests.get(
            f"{BINANCE_F}/fapi/v1/openInterest",
            params={"symbol": symbol},
            timeout=8
        )
        current_oi = float(r1.json()["openInterest"])

        # История OI (последние 4 часа)
        r2 = requests.get(
            f"{BINANCE_F}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "1h", "limit": 5},
            timeout=8
        )
        hist = r2.json()
        if not hist:
            return None

        old_oi = float(hist[0]["sumOpenInterest"])
        change_pct = (current_oi - old_oi) / old_oi * 100 if old_oi > 0 else 0

        return {
            "current": current_oi,
            "change_pct": round(change_pct, 2),
            "trend": "GROWING" if change_pct > 2 else "FALLING" if change_pct < -2 else "FLAT"
        }
    except:
        return None

# ===== DXY SIGNAL =====

dxy_cache = {}
dxy_cache_time = 0

def get_dxy_signal():
    global dxy_cache, dxy_cache_time
    if time.time() - dxy_cache_time < 3600 and dxy_cache:
        return dxy_cache
    try:
        # DXY через Yahoo Finance RSS
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=5d",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if len(closes) < 2:
            return None

        change = (closes[-1] - closes[-3]) / closes[-3] * 100 if len(closes) >= 3 else 0

        dxy_cache = {
            "value": round(closes[-1], 2),
            "change": round(change, 2),
            "signal": "STRONG" if change > 0.3 else "WEAK" if change < -0.3 else "NEUTRAL"
        }
        dxy_cache_time = time.time()
        return dxy_cache
    except:
        return None

# ===== ECONOMIC CALENDAR =====

econ_cache = []
econ_cache_time = 0

def get_upcoming_events():
    """Предупреждение о важных макро-событиях из ForexFactory RSS"""
    global econ_cache, econ_cache_time
    if time.time() - econ_cache_time < 1800 and econ_cache is not None:
        return econ_cache

    high_impact = ["Federal Reserve", "Fed", "CPI", "NFP", "Non-Farm", "GDP",
                   "Interest Rate", "Inflation", "FOMC", "Powell", "SEC", "ECB"]
    try:
        items = parse_rss("https://feeds.reuters.com/reuters/businessNews", "Reuters", limit=10)
        now = datetime.now()
        warnings = []
        for item in items:
            title = item.get("title", "")
            if any(kw.lower() in title.lower() for kw in high_impact):
                warnings.append(f"{item.get('date','')}: {title[:60]}")

        econ_cache = " | ".join(warnings[:2]) if warnings else ""
        econ_cache_time = time.time()
        return econ_cache
    except:
        return ""

# ===== РЫНОЧНЫЙ РЕЖИМ =====

regime_cache = {}
regime_cache_time = {}

def get_market_regime(symbol):
    """
    Определяет режим рынка: TRENDING / SIDEWAYS / VOLATILE
    Основано на ATR, BB Width, последовательности свечей
    """
    global regime_cache, regime_cache_time
    now = time.time()
    if symbol in regime_cache and now - regime_cache_time.get(symbol, 0) < 1800:
        return regime_cache[symbol]

    try:
        candles = get_candles(symbol, "1h", 50)
        if len(candles) < 20:
            return {"mode": "UNKNOWN", "direction": "NONE", "confidence": 0}

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

        # ATR — средний диапазон свечи
        atrs = [highs[i] - lows[i] for i in range(len(candles))]
        avg_atr = sum(atrs[-14:]) / 14
        atr_pct = avg_atr / closes[-1] * 100

        # BB Width
        avg20 = sum(closes[-20:]) / 20
        std20 = (sum((x - avg20) ** 2 for x in closes[-20:]) / 20) ** 0.5
        bb_width = std20 * 4 / avg20 * 100

        # Направление тренда
        ema9 = sum(closes[-9:]) / 9
        ema21 = sum(closes[-21:]) / 21
        trend_dir = "BULLISH" if ema9 > ema21 else "BEARISH"

        # Последовательность — 3+ свечи в одну сторону
        streak = 1
        for i in range(len(candles) - 2, max(len(candles) - 8, 0), -1):
            if (candles[i]["close"] > candles[i]["open"]) == (candles[-1]["close"] > candles[-1]["open"]):
                streak += 1
            else:
                break

        # Режим
        if bb_width < 3 and atr_pct < 1.5:
            mode = "SIDEWAYS"
            confidence = 80
        elif bb_width > 6 or atr_pct > 3:
            mode = "VOLATILE"
            confidence = 70
        elif streak >= 3:
            mode = "TRENDING"
            confidence = 75
        else:
            mode = "TRENDING"
            confidence = 50

        result = {"mode": mode, "direction": trend_dir, "confidence": confidence,
                  "bb_width": round(bb_width, 2), "atr_pct": round(atr_pct, 2)}
        regime_cache[symbol] = result
        regime_cache_time[symbol] = now
        return result
    except:
        return {"mode": "UNKNOWN", "direction": "NONE", "confidence": 0}

# ===== ВЕСА CONFLUENCE (самообучение) =====

def get_confluence_weights(symbol):
    """
    Веса факторов обновляются на основе реальной статистики.
    Если OB давал победы чаще — его вес растёт.
    """
    default = {"mtf": 30, "ob": 25, "fvg": 15, "orderbook": 10, "fg": 10, "funding": 8, "oi": 7, "dxy": 5}
    try:
        conn = sqlite3.connect("brain.db")
        row = conn.execute(
            "SELECT wins, total FROM signal_learning WHERE symbol=?", (symbol,)
        ).fetchone()
        conn.close()

        if not row or row[1] < 10:
            return default

        wr = row[0] / row[1]

        # Динамически регулируем веса на основе WR этого символа
        if wr > 0.65:
            # Хорошая монета — повышаем вес MTF (основной фактор работает)
            default["mtf"] = 35
            default["ob"] = 28
        elif wr < 0.4:
            # Плохая монета — повышаем вес дополнительных фильтров
            default["fg"] = 15
            default["funding"] = 12
            default["oi"] = 10

        return default
    except:
        return default

# ===== ПАМП ДЕТЕКТОР РЕАЛЬНОГО ВРЕМЕНИ (каждые 5 мин) =====

pump_alerted = set()  # Чтобы не спамить одинаковыми

async def realtime_pump_detector():
    """Каждые 5 минут ищет резкий рост объёма x3+ за 3 свечи"""
    try:
        prices = get_live_prices()
        pairs = get_top_pairs(50)

        for symbol in pairs:
            if symbol in pump_alerted:
                continue
            try:
                candles = get_candles(symbol, "5m", 20)
                if len(candles) < 10:
                    continue

                vols = [c["volume"] for c in candles]
                avg_vol = sum(vols[:-3]) / len(vols[:-3]) if len(vols) > 3 else 1
                recent_vol = sum(vols[-3:]) / 3
                vol_spike = recent_vol / avg_vol if avg_vol > 0 else 1

                price_change = (candles[-1]["close"] - candles[-4]["close"]) / candles[-4]["close"] * 100

                if vol_spike >= 3 and abs(price_change) >= 1.5:
                    direction = "🚀 ПАМП" if price_change > 0 else "💥 ДАМП"
                    p = candles[-1]["close"]
                    ps = f"${p:,.4f}" if p < 1 else f"${p:,.2f}"

                    pump_alerted.add(symbol)
                    # Снимаем алерт через 30 мин
                    asyncio.get_event_loop().call_later(1800, lambda s=symbol: pump_alerted.discard(s))

                    if ADMIN_ID:
                        await bot.send_message(
                            ADMIN_ID,
                            f"{direction} ДЕТЕКТОР\n\n"
                            f"⚡️ <b>{symbol}</b> | {ps}\n"
                            f"📊 Объём x{vol_spike:.1f} от среднего\n"
                            f"📈 Изменение цены: {price_change:+.2f}% за 15 мин\n"
                            f"⏰ {datetime.now().strftime('%H:%M:%S')}",
                            parse_mode="HTML"
                        )
                await asyncio.sleep(0.2)
            except:
                pass
    except Exception as e:
        logging.error(f"Pump detector error: {e}")

def full_scan(symbol, timeframe="1h"):
    """Полный SMC анализ с мультитаймфреймом + все новые фильтры"""
    try:
        # ── 0. Рыночный режим — в боковике сигналов нет ──
        regime = get_market_regime(symbol)
        if regime["mode"] == "SIDEWAYS" and regime["confidence"] > 85:
            return None

        # ── 0.5. Проверяем правила самообучения ──
        avoid_rules = get_self_rules("avoid")
        # Если есть сильное правило избегать эту монету — пропускаем
        for rule_row in avoid_rules:
            rule_text = rule_row[0] if len(rule_row) == 1 else rule_row[1]
            conf = rule_row[1] if len(rule_row) >= 2 else 0.5
            if symbol in rule_text and conf >= 0.75:
                logging.info(f"full_scan {symbol}: пропущен по правилу самообучения")
                return None

        # ── 1. Мультитаймфрейм SMC ──
        mtf = multi_tf_analysis(symbol, ["15m", "1h", "4h"])
        if not mtf:
            return None

        direction = mtf["direction"]
        candles = get_candles(symbol, timeframe, 150)
        if len(candles) < 20:
            return None

        price = candles[-1]["close"]
        ob = find_ob(candles, direction)
        fvg = find_fvg(candles, direction)
        ob_data = get_orderbook(symbol)

        # ── 2. Дополнительные фильтры ──
        fg = get_fear_greed()
        funding = get_funding_rate(symbol)
        oi = get_open_interest(symbol)
        dxy = get_dxy_signal()
        econ = get_upcoming_events()

        # ── 3. Взвешенный confluence ──
        weights = get_confluence_weights(symbol)
        confluence = []
        total_weight = 0

        # MTF — базовый вес
        mtf_w = weights.get("mtf", 30)
        confluence.append(f"✅ {mtf['match_count']}/{mtf['total']} ТФ совпали (вес {mtf_w})")
        total_weight += mtf_w

        if ob:
            ob_w = weights.get("ob", 25)
            confluence.append(f"✅ Order Block: {ob['bottom']:.4f}–{ob['top']:.4f} (вес {ob_w})")
            total_weight += ob_w

        if fvg:
            fvg_w = weights.get("fvg", 15)
            confluence.append(f"✅ FVG: {fvg['bottom']:.4f}–{fvg['top']:.4f} (вес {fvg_w})")
            total_weight += fvg_w

        if ob_data:
            match = (direction == "BULLISH" and ob_data["bias"] == "BUY") or \
                    (direction == "BEARISH" and ob_data["bias"] == "SELL")
            if match:
                ob_w2 = weights.get("orderbook", 10)
                confluence.append(f"✅ OrderBook {ob_data['bias']} (вес {ob_w2})")
                total_weight += ob_w2

        # Fear & Greed
        fg_ok = False
        if fg:
            if direction == "BULLISH" and fg["value"] < 75:
                fg_ok = True
                confluence.append(f"✅ F&G: {fg['value']} ({fg['label']}) — не перегрет")
                total_weight += 10
            elif direction == "BEARISH" and fg["value"] > 25:
                fg_ok = True
                confluence.append(f"✅ F&G: {fg['value']} ({fg['label']}) — не в панике")
                total_weight += 10
            else:
                confluence.append(f"⚠️ F&G: {fg['value']} ({fg['label']}) — экстремум, осторожно")

        # Funding Rate
        if funding is not None:
            if direction == "BULLISH" and funding < 0.05:
                confluence.append(f"✅ Funding: {funding:+.4f}% — нейтральный")
                total_weight += 8
            elif direction == "BEARISH" and funding > -0.05:
                confluence.append(f"✅ Funding: {funding:+.4f}% — нейтральный")
                total_weight += 8
            elif abs(funding) > 0.15:
                confluence.append(f"⚠️ Funding: {funding:+.4f}% — перегрев, риск ликвидаций")

        # Open Interest
        if oi:
            if oi["trend"] == "GROWING" and direction == "BULLISH":
                confluence.append(f"✅ OI растёт +{oi['change_pct']:.1f}% — сильный тренд")
                total_weight += 7
            elif oi["trend"] == "GROWING" and direction == "BEARISH":
                confluence.append(f"✅ OI растёт — шортисты добавляют")
                total_weight += 7

        # DXY
        if dxy:
            if direction == "BULLISH" and dxy["signal"] == "WEAK":
                confluence.append(f"✅ DXY слабеет — хорошо для крипты")
                total_weight += 5
            elif direction == "BULLISH" and dxy["signal"] == "STRONG":
                confluence.append(f"⚠️ DXY растёт — риск для лонгов")

        # Режим рынка
        if regime["mode"] == "TRENDING":
            confluence.append(f"✅ Рынок в тренде ({regime['direction']})")
            total_weight += 5

        # Минимальный порог
        if total_weight < 25:
            return None

        # ── 4. Уровни входа ──
        risk = price * 0.015
        if direction == "BULLISH":
            entry = ob["top"] if ob else price
            sl = round(entry - risk, 4)
            tp1 = round(entry + risk * 2, 4)
            tp2 = round(entry + risk * 3, 4)
            tp3 = round(entry + risk * 5, 4)
        else:
            entry = ob["bottom"] if ob else price
            sl = round(entry + risk, 4)
            tp1 = round(entry - risk * 2, 4)
            tp2 = round(entry - risk * 3, 4)
            tp3 = round(entry - risk * 5, 4)

        # ── 5. Время отработки ──
        est_hours, confidence_str, win_rate = get_estimated_time(symbol, timeframe)
        time_str = f"~{est_hours}ч" if est_hours < 24 else f"~{est_hours//24}дн"
        wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"

        save_signal_db(symbol, direction, "MTF", entry, tp1, tp2, tp3, sl, timeframe, est_hours, mtf["grade"])

        # ── 6. AI комментарий к сигналу — с учётом правил самообучения ──
        brain_ctx = get_brain_context(symbol, direction)
        signal_comment = generate_signal_comment(
            symbol, direction, mtf, total_weight, regime, fg, funding, ob, fvg, brain_ctx
        )

        # ── 7. Уровень инвалидации (когда сигнал отменяется) ──
        invalidation = sl  # Если цена закроется за стопом — сигнал недействителен
        inv_text = f"Сигнал отменяется если цена закроется {'ниже' if direction == 'BULLISH' else 'выше'} <code>{invalidation:.4f}</code>"

        # ── 8. Предупреждение об экономических событиях ──
        econ_warn = f"\n⚠️ <b>Макро:</b> {econ}\n" if econ else ""

        emoji = "🟢" if direction == "BULLISH" else "🔴"
        conf_text = "\n".join(confluence)

        return (
            f"{'━'*26}\n"
            f"{mtf['grade_emoji']} <b>{mtf['grade']}</b> [скор: {total_weight}/100]\n"
            f"{emoji} <b>{symbol}</b> — {direction}\n"
            f"{'━'*26}\n\n"
            f"📐 <b>Таймфреймы:</b>\n{mtf['tf_status']}\n"
            f"{mtf['stars']}\n\n"
            f"💰 <b>Вход:</b> <code>{entry:.4f}</code>\n"
            f"🛑 <b>Стоп:</b> <code>{sl:.4f}</code>\n"
            f"🎯 <b>TP1:</b> <code>{tp1:.4f}</code> (+2R)\n"
            f"🎯 <b>TP2:</b> <code>{tp2:.4f}</code> (+3R)\n"
            f"🎯 <b>TP3:</b> <code>{tp3:.4f}</code> (+5R)\n\n"
            f"⏱ <b>Время отработки:</b> {time_str}\n"
            f"📊 <b>Точность:</b> {wr_str} | {confidence_str}\n"
            f"🧠 <b>Режим рынка:</b> {regime['mode']} ({regime['direction']})\n"
            f"{econ_warn}"
            f"❌ <b>Инвалидация:</b> {inv_text}\n\n"
            f"📋 <b>Confluence [{total_weight}/100]:</b>\n{conf_text}\n\n"
            f"💬 <b>APEX думает:</b>\n<i>{signal_comment}</i>\n"
            f"{'━'*26}"
        )
    except Exception as e:
        logging.error(f"Scan error {symbol}: {e}")
        return None


def save_signal_db(symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, timeframe, est_hours, grade):
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute(
            "INSERT INTO signals VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,'pending',CURRENT_TIMESTAMP,NULL)",
            (symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, timeframe, est_hours, grade)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Save signal error: {e}")

# ===== САМООБУЧЕНИЕ =====

def get_estimated_time(symbol, timeframe):
    try:
        conn = sqlite3.connect("brain.db")
        row = conn.execute(
            "SELECT avg_hours_to_tp, win_rate, total FROM signal_learning WHERE symbol=?",
            (symbol,)
        ).fetchone()
        conn.close()
        base = TF_HOURS.get(timeframe, 24)
        if row and row[0] and row[2] > 5:
            wr = row[1]
            confidence = "высокая" if wr > 60 else "средняя" if wr > 45 else "низкая"
            return int(row[0]), confidence, wr
        return base, "нет данных", 0
    except:
        return 24, "нет данных", 0

def check_pending_signals():
    """Проверяем открытые сигналы — сработал ли TP/SL"""
    try:
        conn = sqlite3.connect("brain.db")
        pending = conn.execute(
            "SELECT id, symbol, direction, entry, tp1, tp2, tp3, sl, timeframe, grade, created_at FROM signals WHERE result='pending'"
        ).fetchall()
        conn.close()

        closed = []
        for row in pending:
            sig_id, symbol, direction, entry, tp1, tp2, tp3, sl, timeframe, grade, created_at = row
            prices = get_live_prices()
            if symbol not in prices:
                continue
            current = prices[symbol]["price"]
            created = datetime.fromisoformat(created_at)
            hours_elapsed = (datetime.now() - created).total_seconds() / 3600

            result = None
            hit_tp = None
            if direction == "BULLISH":
                if current >= tp3:
                    result, hit_tp = "tp3", 3
                elif current >= tp2:
                    result, hit_tp = "tp2", 2
                elif current >= tp1:
                    result, hit_tp = "tp1", 1
                elif current <= sl:
                    result = "sl"
            else:
                if current <= tp3:
                    result, hit_tp = "tp3", 3
                elif current <= tp2:
                    result, hit_tp = "tp2", 2
                elif current <= tp1:
                    result, hit_tp = "tp1", 1
                elif current >= sl:
                    result = "sl"

            if not result and hours_elapsed > 72:
                result = "expired"

            if result:
                conn2 = sqlite3.connect("brain.db")
                conn2.execute(
                    "UPDATE signals SET result=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                    (result, sig_id)
                )
                conn2.commit()
                conn2.close()

                is_win = result in ("tp1", "tp2", "tp3")
                update_signal_learning(symbol, hours_elapsed, is_win, timeframe, result)

                # Рефлексия по КАЖДОМУ сигналу
                asyncio.create_task(signal_reflection(
                    symbol, direction, entry, sl, tp1, result, hours_elapsed, timeframe
                ))

                # Самообучение — извлекаем правило из каждой сделки
                candles = get_candles(symbol, timeframe, 50)
                asyncio.create_task(self_learn_from_signal(
                    symbol, direction, entry, result, hours_elapsed, timeframe, candles
                ))

                # Глубокий анализ ошибок только для проигрышей
                if not is_win and result != "expired":
                    asyncio.create_task(deep_error_analysis(
                        sig_id, symbol, direction, entry, sl, result, hours_elapsed, timeframe
                    ))

                closed.append({
                    "signal_id": sig_id,
                    "symbol": symbol,
                    "result": result,
                    "hours": round(hours_elapsed, 1),
                    "grade": grade,
                    "is_win": is_win
                })

        return closed
    except Exception as e:
        logging.error(f"Check signals error: {e}")
        return []

def update_signal_learning(symbol, hours_to_close, is_win, timeframe, result):
    try:
        conn = sqlite3.connect("brain.db")
        existing = conn.execute(
            "SELECT total, wins, losses, avg_hours_to_tp FROM signal_learning WHERE symbol=?",
            (symbol,)
        ).fetchone()
        now = datetime.now().isoformat()
        if existing:
            total = existing[0] + 1
            wins = existing[1] + (1 if is_win else 0)
            losses = existing[2] + (0 if is_win else 1)
            avg_h = (existing[3] * existing[0] + hours_to_close) / total
            wr = round(wins / total * 100, 1)
            conn.execute(
                "UPDATE signal_learning SET total=?, wins=?, losses=?, avg_hours_to_tp=?, win_rate=?, last_analysis=? WHERE symbol=?",
                (total, wins, losses, round(avg_h, 1), wr, now, symbol)
            )
        else:
            conn.execute(
                "INSERT INTO signal_learning VALUES (?,1,?,?,?,?,?,?,?)",
                (symbol, 1 if is_win else 0, 0 if is_win else 1,
                 float(hours_to_close), timeframe, None,
                 100.0 if is_win else 0.0, now)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Learning update error: {e}")

# ===== BACKTESTING =====

def backtest(symbol, timeframe="1h", periods=500):
    """Прогон SMC стратегии на исторических данных"""
    try:
        candles = get_candles(symbol, timeframe, periods)
        if len(candles) < 100:
            return None

        results = {"total": 0, "wins": 0, "losses": 0, "expired": 0}
        trades = []
        lookback = 50

        for i in range(lookback, len(candles) - 20):
            window = candles[i-lookback:i]
            highs, lows = find_swings(window)
            classified = classify_swings(highs, lows)
            events = detect_events(window, classified)

            if not events:
                continue

            event = events[0]
            direction = event["direction"]
            price = candles[i]["close"]
            risk = price * 0.015

            if direction == "BULLISH":
                entry = price
                sl = round(entry - risk, 4)
                tp1 = round(entry + risk * 2, 4)
            else:
                entry = price
                sl = round(entry + risk, 4)
                tp1 = round(entry - risk * 2, 4)

            # Проверяем следующие 20 свечей
            result = "expired"
            for j in range(i+1, min(i+21, len(candles))):
                c = candles[j]
                if direction == "BULLISH":
                    if c["low"] <= sl:
                        result = "loss"
                        break
                    if c["high"] >= tp1:
                        result = "win"
                        break
                else:
                    if c["high"] >= sl:
                        result = "loss"
                        break
                    if c["low"] <= tp1:
                        result = "win"
                        break

            results["total"] += 1
            if result == "win":
                results["wins"] += 1
            elif result == "loss":
                results["losses"] += 1
            else:
                results["expired"] += 1

            if len(trades) < 5:
                trades.append({"direction": direction, "entry": entry, "result": result})

        if results["total"] == 0:
            return None

        wr = round(results["wins"] / results["total"] * 100, 1)
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "total": results["total"],
            "wins": results["wins"],
            "losses": results["losses"],
            "win_rate": wr,
            "periods": periods,
            "trades": trades
        }
    except Exception as e:
        logging.error(f"Backtest error: {e}")
        return None

# ===== РИСК КАЛЬКУЛЯТОР =====

def calc_risk(deposit, risk_percent, entry, sl):
    """Считаем размер позиции"""
    risk_amount = deposit * (risk_percent / 100)
    sl_distance_pct = abs(entry - sl) / entry * 100
    if sl_distance_pct == 0:
        return None
    position_size = risk_amount / (sl_distance_pct / 100)
    leverage = round(position_size / deposit, 1)
    return {
        "risk_amount": round(risk_amount, 2),
        "position_size": round(position_size, 2),
        "sl_distance": round(sl_distance_pct, 2),
        "leverage": min(leverage, 20),
        "contracts": round(position_size / entry, 4)
    }

# ===== АЛЕРТЫ =====

async def check_alerts():
    """Проверяем алерты каждые 5 минут"""
    try:
        conn = sqlite3.connect("brain.db")
        alerts = conn.execute(
            "SELECT id, user_id, symbol, price_level, direction FROM alerts WHERE triggered=0"
        ).fetchall()
        conn.close()

        prices = get_live_prices()
        for alert_id, user_id, symbol, level, direction in alerts:
            if symbol not in prices:
                continue
            current = prices[symbol]["price"]
            triggered = False
            if direction == "above" and current >= level:
                triggered = True
            elif direction == "below" and current <= level:
                triggered = True

            if triggered:
                conn2 = sqlite3.connect("brain.db")
                conn2.execute("UPDATE alerts SET triggered=1 WHERE id=?", (alert_id,))
                conn2.commit()
                conn2.close()
                arrow = "⬆️" if direction == "above" else "⬇️"
                try:
                    await bot.send_message(
                        user_id,
                        f"🔔 <b>АЛЕРТ СРАБОТАЛ!</b>\n\n"
                        f"{arrow} <b>{symbol}</b> достиг уровня <code>{level}</code>\n"
                        f"Текущая цена: <code>{current:.4f}</code>",
                        parse_mode="HTML"
                    )
                except:
                    pass
    except Exception as e:
        logging.error(f"Alert check error: {e}")

# ===== TAVILY =====

def parse_rss(url, source_name, limit=5):
    """Парсим RSS без API ключей"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = "utf-8"
        content = r.text

        items = []
        # Парсим XML вручную
        import re
        entries = re.findall(r"<item>(.*?)</item>", content, re.DOTALL)
        if not entries:
            entries = re.findall(r"<entry>(.*?)</entry>", content, re.DOTALL)

        for entry in entries[:limit]:
            # Заголовок
            title_m = re.search(r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", entry, re.DOTALL)
            title = title_m.group(1).strip() if title_m else ""

            # Ссылка
            link_m = re.search(r"<link[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</link>", entry, re.DOTALL)
            if not link_m:
                link_m = re.search(r"<link[^>]*href=['\"]([^'\"]+)['\"]", entry)
            link = link_m.group(1).strip() if link_m else ""

            # Дата
            date_m = re.search(r"<pubDate>(.*?)</pubDate>", entry, re.DOTALL)
            if not date_m:
                date_m = re.search(r"<published>(.*?)</published>", entry, re.DOTALL)
            raw_date = date_m.group(1).strip() if date_m else ""

            # Парсим дату
            date_str = ""
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(raw_date)
                date_str = dt.strftime("%d.%m %H:%M")
            except:
                try:
                    dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
                    date_str = dt.strftime("%d.%m %H:%M")
                except:
                    date_str = raw_date[:16] if raw_date else ""

            if title:
                items.append({
                    "title": title,
                    "link": link,
                    "date": date_str,
                    "source": source_name
                })

        return items
    except Exception as e:
        logging.error(f"RSS parse error {source_name}: {e}")
        return []


def get_crypto_news(limit=15):
    """
    Собираем новости с нескольких источников:
    CoinTelegraph, CoinDesk, Investing.com, Reuters, Bloomberg
    Без API ключей — прямой RSS парсинг
    """
    sources = [
        ("https://cointelegraph.com/rss", "CoinTelegraph"),
        ("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk"),
        ("https://cryptonews.com/news/feed/", "CryptoNews"),
        ("https://decrypt.co/feed", "Decrypt"),
        ("https://feeds.reuters.com/reuters/businessNews", "Reuters"),
        ("https://investing.com/rss/news_301.rss", "Investing.com"),
        ("https://www.forexfactory.com/ff_calendar_thisweek.xml", "ForexFactory"),
    ]

    all_news = []
    for url, name in sources:
        try:
            items = parse_rss(url, name, limit=4)
            all_news.extend(items)
            time.sleep(0.3)
        except:
            pass

    # Сортируем по дате (свежие первые)
    return all_news[:limit]


def format_news(news_items):
    """Форматируем новости с датой и источником"""
    if not news_items:
        return "Новости временно недоступны"

    lines = []
    for item in news_items:
        date = f"[{item['date']}] " if item["date"] else ""
        source = f" — {item['source']}"
        lines.append(f"📰 {date}<b>{item['title']}</b>{source}")

    return "\n\n".join(lines)


def get_market_impact_news():
    """Макро-новости которые влияют на рынок: ФРС, CPI, геополитика"""
    sources = [
        ("https://feeds.reuters.com/reuters/businessNews", "Reuters"),
        ("https://feeds.bloomberg.com/markets/news.rss", "Bloomberg"),
        ("https://investing.com/rss/news_301.rss", "Investing.com"),
        ("https://feeds.feedburner.com/streetinsider/crypto", "StreetInsider"),
    ]
    all_news = []
    for url, name in sources:
        try:
            items = parse_rss(url, name, limit=3)
            all_news.extend(items)
        except:
            pass
    return all_news[:8]


def tavily_search(query, max_results=4):
    """Tavily если есть ключ, иначе DuckDuckGo"""
    if TAVILY_KEY:
        try:
            r = requests.post(
                "https://api.tavily.com/search",
                json={"api_key": TAVILY_KEY, "query": query, "max_results": max_results, "include_answer": True},
                timeout=20
            )
            data = r.json()
            results = []
            if data.get("answer"):
                results.append(data["answer"])
            for item in data.get("results", []):
                results.append(f"• {item.get('title','')}: {item.get('content','')[:200]}")
            return "\n\n".join(results) if results else ""
        except:
            pass

    # Fallback: DuckDuckGo без API
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": query, "format": "json", "no_html": 1},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json()
        results = []
        if data.get("AbstractText"):
            results.append(data["AbstractText"])
        for item in data.get("RelatedTopics", [])[:3]:
            if isinstance(item, dict) and item.get("Text"):
                results.append(f"• {item['Text'][:200]}")
        return "\n".join(results) if results else ""
    except:
        return ""

def save_news(query, content):
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute("INSERT INTO news_cache VALUES (NULL,?,?,CURRENT_TIMESTAMP)", (query, content[:1000]))
        conn.commit()
        conn.close()
    except:
        pass

def get_recent_news():
    try:
        conn = sqlite3.connect("brain.db")
        rows = conn.execute("SELECT query, content FROM news_cache ORDER BY created_at DESC LIMIT 3").fetchall()
        conn.close()
        return "\n\n".join([f"{r[0]}: {r[1]}" for r in rows])
    except:
        return ""

def save_knowledge(topic, content, source="auto"):
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute("INSERT INTO knowledge VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)", (topic, content, source))
        conn.commit()
        conn.close()
    except:
        pass

def get_knowledge(topic):
    try:
        conn = sqlite3.connect("brain.db")
        rows = conn.execute(
            "SELECT content FROM knowledge WHERE topic LIKE ? ORDER BY created_at DESC LIMIT 3",
            (f"%{topic}%",)
        ).fetchall()
        conn.close()
        return "\n".join([r[0] for r in rows])
    except:
        return ""

# ===== СИСТЕМА САМООБУЧЕНИЯ =====

def get_self_rules(category=None):
    """Получить текущие правила стратегии"""
    try:
        conn = sqlite3.connect("brain.db")
        if category:
            rows = conn.execute(
                "SELECT rule, confidence, confirmed_by FROM self_rules WHERE category=? ORDER BY confidence DESC",
                (category,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT category, rule, confidence FROM self_rules ORDER BY confidence DESC LIMIT 20"
            ).fetchall()
        conn.close()
        return rows
    except:
        return []


def save_self_rule(category, rule, confidence=0.5, source="auto"):
    """Сохранить новое правило или обновить существующее"""
    try:
        conn = sqlite3.connect("brain.db")
        existing = conn.execute(
            "SELECT id, confidence, confirmed_by FROM self_rules WHERE rule LIKE ? AND category=?",
            (f"%{rule[:50]}%", category)
        ).fetchone()

        if existing:
            new_conf = min(1.0, existing[1] + 0.1)
            conn.execute(
                "UPDATE self_rules SET confidence=?, confirmed_by=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (new_conf, existing[2] + 1, existing[0])
            )
            log_brain_event("rule_strengthened", f"{category}: {rule[:80]}", f"confidence → {new_conf:.1f}")
        else:
            conn.execute(
                "INSERT INTO self_rules VALUES (NULL,?,?,?,0,0,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                (category, rule, confidence, source)
            )
            log_brain_event("rule_added", f"{category}: {rule[:80]}", f"confidence={confidence}")

        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"save_self_rule: {e}")


def weaken_rule(rule_text):
    """Ослабить правило если оно дало неправильный результат"""
    try:
        conn = sqlite3.connect("brain.db")
        row = conn.execute(
            "SELECT id, confidence, contradicted_by FROM self_rules WHERE rule LIKE ?",
            (f"%{rule_text[:50]}%",)
        ).fetchone()
        if row:
            new_conf = max(0.0, row[1] - 0.15)
            conn.execute(
                "UPDATE self_rules SET confidence=?, contradicted_by=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (new_conf, row[2] + 1, row[0])
            )
            if new_conf < 0.2:
                conn.execute("DELETE FROM self_rules WHERE id=?", (row[0],))
                log_brain_event("rule_deleted", rule_text[:80], "confidence too low")
            conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"weaken_rule: {e}")


def update_market_model(symbol, candles, direction, result=None):
    """Обновляет модель понимания монеты на основе реальных данных"""
    try:
        if len(candles) < 20:
            return

        closes = [c["close"] for c in candles[-50:]]
        highs = [c["high"] for c in candles[-50:]]
        lows = [c["low"] for c in candles[-50:]]

        # Ключевые уровни
        resistance = max(highs[-20:])
        support = min(lows[-20:])
        current = closes[-1]
        avg = sum(closes[-20:]) / 20
        trend = "BULLISH" if closes[-1] > closes[-10] > closes[-20] else \
                "BEARISH" if closes[-1] < closes[-10] < closes[-20] else "SIDEWAYS"

        # Волатильность
        volatility = round((resistance - support) / avg * 100, 1)
        vol_note = "высокая волатильность" if volatility > 10 else \
                   "умеренная волатильность" if volatility > 5 else "низкая волатильность"

        behavior = f"Тренд: {trend} | Волат: {volatility}% ({vol_note})"
        key_levels = f"Поддержка: {support:.4f} | Сопротивление: {resistance:.4f}"

        best_setup = ""
        avoid = ""
        if result == "win":
            best_setup = f"{direction} работает при текущем тренде {trend}"
        elif result == "loss":
            avoid = f"Избегать {direction} при тренде {trend}"

        conn = sqlite3.connect("brain.db")
        existing = conn.execute("SELECT symbol FROM market_model WHERE symbol=?", (symbol,)).fetchone()
        if existing:
            conn.execute(
                """UPDATE market_model SET trend=?, key_levels=?, behavior_notes=?,
                   best_setup=COALESCE(NULLIF(?, ''), best_setup),
                   avoid_conditions=COALESCE(NULLIF(?,''), avoid_conditions),
                   last_updated=CURRENT_TIMESTAMP WHERE symbol=?""",
                (trend, key_levels, behavior, best_setup, avoid, symbol)
            )
        else:
            conn.execute(
                "INSERT INTO market_model VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP)",
                (symbol, trend, key_levels, behavior, best_setup, avoid)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"update_market_model {symbol}: {e}")


def log_brain_event(event_type, description, impact=""):
    """Лог событий эволюции мозга"""
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute(
            "INSERT INTO brain_log VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)",
            (event_type, description[:200], impact[:100])
        )
        conn.commit()
        conn.close()
    except:
        pass


def save_observation(symbol, observation, context="", outcome=""):
    """Сохранить наблюдение о рынке"""
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute(
            "INSERT INTO observations VALUES (NULL,?,?,?,?,0,CURRENT_TIMESTAMP)",
            (symbol, observation[:300], context[:200], outcome[:100])
        )
        conn.commit()
        conn.close()
    except:
        pass


async def self_learn_from_signal(symbol, direction, entry, result, hours, timeframe, candles):
    """
    Главная функция самообучения — вызывается после каждого закрытого сигнала.
    Анализирует что произошло и обновляет правила/модели.
    """
    try:
        is_win = result in ("tp1", "tp2", "tp3")
        outcome = "WIN" if is_win else "LOSS"

        # 1. Обновляем модель монеты
        update_market_model(symbol, candles, direction, "win" if is_win else "loss")

        # 2. AI анализирует сделку и извлекает правило
        price_now = candles[-1]["close"] if candles else 0
        closes = [c["close"] for c in candles[-20:]] if candles else []
        trend_now = "вверх" if closes and closes[-1] > closes[0] else "вниз"

        prompt = f"""Ты APEX — анализируешь результат своей сделки для самообучения.

СДЕЛКА: {symbol} {direction} на {timeframe}
Вход: {entry} | Результат: {outcome} за {hours:.1f}ч
Тренд был: {trend_now}

Задача: Сформулируй ОДНО конкретное правило которое улучшит будущие решения.

Формат ответа (строго JSON):
{{"category": "entry|exit|filter|timing|risk", "rule": "...", "confidence": 0.1-0.9}}

Примеры правил:
- "Не входить в LONG на {symbol} когда 4H показывает BEARISH"
- "На {timeframe} лучшее время входа — первые 2 часа после открытия свечи"
- "FVG на {symbol} закрывается в среднем за {hours:.0f}ч"

Только JSON, без пояснений."""

        response = ask_groq(prompt, max_tokens=150)
        if response:
            try:
                clean = response.strip().replace("```json", "").replace("```", "")
                data = json.loads(clean)
                rule = data.get("rule", "")
                category = data.get("category", "filter")
                confidence = float(data.get("confidence", 0.5))

                if rule and len(rule) > 10:
                    if is_win:
                        save_self_rule(category, rule, confidence, f"win_{symbol}")
                    else:
                        # При проигрыше сохраняем как анти-паттерн
                        save_self_rule("avoid", f"ИЗБЕГАТЬ: {rule}", confidence * 0.8, f"loss_{symbol}")
            except:
                pass

        # 3. Наблюдение о рынке
        obs = f"{outcome}: {direction} за {hours:.1f}ч при тренде {trend_now}"
        save_observation(symbol, obs, f"TF:{timeframe} entry:{entry:.4f}", outcome)

        # 4. Если выиграли — ищем что сработало
        if is_win and result == "tp3":
            save_self_rule(
                "best_setup",
                f"{symbol} {direction} на {timeframe} — МЕГА сетап, TP3 за {hours:.0f}ч",
                0.8, "tp3_win"
            )

        logging.info(f"Self-learn: {symbol} {outcome} → правило обновлено")

    except Exception as e:
        logging.error(f"self_learn_from_signal: {e}")


async def self_research_loop():
    """
    Фоновый цикл — бот сам ищет паттерны в своей истории
    и обновляет правила каждые 4 часа
    """
    try:
        conn = sqlite3.connect("brain.db")

        # Анализ лучших таймфреймов
        tf_stats = conn.execute(
            """SELECT timeframe,
               SUM(CASE WHEN result LIKE 'tp%' THEN 1 ELSE 0 END) as wins,
               COUNT(*) as total
               FROM signals WHERE result != 'pending'
               GROUP BY timeframe"""
        ).fetchall()

        for tf, wins, total in tf_stats:
            if total >= 5:
                wr = round(wins / total * 100, 1)
                if wr >= 65:
                    save_self_rule("timing",
                        f"Таймфрейм {tf} показывает лучший WR: {wr}%",
                        0.7, "stats_analysis")
                elif wr <= 35:
                    save_self_rule("avoid",
                        f"ИЗБЕГАТЬ таймфрейм {tf} — WR только {wr}%",
                        0.7, "stats_analysis")

        # Анализ лучших монет
        best_coins = conn.execute(
            """SELECT symbol, win_rate, total FROM signal_learning
               WHERE total >= 3 ORDER BY win_rate DESC LIMIT 10"""
        ).fetchall()

        for symbol, wr, total in best_coins:
            if wr >= 70:
                save_self_rule("best_setup",
                    f"{symbol} — высокий WR {wr}% за {total} сигналов",
                    0.75, "coin_analysis")
            elif wr <= 30:
                save_self_rule("avoid",
                    f"ИЗБЕГАТЬ {symbol} — низкий WR {wr}% за {total} сигналов",
                    0.7, "coin_analysis")

        # Анализ времени суток
        hour_stats = conn.execute(
            """SELECT strftime('%H', created_at) as hour,
               SUM(CASE WHEN result LIKE 'tp%' THEN 1 ELSE 0 END) as wins,
               COUNT(*) as total
               FROM signals WHERE result != 'pending'
               GROUP BY hour HAVING total >= 3"""
        ).fetchall()

        best_hour = None
        best_hour_wr = 0
        for hour, wins, total in hour_stats:
            wr = wins / total * 100
            if wr > best_hour_wr:
                best_hour_wr = wr
                best_hour = hour

        if best_hour and best_hour_wr >= 65:
            save_self_rule("timing",
                f"Лучшее время входа ~{best_hour}:00 UTC — WR {best_hour_wr:.0f}%",
                0.65, "time_analysis")

        # Количество активных правил
        rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        conn.close()

        log_brain_event("self_research", f"Проанализировано: ТФ={len(tf_stats)}, монет={len(best_coins)}", f"правил: {rule_count}")
        logging.info(f"Self-research завершён. Активных правил: {rule_count}")

    except Exception as e:
        logging.error(f"self_research_loop: {e}")


def get_brain_context(symbol=None, direction=None):
    """
    Формирует контекст из базы знаний для улучшения сигналов.
    Вызывается перед каждым сигналом.
    """
    try:
        conn = sqlite3.connect("brain.db")
        context_parts = []

        # Общие правила высокой уверенности
        rules = conn.execute(
            "SELECT category, rule FROM self_rules WHERE confidence >= 0.6 ORDER BY confidence DESC LIMIT 10"
        ).fetchall()
        if rules:
            rules_text = "\n".join([f"[{r[0]}] {r[1]}" for r in rules])
            context_parts.append(f"МОИ ПРАВИЛА:\n{rules_text}")

        # Модель конкретной монеты
        if symbol:
            model = conn.execute(
                "SELECT trend, key_levels, behavior_notes, best_setup, avoid_conditions FROM market_model WHERE symbol=?",
                (symbol,)
            ).fetchone()
            if model:
                context_parts.append(
                    f"МОДЕЛЬ {symbol}:\n"
                    f"Тренд: {model[0]} | {model[1]}\n"
                    f"{model[2]}\n"
                    f"Лучший сетап: {model[3] or '—'}\n"
                    f"Избегать: {model[4] or '—'}"
                )

        # Правила избегания
        avoid = conn.execute(
            "SELECT rule FROM self_rules WHERE category='avoid' AND confidence >= 0.5"
        ).fetchall()
        if avoid and direction:
            relevant_avoid = [r[0] for r in avoid if direction in r[0] or (symbol and symbol in r[0])]
            if relevant_avoid:
                context_parts.append("⚠️ ПРЕДУПРЕЖДЕНИЯ:\n" + "\n".join(relevant_avoid[:3]))

        conn.close()
        return "\n\n".join(context_parts)
    except:
        return ""


# ===== СИСТЕМА ОШИБОК БОТА =====

ERROR_TYPES = {
    "against_trend": "Вход против тренда",
    "sideways_entry": "Вход в боковик",
    "bad_rr": "Плохой RR (риск/прибыль)",
    "news_stop": "Новости срезали стоп",
    "false_breakout": "Ложный пробой",
    "early_entry": "Ранний вход (не дождался подтверждения)",
    "late_entry": "Поздний вход (перегнался за ценой)",
    "weak_confluence": "Слабый confluence",
    "unknown": "Неизвестная причина"
}

def classify_error(symbol, direction, entry, sl, result, hours, market_context=""):
    """AI классифицирует тип ошибки"""
    try:
        prompt = f"""Ты SMC трейдер. Классифицируй ошибку в сделке.

СДЕЛКА:
Монета: {symbol} | Направление: {direction}
Вход: {entry} | Стоп: {sl}
Результат: {result} | Время: {hours:.1f}ч
Контекст рынка: {market_context}

Выбери ОДИН тип ошибки из списка:
- against_trend (вход против тренда)
- sideways_entry (вход в боковик)
- bad_rr (плохое соотношение риск/прибыль)
- news_stop (новости срезали стоп)
- false_breakout (ложный пробой)
- early_entry (ранний вход)
- late_entry (поздний вход)
- weak_confluence (слабый confluence)
- unknown

Верни ТОЛЬКО код типа ошибки, без пояснений."""

        result_type = ask_groq(prompt, max_tokens=20)
        if result_type:
            result_type = result_type.strip().lower().split()[0]
            if result_type in ERROR_TYPES:
                return result_type
        return "unknown"
    except:
        return "unknown"

async def deep_error_analysis(signal_id, symbol, direction, entry, sl, result, hours, timeframe):
    """
    Полный AI разбор ошибки:
    - Что пошло не так
    - Урок
    - Как поступать в следующий раз
    Сохраняет в таблицу bot_errors
    """
    try:
        candles = get_candles(symbol, timeframe, 100)
        price_now = candles[-1]["close"] if candles else 0

        # Получаем рыночный контекст на момент сделки
        regime = get_market_regime(symbol)
        fg = get_fear_greed()
        funding = get_funding_rate(symbol)

        market_context = (
            f"Режим: {regime.get('mode','?')} | "
            f"F&G: {fg['value'] if fg else '?'} | "
            f"Funding: {funding:.4f}%" if funding else ""
        )

        # Классифицируем ошибку
        error_type = classify_error(symbol, direction, entry, sl, result, hours, market_context)
        error_label = ERROR_TYPES.get(error_type, "Неизвестно")

        # Ищем в интернете что случилось с монетой
        web_context = ""
        symbol_name = symbol.replace("USDT", "")
        items = parse_rss("https://cointelegraph.com/rss", "CT", limit=15)
        relevant = [i for i in items if symbol_name.lower() in i["title"].lower()]
        if relevant:
            web_context = "\n".join([f"[{i['date']}] {i['title']}" for i in relevant[:3]])

        # Глубокий AI анализ
        analysis_prompt = f"""Ты APEX — проводишь честный разбор своей ошибки.

СДЕЛКА КОТОРАЯ ПРОВАЛИЛАСЬ:
Монета: {symbol} | Направление: {direction}
Вход: {entry} | Стоп: {sl} | Цена сейчас: {price_now}
Время в позиции: {hours:.1f}ч
Тип ошибки: {error_label}
Рыночный контекст: {market_context}

{f"ЧТО ПРОИСХОДИЛО С МОНЕТОЙ:{chr(10)}{web_context}" if web_context else ""}

Дай честный разбор в 3 частях:
1. АНАЛИЗ: Что конкретно пошло не так? (2-3 предложения)
2. УРОК: Какой вывод из этой сделки? (1-2 предложения)
3. В СЛЕДУЮЩИЙ РАЗ: Конкретное правило которое применю. (1 предложение, начни с "В следующий раз...")"""

        full_analysis = ask_groq(analysis_prompt, max_tokens=400)

        # Парсим части ответа
        ai_analysis = ""
        ai_lesson = ""
        ai_next_time = ""

        if full_analysis:
            lines = full_analysis.strip().split("\n")
            current_section = None
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if "АНАЛИЗ" in line.upper() or line.startswith("1."):
                    current_section = "analysis"
                    continue
                elif "УРОК" in line.upper() or line.startswith("2."):
                    current_section = "lesson"
                    continue
                elif "СЛЕДУЮЩИЙ" in line.upper() or line.startswith("3."):
                    current_section = "next"
                    continue

                if current_section == "analysis":
                    ai_analysis += line + " "
                elif current_section == "lesson":
                    ai_lesson += line + " "
                elif current_section == "next":
                    ai_next_time += line + " "

            # Если парсинг не сработал — берём весь текст
            if not ai_analysis:
                ai_analysis = full_analysis[:300]

        # Сохраняем ошибку в БД
        conn = sqlite3.connect("brain.db")
        conn.execute("""INSERT INTO bot_errors VALUES
            (NULL,?,?,?,?,?,?,?,?,?,?,?,0,NULL,?,?,CURRENT_TIMESTAMP,NULL)""",
            (signal_id, symbol, direction, entry, sl, result,
             error_type, error_label,
             ai_analysis.strip(), ai_lesson.strip(), ai_next_time.strip(),
             round(hours, 1), market_context)
        )

        # Обновляем счётчик паттернов ошибок
        existing = conn.execute(
            "SELECT count FROM error_patterns WHERE error_type=?", (error_type,)
        ).fetchone()

        if existing:
            new_count = existing[0] + 1
            conn.execute(
                "UPDATE error_patterns SET count=?, last_seen=CURRENT_TIMESTAMP WHERE error_type=?",
                (new_count, error_type)
            )
            # Если ошибка повторилась 3+ раз — добавляем правило автоматически
            if new_count >= 3:
                rule = await auto_add_rule(error_type, new_count)
                if rule:
                    conn.execute(
                        "UPDATE error_patterns SET rule_added=? WHERE error_type=?",
                        (rule, error_type)
                    )
                    # Уведомляем пользователя
                    if ADMIN_ID:
                        await bot.send_message(
                            ADMIN_ID,
                            f"🧠 <b>Новое правило добавлено в стратегию</b>\n\n"
                            f"Ошибка <b>{ERROR_TYPES[error_type]}</b> повторилась {new_count} раз.\n\n"
                            f"📌 <b>Правило:</b> {rule}",
                            parse_mode="HTML"
                        )
        else:
            conn.execute(
                "INSERT INTO error_patterns VALUES (?,1,CURRENT_TIMESTAMP,NULL,1)",
                (error_type,)
            )

        conn.commit()
        conn.close()

        # Сохраняем в базу знаний
        save_knowledge(
            f"error_{symbol}_{error_type}",
            f"Ошибка: {error_label}. {ai_analysis} Урок: {ai_lesson} Правило: {ai_next_time}",
            "error-analysis"
        )

        logging.info(f"Error analyzed: {symbol} {error_type}")

    except Exception as e:
        logging.error(f"Deep error analysis failed: {e}")


async def auto_add_rule(error_type, count):
    """Когда ошибка повторяется 3+ раз — AI формулирует правило для стратегии"""
    try:
        # Достаём последние 3 анализа этого типа
        conn = sqlite3.connect("brain.db")
        rows = conn.execute(
            "SELECT symbol, ai_analysis, ai_lesson FROM bot_errors WHERE error_type=? ORDER BY id DESC LIMIT 3",
            (error_type,)
        ).fetchall()
        conn.close()

        examples = "\n".join([f"- {r[0]}: {r[1][:100]}" for r in rows])

        prompt = f"""Ошибка типа "{ERROR_TYPES[error_type]}" повторилась {count} раз.

Примеры:
{examples}

Сформулируй ОДНО конкретное правило для стратегии которое предотвратит эту ошибку.
Начни с "Не входить если..." или "Всегда проверять..." или "Обязательно..."
Максимум 1 предложение."""

        rule = ask_groq(prompt, max_tokens=100)
        return rule.strip() if rule else None
    except:
        return None


def generate_signal_comment(symbol, direction, mtf, confluence_score, regime, fg, funding, ob, fvg, brain_ctx=""):
    """Короткий AI-комментарий к сигналу — с учётом накопленного опыта"""
    try:
        factors = []
        if mtf:
            factors.append(f"{mtf['match_count']} из {mtf['total']} таймфреймов показывают {direction}")
        if ob:
            factors.append(f"Order Block на уровне {ob['bottom']:.4f}–{ob['top']:.4f}")
        if fvg:
            factors.append(f"Fair Value Gap заполняется")
        if fg:
            factors.append(f"Fear & Greed = {fg['value']} ({fg['label']})")
        if funding is not None:
            factors.append(f"Funding Rate {funding:+.4f}%")
        if regime:
            factors.append(f"рынок в режиме {regime['mode']}")

        factors_text = ", ".join(factors)
        past_errors = get_knowledge(f"error_{symbol}")

        brain_section = f"\nМОЙ НАКОПЛЕННЫЙ ОПЫТ:\n{brain_ctx[:400]}" if brain_ctx else ""
        errors_section = f"\nПРОШЛЫЕ ОШИБКИ ПО {symbol}: {past_errors[:200]}" if past_errors else ""

        prompt = f"""Ты APEX — торговый бот который учится на каждой сделке.

Сигнал: {symbol} {direction} | Скор: {confluence_score}/100
Факторы: {factors_text}{brain_section}{errors_section}

Напиши 2-3 предложения:
1. Почему даёшь этот сигнал
2. Что ты УЖЕ ЗНАЕШЬ об этой монете из прошлого опыта (если есть)
3. На что обратить внимание

Коротко, конкретно, без воды."""

        comment = ask_groq(prompt, max_tokens=180)
        return comment.strip() if comment else ""
    except:
        return ""


# ===== AI BRAIN =====

def ask_groq(prompt, max_tokens=800):
    try:
        r = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens
        )
        return r.choices[0].message.content
    except Exception as e:
        logging.error(f"Groq error: {e}")
        return None

# ===== СИСТЕМА 1: ГЛУБОКИЙ РЕСЁРЧ =====
# Бот сам ищет инфу в интернете, читает статьи, строит выводы

def deep_research(topic, context=""):
    """
    Многошаговый ресёрч:
    1. Ищем через RSS + DuckDuckGo
    2. Читаем найденное
    3. AI строит выводы и сохраняет факты
    """
    try:
        # Шаг 1: Ищем по RSS источникам
        sources = [
            (f"https://cointelegraph.com/rss/tag/{topic.lower().replace(' ','-')}", "CoinTelegraph"),
            ("https://cointelegraph.com/rss", "CoinTelegraph"),
            ("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk"),
            ("https://decrypt.co/feed", "Decrypt"),
        ]
        raw_news = []
        for url, name in sources[:2]:
            items = parse_rss(url, name, limit=3)
            raw_news.extend(items)

        # Шаг 2: DuckDuckGo поиск
        ddg_result = ""
        try:
            r = requests.get(
                "https://api.duckduckgo.com/",
                params={"q": f"{topic} crypto 2025", "format": "json", "no_html": 1},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            data = r.json()
            if data.get("AbstractText"):
                ddg_result = data["AbstractText"]
            for item in data.get("RelatedTopics", [])[:3]:
                if isinstance(item, dict) and item.get("Text"):
                    ddg_result += f"\n• {item['Text'][:150]}"
        except:
            pass

        # Шаг 3: AI строит глубокий вывод
        news_text = "\n".join([f"[{n['date']}] {n['title']}" for n in raw_news[:6]])
        prompt = f"""Ты APEX — крипто аналитик. Проведи глубокий анализ темы.

ТЕМА: {topic}
КОНТЕКСТ: {context}

НАЙДЕННЫЕ НОВОСТИ:
{news_text}

ДАННЫЕ ИЗ ИНТЕРНЕТА:
{ddg_result[:600]}

Сделай структурированный анализ:
1. Что происходит сейчас
2. Ключевые факты и цифры
3. Влияние на рынок
4. Вывод для трейдера (конкретно)

Только реальные данные, без воды."""

        analysis = ask_groq(prompt, max_tokens=600)

        if analysis:
            # Сохраняем как знание
            save_knowledge(topic, analysis, "deep-research")
            logging.info(f"Deep research done: {topic}")

        return analysis or "Недостаточно данных для анализа"

    except Exception as e:
        logging.error(f"Deep research error: {e}")
        return None


# ===== СИСТЕМА 2: САМО-РЕФЛЕКСИЯ СИГНАЛОВ =====
# После закрытия сигнала бот думает: почему так вышло?

async def signal_reflection(symbol, direction, entry, sl, tp1, result, hours, timeframe):
    """
    Бот сам анализирует закрытый сигнал:
    - Если выиграл — что сработало хорошо
    - Если проиграл — ищет в интернете что случилось с монетой
    - Строит вывод и обновляет стратегию
    """
    try:
        candles = get_candles(symbol, timeframe, 100)
        price_now = candles[-1]["close"] if candles else 0
        is_win = result in ("tp1", "tp2", "tp3")

        # Ищем что случилось с монетой в интернете
        web_context = ""
        if not is_win:
            items = parse_rss("https://cointelegraph.com/rss", "CT", limit=10)
            symbol_name = symbol.replace("USDT", "")
            relevant = [i for i in items if symbol_name.lower() in i["title"].lower()]
            if relevant:
                web_context = "\n".join([f"[{i['date']}] {i['title']}" for i in relevant[:3]])

        prompt = f"""Ты APEX — ты только что закрыл сигнал. Проведи честный разбор.

СИГНАЛ:
Монета: {symbol} | Направление: {direction}
Вход: {entry} | Стоп: {sl} | TP1: {tp1}
Результат: {result} | Время в позиции: {hours:.1f}ч
Цена сейчас: {price_now}

{f"ЧТО ПРОИСХОДИЛО С МОНЕТОЙ:{chr(10)}{web_context}" if web_context else ""}

Ответь на вопросы:
1. Почему сигнал {"сработал" if is_win else "провалился"}?
2. Что нужно учесть в следующий раз для {symbol}?
3. Одно конкретное правило которое добавить в стратегию.

Коротко и честно."""

        reflection = ask_groq(prompt, max_tokens=300)

        if reflection:
            topic = f"reflection_{symbol}_{result}"
            save_knowledge(topic, reflection, "self-reflection")
            logging.info(f"Reflection saved: {symbol} {result}")

        return reflection

    except Exception as e:
        logging.error(f"Reflection error: {e}")
        return None


# ===== СИСТЕМА 3: НОЧНЫЕ ЗАДАЧИ (пока ты спишь) =====
# Бот сам ставит себе задачи и выполняет их

NIGHT_TASKS = [
    "bitcoin dominance trend analysis",
    "ethereum layer2 development news",
    "DXY dollar index crypto correlation",
    "crypto whale movements today",
    "altseason indicators 2025",
    "federal reserve crypto market impact",
    "solana ecosystem updates",
    "defi tvl trends analysis",
]

async def night_brain_tasks():
    """
    Каждые 4 часа бот сам:
    1. Ищет паттерны в своей истории → обновляет правила
    2. Изучает тему → сохраняет в базу знаний
    3. Обновляет модели монет
    4. Уведомляет если нашёл что-то важное
    """
    try:
        now_hour = datetime.now().hour
        task_idx = (now_hour // 4) % len(NIGHT_TASKS)
        topic = NIGHT_TASKS[task_idx]
        logging.info(f"Ночная задача: {topic}")

        # 1. Самообучение — анализ истории сигналов
        rules_before = len(get_self_rules() or [])
        await self_research_loop()
        rules_after_rows = get_self_rules() or []
        rules_after = len(rules_after_rows)
        new_rules = rules_after - rules_before

        # 2. Исследование темы
        old_knowledge = get_knowledge(topic)
        new_analysis = deep_research(topic)

        comparison = ""
        if new_analysis and old_knowledge:
            comparison_prompt = f"""Сравни старый и новый анализ по теме: {topic}

СТАРЫЙ: {old_knowledge[:400]}
НОВЫЙ: {new_analysis[:400]}

Что изменилось? Прошлый прогноз сбылся? Какое правило нужно добавить в стратегию?
Ответ: 2-3 предложения + одно конкретное правило."""

            comparison = ask_groq(comparison_prompt, max_tokens=200)
            if comparison:
                save_knowledge(f"comparison_{topic}", comparison, "self-compare")
                # Если нашли правило — сохраняем
                if "правило" in comparison.lower() or "избегать" in comparison.lower():
                    save_self_rule("market", comparison[:150], 0.6, "night_comparison")

        # 3. Обновляем модели топ-монет
        top_coins = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
        for sym in top_coins:
            try:
                candles = get_candles(sym, "4h", 50)
                if candles:
                    update_market_model(sym, candles, "NEUTRAL")
            except:
                pass

        # 4. Уведомление если нашли что-то важное
        if ADMIN_ID and (new_rules > 0 or new_analysis):
            rules_text = ""
            if new_rules > 0 and rules_after_rows:
                # Показываем самые новые правила
                new_rule_texts = [r[1] if len(r) >= 2 else r[0] for r in rules_after_rows[-new_rules:]]
                rules_text = "\n".join([f"• {r[:80]}" for r in new_rule_texts[:3]])

            msg = (
                f"🧠 <b>APEX обновил мозг</b>\n"
                f"{'━'*22}\n\n"
                f"📚 Тема: {topic}\n"
                f"📌 Новых правил: <b>{new_rules}</b>\n"
                f"🗂 Всего правил: {rules_after}\n"
            )
            if rules_text:
                msg += f"\n<b>Новые правила:</b>\n{rules_text}\n"
            if comparison:
                msg += f"\n<b>Вывод:</b>\n{comparison[:200]}"

            try:
                await bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
            except:
                pass

        logging.info(f"Ночная задача выполнена. Новых правил: {new_rules}, всего: {rules_after}")

    except Exception as e:
        logging.error(f"Night brain error: {e}")


# ===== СИСТЕМА 4: УМНЫЙ ASK_AI С АВТО-РЕСЁРЧЕМ =====

def get_price_realtime(symbol="BTCUSDT"):
    """Получаем цену прямо сейчас из нескольких источников"""
    cg_id = COINGECKO_IDS.get(symbol, "bitcoin")
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd", "include_24hr_change": "true"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8
        )
        data = r.json()
        if cg_id in data:
            return {"price": data[cg_id]["usd"], "change": round(data[cg_id].get("usd_24h_change", 0), 2), "source": "CoinGecko"}
    except:
        pass
    try:
        r = requests.get(f"{BINANCE}/api/v3/ticker/price", params={"symbol": symbol}, timeout=6)
        data = r.json()
        if "price" in data:
            return {"price": float(data["price"]), "change": 0, "source": "Binance"}
    except:
        pass
    return None


# ===== ДОПОЛНИТЕЛЬНЫЕ ИСТОЧНИКИ ДАННЫХ =====

# Yahoo Finance символы для крипты
YAHOO_SYMBOLS = {
    "BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "SOLUSDT": "SOL-USD",
    "BNBUSDT": "BNB-USD", "XRPUSDT": "XRP-USD", "DOGEUSDT": "DOGE-USD",
    "AVAXUSDT": "AVAX-USD", "LINKUSDT": "LINK-USD", "ADAUSDT": "ADA-USD",
    "DOTUSDT": "DOT-USD", "MATICUSDT": "MATIC-USD", "LTCUSDT": "LTC-USD",
    "ATOMUSDT": "ATOM-USD", "TRXUSDT": "TRX-USD", "XLMUSDT": "XLM-USD",
}

# CryptoCompare символы
CRYPTOCOMPARE_SYMS = [
    "BTC","ETH","SOL","BNB","XRP","DOGE","AVAX","LINK","ADA","DOT",
    "MATIC","LTC","ATOM","TRX","XLM","NEAR","ARB","OP","UNI","PEPE",
    "SHIB","TON","SUI","INJ","APT","WIF","RENDER","FET","STX","HBAR"
]

yahoo_cache = {}
yahoo_cache_time = 0
cryptocompare_cache = {}
cryptocompare_cache_time = 0
messari_cache = {}
messari_cache_time = 0


def get_yahoo_finance_prices():
    """Yahoo Finance — цены крипты + DXY + индексы"""
    global yahoo_cache, yahoo_cache_time
    if time.time() - yahoo_cache_time < 60 and yahoo_cache:
        return yahoo_cache
    try:
        syms = " ".join(YAHOO_SYMBOLS.values())
        r = requests.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": syms, "fields": "regularMarketPrice,regularMarketChangePercent"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json()
        result = {}
        reverse = {v: k for k, v in YAHOO_SYMBOLS.items()}
        for item in data.get("quoteResponse", {}).get("result", []):
            sym = item.get("symbol", "")
            our_sym = reverse.get(sym)
            if our_sym and item.get("regularMarketPrice"):
                result[our_sym] = {
                    "price": float(item["regularMarketPrice"]),
                    "change": round(float(item.get("regularMarketChangePercent", 0)), 2),
                    "source": "Yahoo"
                }
        if result:
            yahoo_cache = result
            yahoo_cache_time = time.time()
            logging.info(f"Yahoo Finance: {len(result)} монет")
        return result
    except Exception as e:
        logging.warning(f"Yahoo Finance: {e}")
        return {}


def get_cryptocompare_prices():
    """CryptoCompare — свечи и цены без API ключа"""
    global cryptocompare_cache, cryptocompare_cache_time
    if time.time() - cryptocompare_cache_time < 60 and cryptocompare_cache:
        return cryptocompare_cache
    try:
        fsyms = ",".join(CRYPTOCOMPARE_SYMS)
        r = requests.get(
            "https://min-api.cryptocompare.com/data/pricemultifull",
            params={"fsyms": fsyms, "tsyms": "USD"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json().get("RAW", {})
        result = {}
        for sym, val in data.items():
            usd = val.get("USD", {})
            our_sym = sym + "USDT"
            if usd.get("PRICE"):
                result[our_sym] = {
                    "price": float(usd["PRICE"]),
                    "change": round(float(usd.get("CHANGEPCT24HOUR", 0)), 2),
                    "volume": float(usd.get("VOLUME24HOURTO", 0)),
                    "source": "CryptoCompare"
                }
        if result:
            cryptocompare_cache = result
            cryptocompare_cache_time = time.time()
            logging.info(f"CryptoCompare: {len(result)} монет")
        return result
    except Exception as e:
        logging.warning(f"CryptoCompare: {e}")
        return {}


def get_cryptocompare_candles(symbol, interval="1h", limit=200):
    """Свечи с CryptoCompare — запасной источник для графиков"""
    try:
        base = symbol.replace("USDT", "")
        endpoint_map = {
            "1m": "histominute", "5m": "histominute", "15m": "histominute",
            "30m": "histominute", "1h": "histohour", "4h": "histohour",
            "1d": "histoday"
        }
        endpoint = endpoint_map.get(interval, "histohour")
        aggregate_map = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 1, "4h": 4, "1d": 1
        }
        aggregate = aggregate_map.get(interval, 1)
        r = requests.get(
            f"https://min-api.cryptocompare.com/data/{endpoint}",
            params={"fsym": base, "tsym": "USD", "limit": limit, "aggregate": aggregate},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json().get("Data", [])
        if not data:
            return []
        candles = [{
            "open": float(c["open"]), "high": float(c["high"]),
            "low": float(c["low"]), "close": float(c["close"]),
            "volume": float(c["volumeto"])
        } for c in data if c.get("close")]
        if candles:
            logging.info(f"CryptoCompare candles: {symbol} {interval} {len(candles)}шт")
        return candles
    except Exception as e:
        logging.warning(f"CryptoCompare candles {symbol}: {e}")
        return []


def get_messari_data(symbol):
    """Messari — фундаментальные данные монеты"""
    global messari_cache, messari_cache_time
    cache_key = symbol
    if cache_key in messari_cache and time.time() - messari_cache_time < 3600:
        return messari_cache.get(cache_key)
    try:
        base = symbol.replace("USDT", "").lower()
        r = requests.get(
            f"https://data.messari.io/api/v1/assets/{base}/metrics",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json().get("data", {})
        market = data.get("market_data", {})
        roi = data.get("roi_data", {})
        dev = data.get("developer_activity", {})
        result = {
            "price": market.get("price_usd"),
            "volume_24h": market.get("volume_last_24_hours"),
            "market_cap": market.get("real_volume_last_24_hours"),
            "change_1h": market.get("percent_change_usd_last_1_hour"),
            "change_24h": market.get("percent_change_usd_last_24_hours"),
            "change_7d": market.get("percent_change_usd_last_7_days"),
            "roi_7d": roi.get("percent_change_last_1_week"),
            "github_commits": dev.get("commit_count_4_weeks"),
            "source": "Messari"
        }
        if result["price"]:
            messari_cache[cache_key] = result
            messari_cache_time = time.time()
        return result
    except Exception as e:
        logging.warning(f"Messari {symbol}: {e}")
        return None


def get_all_prices_merged():
    """
    Объединяет данные со ВСЕХ источников.
    Bybit — основной, остальные дополняют недостающие монеты.
    """
    # Параллельно запрашиваем несколько источников
    import concurrent.futures
    result = {}

    def fetch_bybit():
        try:
            r = requests.get(BYBIT_TICKERS, params={"category": "linear"}, timeout=8)
            data = r.json()
            if data.get("retCode") == 0:
                items = data["result"]["list"]
                items.sort(key=lambda x: float(x.get("turnover24h", 0) or 0), reverse=True)
                out = {}
                for t in items:
                    sym = t.get("symbol", "")
                    if sym.endswith("USDT") and t.get("lastPrice"):
                        out[sym] = {
                            "price": float(t["lastPrice"]),
                            "change": round(float(t.get("price24hPcnt", 0)) * 100, 2),
                            "source": "Bybit"
                        }
                    if len(out) >= 50:
                        break
                return out
        except:
            return {}

    def fetch_cryptocompare():
        return get_cryptocompare_prices()

    def fetch_yahoo():
        return get_yahoo_finance_prices()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        f_bybit = ex.submit(fetch_bybit)
        f_cc = ex.submit(fetch_cryptocompare)
        f_yf = ex.submit(fetch_yahoo)

        bybit_data = f_bybit.result()
        cc_data = f_cc.result()
        yf_data = f_yf.result()

    # Bybit — приоритет
    result.update(cc_data)   # сначала CC как база
    result.update(yf_data)   # Yahoo поверх
    result.update(bybit_data)  # Bybit всегда побеждает (самый точный)

    logging.info(f"Merged prices: {len(result)} монет (Bybit:{len(bybit_data)} CC:{len(cc_data)} YF:{len(yf_data)})")
    return result


def get_multiple_prices_realtime():
    """
    Живые цены со ВСЕХ источников параллельно:
    Bybit + CryptoCompare + Yahoo Finance → merge → Binance → CoinGecko fallback
    """
    # Пробуем получить с всех источников параллельно
    try:
        merged = get_all_prices_merged()
        if len(merged) >= 10:
            return merged
    except Exception as e:
        logging.warning(f"get_all_prices_merged failed: {e}")

    # Fallback цепочка если параллельный запрос не сработал
    # 1. Bybit — быстро, много монет, работает с Render
    try:
        r = requests.get(BYBIT_TICKERS, params={"category": "linear"}, timeout=8)
        data = r.json()
        if data.get("retCode") == 0:
            result = {}
            items = data["result"]["list"]
            # Сортируем по объёму
            items.sort(key=lambda x: float(x.get("turnover24h", 0) or 0), reverse=True)
            for t in items:
                sym = t.get("symbol", "")
                if sym.endswith("USDT") and t.get("lastPrice"):
                    try:
                        price = float(t["lastPrice"])
                        change = float(t.get("price24hPcnt", 0)) * 100
                        result[sym] = {"price": price, "change": round(change, 2)}
                    except:
                        pass
                if len(result) >= 30:
                    break
            if result:
                logging.info(f"Живые цены: Bybit ({len(result)} монет)")
                return result
    except Exception as e:
        logging.warning(f"get_multiple_prices_realtime Bybit: {e}")

    # 2. Binance Futures
    try:
        r = requests.get(f"{BINANCE_F}/fapi/v1/ticker/24hr", timeout=8)
        data = r.json()
        if isinstance(data, list):
            result = {}
            data.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            for t in data:
                sym = t.get("symbol", "")
                if sym.endswith("USDT"):
                    result[sym] = {
                        "price": float(t["lastPrice"]),
                        "change": round(float(t["priceChangePercent"]), 2)
                    }
                if len(result) >= 30:
                    break
            if result:
                logging.info(f"Живые цены: Binance Futures ({len(result)} монет)")
                return result
    except Exception as e:
        logging.warning(f"get_multiple_prices_realtime Binance: {e}")

    # 3. Binance Spot
    try:
        r = requests.get(f"{BINANCE}/api/v3/ticker/24hr", timeout=8)
        data = r.json()
        if isinstance(data, list):
            result = {}
            data.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            for t in data:
                sym = t.get("symbol", "")
                if sym.endswith("USDT"):
                    result[sym] = {
                        "price": float(t["lastPrice"]),
                        "change": round(float(t["priceChangePercent"]), 2)
                    }
                if len(result) >= 30:
                    break
            if result:
                logging.info(f"Живые цены: Binance Spot ({len(result)} монет)")
                return result
    except Exception as e:
        logging.warning(f"get_multiple_prices_realtime Binance Spot: {e}")

    # 4. CoinGecko — последний резерв
    try:
        ids = ",".join(set(COINGECKO_IDS.values()))
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": ids, "vs_currencies": "usd", "include_24hr_change": "true"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json()
        result = {}
        reverse_map = {v: k for k, v in COINGECKO_IDS.items()}
        for cg_id, values in data.items():
            symbol = reverse_map.get(cg_id)
            if symbol and "usd" in values:
                result[symbol] = {
                    "price": values["usd"],
                    "change": round(values.get("usd_24h_change", 0), 2)
                }
        if result:
            logging.info(f"Живые цены: CoinGecko ({len(result)} монет)")
        return result
    except Exception as e:
        logging.warning(f"get_multiple_prices_realtime CoinGecko: {e}")
        return {}


def ask_ai(user_id, user_name, user_message):
    mem = get_user_memory(user_id)
    history_rows = get_chat_history(user_id, limit=15)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    history_text = ""
    for row in history_rows:
        role_label = "Ты" if row[0] == "user" else "APEX"
        history_text += f"{role_label}: {row[1]}\n"

    msg_lower = user_message.lower()

    needs_price = any(kw in msg_lower for kw in [
        "цена", "курс", "сколько", "почём", "стоит", "биткоин", "btc", "бтк", "бткс",
        "eth", "эфир", "sol", "соль", "рынок", "памп", "дамп", "упал", "вырос", "сейчас",
        "bnb", "xrp", "рипл", "dogeусdt", "doge", "avax", "link", "цены", "монет",
        "торгуется", "котировки", "стоимость", "baidu", "ton", "near", "sui", "apt",
        "крипта", "альты", "альткоины", "покупать", "продавать", "лонг", "шорт"
    ])
    needs_research = any(kw in msg_lower for kw in [
        "почему", "что случилось", "прогноз", "анализ", "расскажи",
        "новости", "что думаешь", "объясни", "загугли", "найди", "поищи",
        "что происходит", "тренд", "перспективы", "будет"
    ])

    # Живые цены — ВСЕГДА тянем для любого сообщения (не только price-запросов)
    live_prices_text = ""
    prices = get_multiple_prices_realtime()
    if not prices:
        cached = get_live_prices()
        prices = {k: {"price": v["price"], "change": v["change"]} for k, v in list(cached.items())[:20]}

    if prices:
        # Приоритетные монеты показываем первыми
        priority = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
                    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "TONUSDT", "ARBUSDT"]
        ordered = [(s, prices[s]) for s in priority if s in prices]
        others = [(s, d) for s, d in prices.items() if s not in priority]
        all_prices = ordered + others

        lines = []
        for sym, d in all_prices[:20]:
            p = d["price"]
            ps = f"${p:,.2f}" if p >= 100 else f"${p:,.4f}" if p >= 1 else f"${p:.6f}"
            emoji = "🟢" if d["change"] >= 0 else "🔴"
            lines.append(f"{emoji} {sym.replace('USDT','')}: {ps} ({d['change']:+.2f}%)")
        source = "Bybit/Binance"
        live_prices_text = f"ЖИВЫЕ ЦЕНЫ ({source}, {datetime.now().strftime('%H:%M')}):\n" + "\n".join(lines)
    else:
        live_prices_text = "ЦЕНЫ: все источники недоступны — не называй цены из памяти"

    # Поиск в интернете — всегда ищем свежие новости
    research_result = ""
    search_words = [w for w in msg_lower.split() if len(w) > 3 and w not in
                    ("что", "как", "это", "для", "бро", "можешь", "хочу", "нужно")][:4]
    news_items = []
    for feed, name in [
        ("https://cointelegraph.com/rss", "CT"),
        ("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk"),
        ("https://decrypt.co/feed", "Decrypt"),
        ("https://feeds.reuters.com/reuters/businessNews", "Reuters"),
    ]:
        try:
            items = parse_rss(feed, name, limit=5)
            news_items.extend(items)
        except:
            pass
    if search_words:
        relevant = [i for i in news_items if any(w in i["title"].lower() for w in search_words)]
    else:
        relevant = []
    if relevant:
        research_result = "НАШЁЛ В ИНТЕРНЕТЕ:\n" + "\n".join([f"[{i['date']}] {i['title']} — {i['source']}" for i in relevant[:5]])
    elif news_items:
        research_result = "ПОСЛЕДНИЕ НОВОСТИ:\n" + "\n".join([f"[{i['date']}] {i['title']} — {i['source']}" for i in news_items[:4]])

    # Если спрашивают про конкретную монету — тянем фундаментал с Messari
    messari_context = ""
    for alias, sym in SYMBOL_ALIASES.items():
        if alias in msg_lower:
            m_data = get_messari_data(sym)
            if m_data and m_data.get("price"):
                messari_context = (
                    f"ФУНДАМЕНТАЛ {sym} (Messari):\n"
                    f"Цена: ${m_data['price']:.4f} | "
                    f"24ч: {m_data.get('change_24h', 0):+.2f}% | "
                    f"7д: {m_data.get('change_7d', 0):+.2f}%\n"
                    f"GitHub коммитов (4 нед): {m_data.get('github_commits', 'н/д')}"
                )
            break

    knowledge = get_knowledge(user_message[:50])
    recent_news = get_recent_news()

    # Контекст самообучения — что бот узнал о рынке
    brain_context = get_brain_context()

    user_context = ""
    if mem["name"] or mem["profile"]:
        user_context = f"ПОЛЬЗОВАТЕЛЬ:\nИмя: {mem['name'] or user_name} | Сообщений: {mem['messages']}\nПрофиль: {mem['profile'] or 'нет'}\nМонеты: {mem['coins'] or 'нет'}\nДепозит: ${mem['deposit']} | Риск: {mem['risk']}%"

    prompt = f"""Ты APEX — дерзкий AI трейдер с доступом к живым данным рынка. Дата: {now}

{user_context}

{live_prices_text}

{f"РЕСЁРЧ:{chr(10)}{research_result}" if research_result else ""}
{f"НОВОСТИ:{chr(10)}{recent_news[:400]}" if recent_news and not research_result else ""}
{f"ЗНАНИЯ:{chr(10)}{knowledge[:300]}" if knowledge else ""}
{f"ФУНДАМЕНТАЛ (Messari):{chr(10)}{messari_context}" if messari_context else ""}
{f"МОЙ ОПЫТ (самообучение):{chr(10)}{brain_context[:500]}" if brain_context else ""}

ИСТОРИЯ:
{history_text}

ПРАВИЛА:
- Цены ВСЕГДА берёшь из блока "ЖИВЫЕ ЦЕНЫ" выше — они актуальны прямо сейчас
- Если спрашивают про монету которой нет в блоке — честно скажи что нет данных
- Никогда не придумывай цены из памяти
- Говори дерзко, кратко, по делу, как опытный трейдер
- Можешь анализировать рынок, уровни, тренд — но только на основе данных выше
- Применяй свой опыт из "МОЙ ОПЫТ" при анализе

{user_name}: {user_message}
APEX:"""

    return ask_groq(prompt, max_tokens=700)


# ===== KEYBOARDS =====

def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Сканировать рынок", callback_data="menu_scan"),
         InlineKeyboardButton(text="📊 Рынок сейчас", callback_data="menu_market")],
        [InlineKeyboardButton(text="⏱ Выбрать таймфрейм", callback_data="menu_tf"),
         InlineKeyboardButton(text="🔬 Бектест", callback_data="menu_backtest")],
        [InlineKeyboardButton(text="💰 Риск калькулятор", callback_data="menu_risk"),
         InlineKeyboardButton(text="📓 Дневник сделок", callback_data="menu_journal")],
        [InlineKeyboardButton(text="🔔 Алерты", callback_data="menu_alerts"),
         InlineKeyboardButton(text="📈 Статистика", callback_data="menu_stats")],
        [InlineKeyboardButton(text="📰 Новости", callback_data="menu_news"),
         InlineKeyboardButton(text="📦 Накопления", callback_data="menu_pump")],
        [InlineKeyboardButton(text="🏆 Удачные сделки", callback_data="menu_wins"),
         InlineKeyboardButton(text="🔍 Ошибки бота", callback_data="menu_errors")],
        [InlineKeyboardButton(text="🧠 Мозг APEX", callback_data="menu_brain")]
    ])

def tf_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5 мин", callback_data="tf_5m"),
         InlineKeyboardButton(text="15 мин", callback_data="tf_15m"),
         InlineKeyboardButton(text="1 час", callback_data="tf_1h")],
        [InlineKeyboardButton(text="4 часа", callback_data="tf_4h"),
         InlineKeyboardButton(text="1 день", callback_data="tf_1d")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
    ])

def pairs_keyboard(action="scan", page=0):
    """Клавиатура монет с пагинацией — топ-50 из Bybit"""
    all_pairs = get_top_pairs(50)
    page_size = 20  # монет на странице
    total_pages = (len(all_pairs) + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))

    start = page * page_size
    page_pairs = all_pairs[start:start + page_size]

    buttons = []
    row = []
    for i, pair in enumerate(page_pairs):
        row.append(InlineKeyboardButton(
            text=pair.replace("USDT", ""),
            callback_data=f"{action}_{pair}"
        ))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    # Навигация
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"pairs_{action}_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"pairs_{action}_{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def backtest_tf_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="15 мин", callback_data="bt_15m"),
         InlineKeyboardButton(text="1 час", callback_data="bt_1h"),
         InlineKeyboardButton(text="4 часа", callback_data="bt_4h")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
    ])

# Хранилище состояний пользователей
user_states = {}

# ===== HANDLERS =====

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    name = message.from_user.first_name or "трейдер"
    update_user_memory(user_id, name=name)
    mem = get_user_memory(user_id)
    greeting = f"С возвращением, {name}! 👊" if mem["messages"] > 1 else f"Привет, {name}!"
    await message.answer(
        f"⚡️ <b>APEX — AI трейдер по SMC</b>\n\n{greeting}\n\nВыбирай что нужно 👇",
        parse_mode="HTML",
        reply_markup=main_menu()
    )

@dp.message(Command("menu"))
async def cmd_menu(message: types.Message):
    await message.answer("Главное меню 👇", reply_markup=main_menu())

@dp.message(Command("scan"))
async def cmd_scan(message: types.Message):
    await message.answer("Выбери монету для скана:", reply_markup=pairs_keyboard("scan"))

@dp.message(Command("backtest"))
async def cmd_backtest(message: types.Message):
    args = message.text.split()
    if len(args) == 3:
        symbol = args[1].upper()
        tf = args[2].lower()
        await run_backtest(message, symbol, tf)
    else:
        await message.answer(
            "Выбери таймфрейм для бектеста:\n(монета выбирается на следующем шаге)",
            reply_markup=backtest_tf_keyboard()
        )

@dp.message(Command("risk"))
async def cmd_risk(message: types.Message):
    args = message.text.split()
    mem = get_user_memory(message.from_user.id)
    if len(args) == 2:
        try:
            deposit = float(args[1])
            update_user_memory(message.from_user.id, deposit=deposit)
            await message.answer(
                f"✅ Депозит сохранён: <b>${deposit:,.2f}</b>\n\n"
                f"Теперь при каждом сигнале я буду считать размер позиции.\n"
                f"Риск на сделку: {mem['risk']}%\n\n"
                f"Изменить риск: /setrisk 2",
                parse_mode="HTML"
            )
        except:
            await message.answer("Использование: /risk 1000 (размер депозита в $)")
    else:
        deposit = mem["deposit"]
        if deposit > 0:
            await message.answer(
                f"💰 <b>Риск калькулятор</b>\n\n"
                f"Твой депозит: <b>${deposit:,.2f}</b>\n"
                f"Риск на сделку: <b>{mem['risk']}%</b>\n"
                f"Риск в $: <b>${deposit * mem['risk'] / 100:.2f}</b>\n\n"
                f"Изменить депозит: /risk 5000\n"
                f"Изменить риск %: /setrisk 2",
                parse_mode="HTML"
            )
        else:
            await message.answer(
                "💰 <b>Риск калькулятор</b>\n\nУкажи свой депозит:\n/risk 1000",
                parse_mode="HTML"
            )

@dp.message(Command("setrisk"))
async def cmd_setrisk(message: types.Message):
    args = message.text.split()
    if len(args) == 2:
        try:
            risk = float(args[1])
            if 0.1 <= risk <= 10:
                update_user_memory(message.from_user.id, risk=risk)
                await message.answer(f"✅ Риск на сделку: <b>{risk}%</b>", parse_mode="HTML")
            else:
                await message.answer("Риск должен быть от 0.1% до 10%")
        except:
            await message.answer("Использование: /setrisk 2")

@dp.message(Command("alert"))
async def cmd_alert(message: types.Message):
    args = message.text.split()
    if len(args) == 3:
        symbol = args[1].upper()
        try:
            level = float(args[2])
            prices = get_live_prices()
            current = prices.get(symbol, {}).get("price", 0)
            direction = "above" if level > current else "below"
            conn = sqlite3.connect("brain.db")
            conn.execute(
                "INSERT INTO alerts VALUES (NULL,?,?,?,?,0,CURRENT_TIMESTAMP)",
                (message.from_user.id, symbol, level, direction)
            )
            conn.commit()
            conn.close()
            arrow = "⬆️" if direction == "above" else "⬇️"
            await message.answer(
                f"🔔 Алерт установлен!\n{arrow} <b>{symbol}</b> → <code>{level}</code>\nТекущая цена: <code>{current:.4f}</code>",
                parse_mode="HTML"
            )
        except:
            await message.answer("Использование: /alert BTCUSDT 70000")
    else:
        await message.answer(
            "🔔 <b>Алерты на пробой уровня</b>\n\nКогда цена достигает твоего уровня — пишу сразу.\n\nУстановить: /alert BTCUSDT 70000",
            parse_mode="HTML"
        )

@dp.message(Command("journal"))
async def cmd_journal(message: types.Message):
    args = message.text.split(maxsplit=1)
    user_id = message.from_user.id

    if len(args) == 1:
        # Показываем дневник
        conn = sqlite3.connect("brain.db")
        rows = conn.execute(
            "SELECT symbol, direction, entry, exit_price, result, pnl_percent, note, created_at FROM journal WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (user_id,)
        ).fetchall()
        conn.close()

        if not rows:
            await message.answer(
                "📓 <b>Дневник сделок</b>\n\nПусто. Добавь сделку:\n/journal BTC LONG 65000 67000 win\n\n"
                "Формат: /journal МОНЕТА НАПРАВЛЕНИЕ ВХОД ВЫХОД win/loss",
                parse_mode="HTML"
            )
            return

        total = len(rows)
        wins = sum(1 for r in rows if r[4] == "win")
        wr = round(wins / total * 100, 1) if total > 0 else 0

        text = f"📓 <b>Дневник сделок</b> (последние 10)\nWin Rate: {wr}%\n\n"
        for r in rows:
            emoji = "✅" if r[4] == "win" else "❌"
            text += f"{emoji} {r[0]} {r[1]}: {r[2]} → {r[3]} ({r[5]:+.1f}%)\n"

        # AI анализ ошибок
        if len(rows) >= 3:
            losses = [r for r in rows if r[4] == "loss"]
            if losses:
                loss_text = "\n".join([f"{r[0]} {r[1]} вход:{r[2]} выход:{r[3]}" for r in losses[:3]])
                analysis = ask_groq(
                    f"Проанализируй проигрышные сделки трейдера и дай 2-3 конкретных совета:\n{loss_text}",
                    max_tokens=300
                )
                if analysis:
                    text += f"\n🧠 <b>Анализ ошибок:</b>\n{analysis}"

        await message.answer(text, parse_mode="HTML")

    else:
        # Добавляем сделку
        try:
            parts = args[1].split()
            symbol = parts[0].upper()
            direction = parts[1].upper()
            entry = float(parts[2])
            exit_price = float(parts[3])
            result = parts[4].lower()
            note = " ".join(parts[5:]) if len(parts) > 5 else ""

            pnl = (exit_price - entry) / entry * 100
            if direction == "SHORT":
                pnl = -pnl

            conn = sqlite3.connect("brain.db")
            conn.execute(
                "INSERT INTO journal VALUES (NULL,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
                (user_id, symbol, direction, entry, exit_price, result, note, round(pnl, 2))
            )
            conn.commit()
            conn.close()

            emoji = "✅" if result == "win" else "❌"
            await message.answer(
                f"{emoji} Сделка добавлена в дневник\n"
                f"{symbol} {direction}: {entry} → {exit_price} ({pnl:+.2f}%)",
                parse_mode="HTML"
            )
        except:
            await message.answer(
                "Формат: /journal BTC LONG 65000 67000 win [заметка]\n"
                "Пример: /journal ETH SHORT 3200 3050 win взял на OB"
            )

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    user_id = message.from_user.id
    mem = get_user_memory(user_id)
    try:
        conn = sqlite3.connect("brain.db")
        total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result LIKE 'tp%'").fetchone()[0]
        losses = conn.execute("SELECT COUNT(*) FROM signals WHERE result='sl'").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM signals WHERE result='pending'").fetchone()[0]
        top = conn.execute(
            "SELECT symbol, win_rate, total, avg_hours_to_tp FROM signal_learning ORDER BY win_rate DESC LIMIT 5"
        ).fetchall()
        conn.close()
    except:
        total = wins = losses = pending = 0
        top = []

    wr = round(wins / total * 100, 1) if total > 0 else 0
    top_text = "\n".join([f"• {r[0]}: {r[1]:.0f}% WR, avg {r[3]:.0f}ч ({r[2]} сигн.)" for r in top]) or "Нет данных"

    profile_text = ""
    if mem["profile"]:
        profile_text = f"\n\n👤 <b>Что я о тебе знаю:</b>\n{mem['profile']}"
        if mem["coins"]:
            profile_text += f"\n💎 Монеты: {mem['coins']}"
        if mem["deposit"] > 0:
            profile_text += f"\n💰 Депозит: ${mem['deposit']:,.0f} | Риск: {mem['risk']}%"

    await message.answer(
        f"📈 <b>Статистика APEX</b>\n\n"
        f"Сигналов: {total} | ✅ {wins} | ❌ {losses} | ⏳ {pending}\n"
        f"🎯 Win Rate: <b>{wr}%</b>\n\n"
        f"🏆 <b>Топ монеты:</b>\n{top_text}"
        f"{profile_text}",
        parse_mode="HTML"
    )

@dp.message(Command("news"))
async def cmd_news(message: types.Message):
    await message.answer("📰 Собираю свежие новости...")
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    crypto_news = await asyncio.get_event_loop().run_in_executor(None, get_crypto_news)
    macro_news = await asyncio.get_event_loop().run_in_executor(None, get_market_impact_news)
    crypto_text = format_news(crypto_news[:5])
    macro_text = format_news(macro_news[:3])
    all_titles = "\n".join([item["title"] for item in (crypto_news + macro_news)[:10]])
    analysis = ask_groq(
        f"Оцени эти новости для трейдера — что важно прямо сейчас? (3 пункта кратко):\n{all_titles}",
        max_tokens=250
    )
    save_news("crypto news", all_titles[:500])
    msg = (
        f"📰 <b>Новости крипторынка</b>\n"
        f"🕐 {now_str}\n{'━'*24}\n\n"
        f"<b>🔥 Крипто:</b>\n{crypto_text}\n\n"
        f"<b>🌍 Макро:</b>\n{macro_text}\n\n"
        f"<b>⚡️ APEX:</b>\n{analysis or 'Анализирую...'}"
    )
    await message.answer(msg[:4000], parse_mode="HTML")

async def run_backtest(target, symbol, timeframe):
    """Запуск бектеста"""
    send = target.message.answer if hasattr(target, "message") else target.answer
    await send(f"🔬 Запускаю бектест {symbol} {TF_LABELS.get(timeframe, timeframe)}...")
    result = backtest(symbol, timeframe)
    if not result:
        await send("Недостаточно данных для бектеста")
        return

    grade = "🔥 Отличная" if result["win_rate"] >= 60 else "✅ Рабочая" if result["win_rate"] >= 50 else "⚠️ Слабая"
    await send(
        f"🔬 <b>Бектест {symbol} [{TF_LABELS.get(timeframe, timeframe)}]</b>\n\n"
        f"Сигналов: {result['total']}\n"
        f"✅ Выигрыши: {result['wins']}\n"
        f"❌ Проигрыши: {result['losses']}\n"
        f"🎯 Win Rate: <b>{result['win_rate']}%</b>\n"
        f"Оценка: {grade}\n\n"
        f"_На основе {result['periods']} свечей_",
        parse_mode="HTML"
    )

def scan_diagnostics(symbol):
    """Объясняет почему нет сигнала — что именно не прошло"""
    try:
        lines = [f"😴 <b>{symbol} — сигнал не найден</b>\n"]

        candles = get_candles(symbol, "1h", 150)
        if not candles or len(candles) < 20:
            lines.append("⚠️ Данные временно недоступны (CoinGecko rate limit)")
            lines.append("\n<i>Подожди 30 секунд и попробуй снова</i>")
            return "\n".join(lines)

        price = candles[-1]["close"]
        ps = f"${price:,.4f}" if price < 1 else f"${price:,.2f}"
        lines.append(f"💰 Цена: <code>{ps}</code>\n")

        results = {}
        for tf in ["15m", "1h", "4h"]:
            d = smc_on_tf(symbol, tf)
            results[tf] = d
            icon = "🟢" if d == "BULLISH" else "🔴" if d == "BEARISH" else "⚪️"
            lines.append(f"{icon} {TF_LABELS.get(tf, tf)}: {d or 'нет структуры'}")

        bullish = [tf for tf, d in results.items() if d == "BULLISH"]
        bearish = [tf for tf, d in results.items() if d == "BEARISH"]

        if not bullish and not bearish:
            lines.append("\n⚠️ SMC структура не определена — рынок в боковике")
        elif len(bullish) == len(bearish):
            lines.append("\n⚠️ Таймфреймы конфликтуют — нет чёткого направления")
        else:
            direction = "BULLISH" if len(bullish) > len(bearish) else "BEARISH"
            lines.append(f"\n{'🟢' if direction == 'BULLISH' else '🔴'} Направление: {direction}")
            ob = find_ob(candles, direction)
            fvg = find_fvg(candles, direction)
            lines.append(f"{'✅' if ob else '❌'} Order Block: {'найден' if ob else 'не найден'}")
            lines.append(f"{'✅' if fvg else '❌'} FVG: {'найден' if fvg else 'не найден'}")
            regime = get_market_regime(symbol)
            lines.append(f"🧠 Режим: {regime['mode']} (уверенность {regime['confidence']}%)")
            if regime["mode"] == "SIDEWAYS" and regime["confidence"] > 85:
                lines.append("⛔️ Заблокировано: рынок в глубоком боковике")
            lines.append(f"\n📊 Confluence набрал меньше 25 очков — сигнал слабый")

        lines.append("\n<i>Попробуй через 15-30 мин или выбери другую монету</i>")
        return "\n".join(lines)

    except Exception as e:
        return f"😴 {symbol}\n⚠️ Временная ошибка: {e}\n\n<i>Попробуй снова через минуту</i>"

# ===== CALLBACK HANDLERS =====

@dp.callback_query()
async def handle_callback(callback: CallbackQuery):
    data = callback.data
    user_id = callback.from_user.id
    try:
        await callback.answer()
    except Exception:
        pass

    if data == "menu_back":
        await callback.message.edit_text("Главное меню 👇", reply_markup=main_menu())

    elif data == "menu_scan":
        try:
            await callback.message.edit_text(
                "🔍 <b>Выбери монету</b> (топ-50 по объёму):",
                parse_mode="HTML",
                reply_markup=pairs_keyboard("scan", 0)
            )
        except Exception:
            await callback.message.answer(
                "🔍 <b>Выбери монету</b> (топ-50 по объёму):",
                parse_mode="HTML",
                reply_markup=pairs_keyboard("scan", 0)
            )

    elif data.startswith("pairs_"):
        # Пагинация: pairs_scan_0, pairs_scan_1 ...
        parts = data.split("_")
        action = parts[1]
        page = int(parts[2]) if len(parts) > 2 else 0
        try:
            await callback.message.edit_reply_markup(reply_markup=pairs_keyboard(action, page))
        except Exception:
            pass

    elif data == "noop":
        pass  # Кнопка номера страницы — ничего не делаем

    elif data.startswith("patch_apply_"):
        patch_id = data.replace("patch_apply_", "")
        await callback.message.edit_text("⏳ Применяю патч и пушу в GitHub...")
        success, result = await apply_patch(patch_id)
        if success:
            ok_text = "✅ <b>Патч применён!</b>\n\n" + "Commit: <code>" + str(result) + "</code>\n" + "Render сейчас задеплоит новую версию автоматически.\n\n⏳ 1-3 мин."
            await callback.message.edit_text(ok_text, parse_mode="HTML")
        else:
            err_text = "❌ <b>Ошибка при пуше в GitHub:</b>\n<code>" + str(result) + "</code>"
            await callback.message.edit_text(err_text, parse_mode="HTML")

    elif data.startswith("patch_cancel_"):
        patch_id = data.replace("patch_cancel_", "")
        if patch_id in pending_patches:
            del pending_patches[patch_id]
        await callback.message.edit_text("❌ Патч отменён. Код не изменён.")

    elif data == "menu_market":
        await callback.message.edit_text("📊 Собираю данные рынка...")
        market = format_market()
        fg = get_fear_greed()
        dxy = get_dxy_signal()
        regime_btc = get_market_regime("BTCUSDT")
        econ = get_upcoming_events()

        # Блок настроения
        sentiment_block = ""
        if fg:
            fg_bar = "█" * (fg["value"] // 10) + "░" * (10 - fg["value"] // 10)
            fg_emoji = "😱" if fg["value"] < 25 else "😨" if fg["value"] < 45 else "😐" if fg["value"] < 55 else "😊" if fg["value"] < 75 else "🤑"
            sentiment_block += f"{fg_emoji} <b>Fear & Greed:</b> {fg['value']} [{fg_bar}] {fg['label']}\n"

        if dxy:
            dxy_emoji = "📈" if dxy["signal"] == "STRONG" else "📉" if dxy["signal"] == "WEAK" else "➡️"
            warn = " ⚠️ давит на крипту" if dxy["signal"] == "STRONG" else " ✅ хорошо для крипты" if dxy["signal"] == "WEAK" else ""
            sentiment_block += f"{dxy_emoji} <b>DXY:</b> {dxy['value']} ({dxy['change']:+.2f}%){warn}\n"

        if regime_btc:
            regime_emoji = "🔥" if regime_btc["mode"] == "TRENDING" else "😴" if regime_btc["mode"] == "SIDEWAYS" else "⚡️"
            sentiment_block += f"{regime_emoji} <b>Режим BTC:</b> {regime_btc['mode']} {regime_btc['direction']}\n"

        if econ:
            sentiment_block += f"\n⚠️ <b>Макро:</b> {econ}\n"

        comment = ask_groq(
            f"2 предложения по рынку для трейдера:\n{market[:300]}\nF&G:{fg}\nDXY:{dxy}",
            max_tokens=120
        )

        await callback.message.edit_text(
            f"📊 <b>Рынок сейчас</b>\n{'━'*24}\n\n"
            f"{sentiment_block}\n"
            f"<b>Цены:</b>\n{market}\n\n"
            f"💬 {comment or ''}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_market"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_tf":
        await callback.message.edit_text(
            "⏱ <b>Выбери таймфрейм для анализа</b>\n\nПосле выбора бот просканирует все монеты на этом ТФ:",
            parse_mode="HTML", reply_markup=tf_keyboard()
        )

    elif data.startswith("tf_"):
        tf = data.replace("tf_", "")
        await callback.message.edit_text(f"🔍 Сканирую все монеты на {TF_LABELS.get(tf, tf)}...")
        found = []
        for symbol in PAIRS:
            try:
                candles = get_candles(symbol, tf, 100)
                if len(candles) < 20:
                    continue
                highs, lows = find_swings(candles)
                classified = classify_swings(highs, lows)
                events = detect_events(candles, classified)
                if events:
                    direction = events[0]["direction"]
                    price = candles[-1]["close"]
                    emoji = "🟢" if direction == "BULLISH" else "🔴"
                    found.append(f"{emoji} {symbol.replace('USDT','')}: {direction} @ {price:.4f}")
                await asyncio.sleep(0.5)
            except:
                pass

        if found:
            text = f"⏱ <b>Сигналы на {TF_LABELS.get(tf, tf)}:</b>\n\n" + "\n".join(found)
        else:
            text = f"😴 На таймфрейме {TF_LABELS.get(tf, tf)} сигналов нет"

        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_tf")]
        ]))

    elif data.startswith("scan_"):
        symbol = data.replace("scan_", "")
        await callback.message.edit_text(f"🔍 Анализирую {symbol}...")
        sig = full_scan(symbol)

        mem = get_user_memory(user_id)
        risk_text = ""
        if mem["deposit"] > 0 and sig:
            prices = get_live_prices()
            if symbol in prices:
                price = prices[symbol]["price"]
                sl_price = price * 0.985
                rc = calc_risk(mem["deposit"], mem["risk"], price, sl_price)
                if rc:
                    risk_text = (
                        f"\n\n💰 <b>Риск-менеджмент:</b>\n"
                        f"Риск в $: <b>${rc['risk_amount']}</b>\n"
                        f"Размер позиции: <b>${rc['position_size']:.0f}</b>\n"
                        f"Рекомендуемое плечо: <b>x{rc['leverage']}</b>"
                    )

        if sig:
            await callback.message.edit_text(
                sig + risk_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 К монетам", callback_data="menu_scan")]
                ])
            )
        else:
            # Диагностика — объясняем почему нет сигнала
            diag = await asyncio.get_event_loop().run_in_executor(None, scan_diagnostics, symbol)
            await callback.message.edit_text(
                diag,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Повторить", callback_data=f"scan_{symbol}"),
                     InlineKeyboardButton(text="🔙 К монетам", callback_data="menu_scan")]
                ])
            )

    elif data == "menu_backtest":
        await callback.message.edit_text(
            "🔬 <b>Бектест стратегии</b>\n\nВыбери таймфрейм:",
            parse_mode="HTML", reply_markup=backtest_tf_keyboard()
        )

    elif data.startswith("bt_"):
        tf = data.replace("bt_", "")
        user_states[user_id] = {"action": "backtest", "tf": tf}
        await callback.message.edit_text(
            f"Бектест на {TF_LABELS.get(tf, tf)}.\n\nНапиши название монеты (например: BTC или ETHUSDT):"
        )

    elif data == "menu_risk":
        mem = get_user_memory(user_id)
        if mem["deposit"] > 0:
            text = (f"💰 <b>Риск калькулятор</b>\n\n"
                    f"Депозит: <b>${mem['deposit']:,.2f}</b>\n"
                    f"Риск на сделку: <b>{mem['risk']}%</b>\n"
                    f"Макс риск в $: <b>${mem['deposit'] * mem['risk'] / 100:.2f}</b>\n\n"
                    f"Изменить: /risk 5000 или /setrisk 2")
        else:
            text = "💰 <b>Риск калькулятор</b>\n\nУкажи депозит командой:\n/risk 1000"
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
        ]))

    elif data == "menu_journal":
        await callback.message.edit_text(
            "📓 <b>Дневник сделок</b>\n\n"
            "/journal — посмотреть историю + анализ ошибок\n\n"
            "Добавить сделку:\n/journal BTC LONG 65000 67000 win",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_alerts":
        await callback.message.edit_text(
            "🔔 <b>Алерты на пробой уровня</b>\n\n"
            "Установить:\n/alert BTCUSDT 70000\n\n"
            "Как только цена достигнет уровня — пришлю сразу ⚡️",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_stats":
        try:
            conn = sqlite3.connect("brain.db")
            total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result LIKE 'tp%'").fetchone()[0]
            losses = conn.execute("SELECT COUNT(*) FROM signals WHERE result='sl'").fetchone()[0]
            pending = conn.execute("SELECT COUNT(*) FROM signals WHERE result='pending'").fetchone()[0]
            top = conn.execute(
                "SELECT symbol, win_rate, total, avg_hours_to_tp FROM signal_learning ORDER BY win_rate DESC LIMIT 5"
            ).fetchall()
            errors_count = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=0").fetchone()[0]
            patterns = conn.execute(
                "SELECT error_type, count FROM error_patterns ORDER BY count DESC LIMIT 3"
            ).fetchall()
            conn.close()
        except:
            total = wins = losses = pending = errors_count = 0
            top = []
            patterns = []

        wr = round(wins / total * 100, 1) if total > 0 else 0
        top_text = "\n".join([f"  {r[0]}: {r[1]:.0f}% WR за {r[2]} сигн." for r in top]) or "  Нет данных"

        err_text = ""
        if errors_count > 0:
            err_text = f"\n\n⚠️ <b>Открытых ошибок:</b> {errors_count}"
            if patterns:
                err_text += "\n" + "\n".join([f"  • {ERROR_TYPES.get(p[0], p[0])}: {p[1]}x" for p in patterns])

        await callback.message.edit_text(
            f"📈 <b>Статистика APEX</b>\n\n"
            f"Всего сигналов: <b>{total}</b>\n"
            f"✅ Прибыльных: <b>{wins}</b>\n"
            f"❌ Убыточных: <b>{losses}</b>\n"
            f"⏳ В работе: <b>{pending}</b>\n"
            f"🎯 Win Rate: <b>{wr}%</b>\n\n"
            f"🏆 <b>Лучшие монеты:</b>\n{top_text}"
            f"{err_text}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏆 Удачные сделки", callback_data="menu_wins"),
                 InlineKeyboardButton(text="🔍 Ошибки", callback_data="menu_errors")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_brain":
        try:
            conn = sqlite3.connect("brain.db")
            rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
            top_rules = conn.execute(
                "SELECT category, rule, confidence FROM self_rules ORDER BY confidence DESC LIMIT 7"
            ).fetchall()
            obs_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            model_count = conn.execute("SELECT COUNT(*) FROM market_model").fetchone()[0]
            brain_events = conn.execute(
                "SELECT event_type, description FROM brain_log ORDER BY id DESC LIMIT 5"
            ).fetchall()
            avoid_count = conn.execute("SELECT COUNT(*) FROM self_rules WHERE category='avoid'").fetchone()[0]
            conn.close()
        except:
            rule_count = obs_count = model_count = avoid_count = 0
            top_rules = []
            brain_events = []

        cat_emoji = {
            "entry": "🎯", "exit": "🏁", "filter": "🔍",
            "timing": "⏱", "risk": "💰", "avoid": "⛔️",
            "best_setup": "🌟", "market": "📊"
        }
        rules_text = "\n".join([
            f"{cat_emoji.get(r[0], '📌')} [{r[0]}] {r[1][:70]} — {r[2]:.0%}"
            for r in top_rules
        ]) or "Пока нет правил"

        events_text = "\n".join([
            f"• {e[0]}: {e[1][:60]}" for e in brain_events
        ]) or "Нет событий"

        await callback.message.edit_text(
            f"🧠 <b>Мозг APEX</b>\n"
            f"{'━'*24}\n\n"
            f"📌 Активных правил: <b>{rule_count}</b>\n"
            f"⛔️ Антипаттернов: <b>{avoid_count}</b>\n"
            f"👁 Наблюдений: <b>{obs_count}</b>\n"
            f"🗂 Моделей монет: <b>{model_count}</b>\n\n"
            f"<b>Топ правила:</b>\n{rules_text}\n\n"
            f"<b>Последние события:</b>\n{events_text}\n\n"
            f"<i>Мозг учится автоматически — после каждой сделки и каждые 4 часа</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Запустить обучение сейчас", callback_data="brain_learn_now")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "brain_learn_now":
        await callback.message.edit_text("🧠 Запускаю самообучение...")
        await self_research_loop()
        conn = sqlite3.connect("brain.db")
        rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        conn.close()
        await callback.message.edit_text(
            f"✅ <b>Самообучение завершено</b>\n\n"
            f"📌 Активных правил: <b>{rule_count}</b>\n\n"
            f"<i>Правила применяются к следующим сигналам автоматически</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🧠 Посмотреть мозг", callback_data="menu_brain")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_wins":
        try:
            conn = sqlite3.connect("brain.db")
            wins_list = conn.execute(
                """SELECT symbol, direction, entry, tp1, result, grade, timeframe,
                   created_at, closed_at,
                   ROUND((julianday(closed_at) - julianday(created_at)) * 24, 1) as hours
                   FROM signals WHERE result LIKE 'tp%'
                   ORDER BY closed_at DESC LIMIT 15"""
            ).fetchall()
            total_wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result LIKE 'tp%'").fetchone()[0]
            total_sigs = conn.execute("SELECT COUNT(*) FROM signals WHERE result != 'pending'").fetchone()[0]
            avg_hours = conn.execute(
                "SELECT AVG((julianday(closed_at)-julianday(created_at))*24) FROM signals WHERE result LIKE 'tp%'"
            ).fetchone()[0] or 0
            best = conn.execute(
                "SELECT symbol, win_rate FROM signal_learning ORDER BY win_rate DESC LIMIT 3"
            ).fetchall()
            conn.close()
        except Exception as e:
            wins_list = []
            total_wins = total_sigs = 0
            avg_hours = 0
            best = []

        wr = round(total_wins / total_sigs * 100, 1) if total_sigs > 0 else 0

        if not wins_list:
            text = (
                "🏆 <b>Удачные сделки</b>\n\n"
                "Пока нет закрытых прибыльных сделок.\n\n"
                "<i>Сигналы отслеживаются автоматически — как только сработает TP, сделка появится здесь.</i>"
            )
        else:
            lines = []
            tp_emoji = {"tp1": "🥉", "tp2": "🥈", "tp3": "🥇"}
            for w in wins_list:
                symbol, direction, entry, tp1, result, grade, tf, created, closed, hours = w
                emoji = "🟢" if direction == "BULLISH" else "🔴"
                tp_icon = tp_emoji.get(result, "✅")
                hours_str = f"{hours:.0f}ч" if hours and hours < 48 else f"{hours/24:.1f}дн" if hours else "?"
                date_str = closed[:10] if closed else created[:10]
                lines.append(
                    f"{tp_icon} <b>{symbol}</b> {emoji} {result.upper()} | {grade or '-'}\n"
                    f"   Вход: {entry:.4f} → TP: {tp1:.4f} | {hours_str} | {date_str}"
                )

            best_text = " | ".join([f"{b[0]} {b[1]:.0f}%" for b in best]) if best else "—"

            text = (
                f"🏆 <b>Удачные сделки APEX</b>\n"
                f"{'━'*24}\n\n"
                f"✅ Всего побед: <b>{total_wins}</b> из {total_sigs}\n"
                f"🎯 Win Rate: <b>{wr}%</b>\n"
                f"⏱ Среднее время: <b>{avg_hours:.1f}ч</b>\n"
                f"🌟 Лучшие: {best_text}\n"
                f"{'━'*24}\n\n"
                + "\n\n".join(lines[:10])
            )

        await callback.message.edit_text(
            text[:4000],
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_wins"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_errors":
        try:
            conn = sqlite3.connect("brain.db")
            total_err = conn.execute("SELECT COUNT(*) FROM bot_errors").fetchone()[0]
            unfixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=0").fetchone()[0]
            fixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=1").fetchone()[0]
            errors = conn.execute(
                """SELECT id, symbol, direction, error_type, result, fixed, created_at
                   FROM bot_errors ORDER BY id DESC LIMIT 8"""
            ).fetchall()
            patterns = conn.execute(
                "SELECT error_type, count, rule_added FROM error_patterns ORDER BY count DESC LIMIT 5"
            ).fetchall()
            conn.close()
        except:
            total_err = unfixed = fixed = 0
            errors = []
            patterns = []

        errors_text = ""
        for e in errors:
            status = "✅" if e[5] else "❌"
            errors_text += f"{status} #{e[0]} <b>{e[1]}</b> {e[2]} — {ERROR_TYPES.get(e[3], e[3])} [{e[6][:10]}]\n"

        patterns_text = ""
        for p in patterns:
            rule_icon = "📌" if p[2] else "⚠️"
            patterns_text += f"{rule_icon} {ERROR_TYPES.get(p[0], p[0])}: {p[1]}x\n"
            if p[2]:
                patterns_text += f"   → {p[2][:60]}\n"

        await callback.message.edit_text(
            f"🔍 <b>Ошибки бота APEX</b>\n"
            f"{'━'*24}\n\n"
            f"Всего: {total_err} | ❌ Открыто: {unfixed} | ✅ Исправлено: {fixed}\n\n"
            f"<b>Последние ошибки:</b>\n{errors_text or 'Нет ошибок'}\n"
            f"<b>Паттерны:</b>\n{patterns_text or 'Нет повторений'}\n"
            f"{'━'*24}\n"
            f"<i>/errors [id] — детальный разбор\n"
            f"/errors fix [id] — отметить как исправленное</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_errors"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_news":
        await callback.message.edit_text("📰 Собираю свежие новости...")
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

        # Крипто новости
        crypto_news = await asyncio.get_event_loop().run_in_executor(None, get_crypto_news)
        # Макро новости
        macro_news = await asyncio.get_event_loop().run_in_executor(None, get_market_impact_news)

        crypto_text = format_news(crypto_news[:6])
        macro_text = format_news(macro_news[:4])

        # AI анализ влияния на рынок
        all_titles = "\n".join([item["title"] for item in (crypto_news + macro_news)[:10]])
        analysis = ask_groq(
            f"Ты крипто трейдер. Оцени эти новости — что важно для рынка прямо сейчас? (3-4 пункта, дерзко и кратко):\n{all_titles}",
            max_tokens=300
        )
        save_news("crypto news", all_titles[:500])

        msg = (
            f"📰 <b>Новости крипторынка</b>\n"
            f"🕐 Обновлено: {now_str}\n"
            f"{'━'*24}\n\n"
            f"<b>🔥 Крипто:</b>\n{crypto_text}\n\n"
            f"{'━'*24}\n"
            f"<b>🌍 Макро (влияет на рынок):</b>\n{macro_text}\n\n"
            f"{'━'*24}\n"
            f"<b>⚡️ APEX анализ:</b>\n{analysis or 'Анализирую...'}"
        )

        # Telegram лимит 4096 символов
        if len(msg) > 4000:
            msg = msg[:3990] + "..."

        await callback.message.edit_text(
            msg,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_news"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "menu_pump":
        await callback.message.edit_text("📦 Сканирую топ-50 на накопление перед пампом...\n⏳ ~30 секунд")
        pairs = await asyncio.get_event_loop().run_in_executor(None, get_top_pairs, 50)
        found = []
        for symbol in pairs:
            try:
                acc = await asyncio.get_event_loop().run_in_executor(None, detect_accumulation, symbol)
                if acc and acc["score"] >= 50:
                    found.append(acc)
            except:
                pass

        found.sort(key=lambda x: x["score"], reverse=True)

        if not found:
            await callback.message.edit_text(
                "📦 Накоплений не найдено.\nРынок в движении — боковиков нет.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_pump"),
                     InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
                ])
            )
            return

        summary = f"📦 <b>Накопления перед пампом</b>\nНайдено: {len(found)} монет\n{'━'*24}\n\n"
        for acc in found[:3]:
            p = acc["price"]
            ps = f"${p:,.4f}" if p < 1 else f"${p:,.2f}"
            bar = "█" * (acc["score"] // 10) + "░" * (10 - acc["score"] // 10)
            summary += (
                f"📦 <b>{acc['symbol']}</b> | {ps}\n"
                f"Скор: [{bar}] {acc['score']}/100\n"
                f"{acc['signals'][0] if acc['signals'] else ''}\n\n"
            )

        summary += f"<i>Полный разбор каждой — команда /pump BTCUSDT</i>"

        await callback.message.edit_text(
            summary,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_pump"),
                 InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

@dp.message(Command("patch"))
async def cmd_patch(message: types.Message):
    """Ручной запуск авто-патча — /patch [описание ошибки]"""
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split(maxsplit=1)
    error_text = args[1].strip() if len(args) > 1 else "manual patch request"
    await message.answer("🔧 Запускаю анализ кода через Groq...")
    await analyze_and_patch(error_text, "manual")


@dp.message(Command("pump"))
async def cmd_pump(message: types.Message):
    args = message.text.split()
    if len(args) == 2:
        symbol = args[1].upper().replace("USDT","") + "USDT"
        await message.answer(f"📦 Анализирую накопление {symbol}...")
        acc = await asyncio.get_event_loop().run_in_executor(None, detect_accumulation, symbol)
        if acc:
            await message.answer(format_accumulation(acc), parse_mode="HTML")
        else:
            await message.answer(f"😴 {symbol} — накоплений не обнаружено.")
    else:
        await message.answer("📦 Сканирую топ-20 на накопление...")
        pairs = get_top_pairs(20)
        found = []
        for symbol in pairs:
            acc = detect_accumulation(symbol)
            if acc and acc["score"] >= 50:
                found.append(acc)
            time.sleep(0.2)
        found.sort(key=lambda x: x["score"], reverse=True)
        if not found:
            await message.answer("😴 Накоплений не найдено.")
            return
        await message.answer(f"📦 Найдено накоплений: {len(found)}")
        for acc in found[:3]:
            await message.answer(format_accumulation(acc), parse_mode="HTML")
            await asyncio.sleep(0.5)

@dp.message(Command("think"))
async def cmd_think(message: types.Message):
    """Бот думает вслух — глубокий ресёрч по теме"""
    args = message.text.split(maxsplit=1)
    topic = args[1].strip() if len(args) > 1 else "bitcoin market analysis"
    await message.answer(f"🧠 Думаю над темой: <b>{topic}</b>...\n⏳ Ищу в интернете, анализирую...", parse_mode="HTML")
    result = await asyncio.get_event_loop().run_in_executor(None, deep_research, topic)
    if result:
        await message.answer(
            f"🧠 <b>Глубокий анализ: {topic}</b>\n\n{result}",
            parse_mode="HTML"
        )
    else:
        await message.answer("Не удалось найти достаточно данных.")

@dp.message(Command("brain"))
async def cmd_brain(message: types.Message):
    """Показываем что бот знает — его база знаний"""
    try:
        conn = sqlite3.connect("brain.db")
        total_k = conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]
        sources = conn.execute(
            "SELECT source, COUNT(*) as cnt FROM knowledge GROUP BY source ORDER BY cnt DESC LIMIT 8"
        ).fetchall()
        recent = conn.execute(
            "SELECT topic, source, created_at FROM knowledge ORDER BY id DESC LIMIT 5"
        ).fetchall()
        reflections = conn.execute(
            "SELECT COUNT(*) FROM knowledge WHERE source='self-reflection'"
        ).fetchone()[0]
        comparisons = conn.execute(
            "SELECT COUNT(*) FROM knowledge WHERE source='self-compare'"
        ).fetchone()[0]
        conn.close()

        sources_text = "\n".join([f"• {r[0]}: {r[1]} записей" for r in sources])
        recent_text = "\n".join([f"• [{r[2][:10]}] {r[0][:40]} ({r[1]})" for r in recent])

        await message.answer(
            f"🧠 <b>Мозг APEX</b>\n\n"
            f"📚 Всего знаний: <b>{total_k}</b>\n"
            f"🔄 Само-рефлексий: <b>{reflections}</b>\n"
            f"📊 Сравнений прогнозов: <b>{comparisons}</b>\n\n"
            f"<b>Источники знаний:</b>\n{sources_text}\n\n"
            f"<b>Последние 5 знаний:</b>\n{recent_text}\n\n"
            f"<i>Используй /think [тема] — заставить думать над конкретным вопросом</i>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

@dp.message(Command("errors"))
async def cmd_errors(message: types.Message):
    """Раздел ошибок бота — просмотр, анализ, исправления"""
    args = message.text.split()

    # /errors fix <id> — отметить ошибку как исправленную
    if len(args) == 3 and args[1] == "fix":
        try:
            error_id = int(args[2])
            conn = sqlite3.connect("brain.db")
            row = conn.execute(
                "SELECT symbol, error_type, ai_next_time FROM bot_errors WHERE id=?",
                (error_id,)
            ).fetchone()

            if not row:
                await message.answer(f"Ошибка #{error_id} не найдена.")
                conn.close()
                return

            # AI формулирует что именно исправлено
            fix_prompt = f"""Ошибка типа "{ERROR_TYPES.get(row[1], row[1])}" по монете {row[0]} отмечена как исправленная.
Правило было: {row[2]}

Напиши 1-2 предложения:
1. Что именно было исправлено в стратегии
2. Как бот будет поступать теперь"""

            fix_desc = ask_groq(fix_prompt, max_tokens=150)

            conn.execute(
                "UPDATE bot_errors SET fixed=1, fix_description=?, fixed_at=CURRENT_TIMESTAMP WHERE id=?",
                (fix_desc or "Исправлено вручную", error_id)
            )
            conn.commit()
            conn.close()

            await message.answer(
                f"✅ <b>Ошибка #{error_id} отмечена как исправленная</b>\n\n"
                f"<b>{row[0]}</b> | {ERROR_TYPES.get(row[1], row[1])}\n\n"
                f"📌 <b>Что изменено:</b>\n{fix_desc or 'Исправлено'}",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}")
        return

    # /errors <id> — детальный просмотр конкретной ошибки
    if len(args) == 2:
        try:
            error_id = int(args[1])
            conn = sqlite3.connect("brain.db")
            row = conn.execute(
                """SELECT id, symbol, direction, entry, sl, result, error_type,
                   error_description, ai_analysis, ai_lesson, ai_next_time,
                   fixed, fix_description, hours_in_trade, market_context, created_at
                   FROM bot_errors WHERE id=?""",
                (error_id,)
            ).fetchone()
            conn.close()

            if not row:
                await message.answer(f"Ошибка #{error_id} не найдена.")
                return

            fixed_block = ""
            if row[11]:  # fixed == 1
                fixed_block = f"\n\n✅ <b>ИСПРАВЛЕНО:</b>\n{row[12]}"
            else:
                fixed_block = f"\n\n❌ Ещё не исправлено\n/errors fix {error_id} — отметить как исправленное"

            await message.answer(
                f"🔍 <b>Разбор ошибки #{row[0]}</b>\n"
                f"{'━'*24}\n\n"
                f"📊 <b>Сделка:</b> {row[1]} {row[2]}\n"
                f"💰 Вход: <code>{row[3]}</code> | Стоп: <code>{row[4]}</code>\n"
                f"❌ Результат: {row[5]} за {row[13]}ч\n"
                f"🏷 Тип ошибки: <b>{ERROR_TYPES.get(row[6], row[6])}</b>\n"
                f"📅 {row[15][:16]}\n\n"
                f"📋 <b>Рыночный контекст:</b>\n{row[14]}\n\n"
                f"🧠 <b>Анализ:</b>\n{row[8]}\n\n"
                f"📚 <b>Урок:</b>\n{row[9]}\n\n"
                f"📌 <b>В следующий раз:</b>\n{row[10]}"
                f"{fixed_block}",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(f"Ошибка: {e}")
        return

    # /errors — список всех ошибок
    try:
        conn = sqlite3.connect("brain.db")
        total = conn.execute("SELECT COUNT(*) FROM bot_errors").fetchone()[0]
        unfixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=0").fetchone()[0]
        fixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=1").fetchone()[0]

        # Последние 10 ошибок
        errors = conn.execute(
            """SELECT id, symbol, direction, error_type, result, fixed, created_at
               FROM bot_errors ORDER BY id DESC LIMIT 10"""
        ).fetchall()

        # Паттерны — повторяющиеся ошибки
        patterns = conn.execute(
            "SELECT error_type, count, rule_added FROM error_patterns ORDER BY count DESC LIMIT 5"
        ).fetchall()
        conn.close()

        errors_text = ""
        for e in errors:
            status = "✅" if e[5] else "❌"
            errors_text += f"{status} #{e[0]} <b>{e[1]}</b> {e[2]} — {ERROR_TYPES.get(e[3], e[3])} ({e[4]}) [{e[6][:10]}]\n"

        patterns_text = ""
        for p in patterns:
            rule_icon = "📌" if p[2] else "⚠️"
            patterns_text += f"{rule_icon} {ERROR_TYPES.get(p[0], p[0])}: {p[1]}x"
            if p[2]:
                patterns_text += f" → правило: {p[2][:60]}"
            patterns_text += "\n"

        await message.answer(
            f"🔍 <b>Ошибки бота APEX</b>\n"
            f"{'━'*24}\n\n"
            f"Всего: {total} | ❌ Открыто: {unfixed} | ✅ Исправлено: {fixed}\n\n"
            f"<b>Последние ошибки:</b>\n{errors_text}\n"
            f"<b>Паттерны (повторяющиеся):</b>\n{patterns_text}\n"
            f"{'━'*24}\n"
            f"<i>/errors [id] — детальный разбор\n"
            f"/errors fix [id] — отметить как исправленное</i>",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(f"Ошибка: {e}")

@dp.message()
async def handle_text(message: types.Message):
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "трейдер"
    text = message.text
    if not text:
        return

    # Проверяем состояние (ожидаем ввод монеты для бектеста)
    if user_id in user_states:
        state = user_states.pop(user_id)
        if state.get("action") == "backtest":
            symbol = text.upper().replace("USDT", "") + "USDT"
            tf = state.get("tf", "1h")
            await message.answer(f"🔬 Запускаю бектест {symbol} {TF_LABELS.get(tf, tf)}...")
            result = backtest(symbol, tf)
            if result:
                grade = "🔥 Отличная" if result["win_rate"] >= 60 else "✅ Рабочая" if result["win_rate"] >= 50 else "⚠️ Слабая"
                await message.answer(
                    f"🔬 <b>Бектест {symbol} [{TF_LABELS.get(tf, tf)}]</b>\n\n"
                    f"Сигналов: {result['total']}\n"
                    f"✅ Выигрыши: {result['wins']}\n"
                    f"❌ Проигрыши: {result['losses']}\n"
                    f"🎯 Win Rate: <b>{result['win_rate']}%</b>\n"
                    f"Оценка: {grade}",
                    parse_mode="HTML"
                )
            else:
                await message.answer("Недостаточно данных для бектеста")
            return

    save_chat_log(user_id, "user", text)
    thinking = await message.answer("⚡️")
    reply = ask_ai(user_id, user_name, text)
    try:
        await thinking.delete()
    except:
        pass
    if reply:
        save_chat_log(user_id, "assistant", reply)
        await message.answer(reply)
        asyncio.create_task(
            asyncio.to_thread(extract_and_save_profile, user_id, user_name, text, reply)
        )
    else:
        await message.answer("⚡️ Перегружен, попробуй через минуту.")

# ===== AUTO TASKS =====

async def auto_research():
    topics = ["bitcoin analysis today", "crypto market today", "altcoins 2025"]
    for topic in topics:
        try:
            result = tavily_search(topic, max_results=3)
            summary = ask_groq(f"Вывод для трейдера (2 предложения):\n{result[:600]}", max_tokens=150)
            if summary:
                save_knowledge(topic, summary, "auto")
                save_news(topic, summary)
            await asyncio.sleep(15)
        except:
            pass

async def auto_scan_job():
    """Каждые 30 мин: SMC сканирование топ-50 пар по всем таймфреймам"""
    closed = check_pending_signals()
    for c in closed:
        if c["is_win"] and ADMIN_ID:
            tp_icons = {"tp1": "🎯", "tp2": "🎯🎯", "tp3": "🎯🎯🎯"}
            icon = tp_icons.get(c["result"], "✅")
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"{icon} <b>{c['symbol']}</b> — {c['result'].upper()}!\n"
                    f"⏱ Закрыто за {c['hours']}ч | {c.get('grade', '-')}",
                    parse_mode="HTML"
                )
            except:
                pass

    pairs = get_top_pairs(50)
    mega_signals = []
    top_signals = []
    good_signals = []

    # Сканируем все пары по всем таймфреймам
    for symbol in pairs:
        for tf in ["15m", "1h", "4h"]:
            try:
                sig_data = full_scan_raw(symbol, tf)
                if sig_data:
                    grade = sig_data["grade"]
                    # Не дублируем один символ с одним направлением
                    existing = [s for s in mega_signals + top_signals + good_signals
                               if s["symbol"] == symbol and s["direction"] == sig_data["direction"]]
                    if existing:
                        # Оставляем лучший грейд
                        continue
                    if grade == "МЕГА ТОП":
                        mega_signals.append(sig_data)
                    elif grade == "ТОП СДЕЛКА":
                        top_signals.append(sig_data)
                    elif grade == "ХОРОШАЯ":
                        good_signals.append(sig_data)
                await asyncio.sleep(0.3)
            except:
                pass

    total = len(mega_signals) + len(top_signals) + len(good_signals)
    logging.info(f"Скан: {len(pairs)} пар × 3 ТФ | 🔥🔥🔥{len(mega_signals)} | 🔥🔥{len(top_signals)} | ✅{len(good_signals)}")

    if not ADMIN_ID:
        return

    # Отправляем МЕГА ТОП сразу
    for sd in mega_signals[:5]:
        try:
            await bot.send_message(ADMIN_ID, sd["text"], parse_mode="HTML")
            await asyncio.sleep(1)
        except:
            pass

    # Отправляем ТОП СДЕЛКИ (до 3)
    for sd in top_signals[:3]:
        try:
            await bot.send_message(ADMIN_ID, sd["text"], parse_mode="HTML")
            await asyncio.sleep(1)
        except:
            pass

    # Итоговая сводка если ничего не нашли
    if not mega_signals and not top_signals:
        if good_signals:
            summary = "✅ " + ", ".join([f"{s['symbol']} {s['direction']}" for s in good_signals[:5]])
        else:
            summary = "Сигналов нет"
        await bot.send_message(
            ADMIN_ID,
            f"📊 <b>Скан завершён</b>\n"
            f"Пар: {len(pairs)} | Сигналов: {total}\n"
            f"{summary}",
            parse_mode="HTML"
        )


async def auto_accumulation_scan():
    """Каждый час: сканируем все топ-50 на накопление перед пампом"""
    pairs = get_top_pairs(50)
    found = []

    for symbol in pairs:
        try:
            acc = detect_accumulation(symbol)
            if acc and acc["score"] >= 60:
                found.append(acc)
            await asyncio.sleep(0.3)
        except:
            pass

    # Сортируем по скору
    found.sort(key=lambda x: x["score"], reverse=True)

    if found and ADMIN_ID:
        await bot.send_message(
            ADMIN_ID,
            f"📦 <b>Накопления перед пампом: {len(found)}</b>\n"
            f"Топ монеты по скору накопления:",
            parse_mode="HTML"
        )
        for acc in found[:4]:
            try:
                await bot.send_message(ADMIN_ID, format_accumulation(acc), parse_mode="HTML")
                await asyncio.sleep(1)
            except:
                pass

    logging.info(f"Накопление скан: {len(pairs)} пар | найдено: {len(found)}")


def full_scan_raw(symbol, timeframe="1h"):
    """Возвращает dict с текстом и grade для фильтрации"""
    try:
        mtf = multi_tf_analysis(symbol, ["15m", "1h", "4h"])
        if not mtf:
            return None

        direction = mtf["direction"]
        candles = get_candles(symbol, timeframe, 200)
        if len(candles) < 20:
            return None

        price = candles[-1]["close"]
        ob = find_ob(candles, direction)
        fvg = find_fvg(candles, direction)
        ob_data = get_orderbook(symbol)

        confluence = [f"✅ {mtf['match_count']}/{mtf['total']} ТФ совпали"]
        if ob:
            confluence.append(f"✅ Order Block: {ob['bottom']:.4f}–{ob['top']:.4f}")
        if fvg:
            confluence.append(f"✅ FVG: {fvg['bottom']:.4f}–{fvg['top']:.4f}")
        if ob_data:
            match = (direction == "BULLISH" and ob_data["bias"] == "BUY") or \
                    (direction == "BEARISH" and ob_data["bias"] == "SELL")
            if match:
                confluence.append(f"✅ OrderBook: {ob_data['bias']}")

        if len(confluence) < 2:
            return None

        risk = price * 0.015
        if direction == "BULLISH":
            entry = ob["top"] if ob else price
            sl = round(entry - risk, 4)
            tp1 = round(entry + risk * 2, 4)
            tp2 = round(entry + risk * 3, 4)
            tp3 = round(entry + risk * 5, 4)
        else:
            entry = ob["bottom"] if ob else price
            sl = round(entry + risk, 4)
            tp1 = round(entry - risk * 2, 4)
            tp2 = round(entry - risk * 3, 4)
            tp3 = round(entry - risk * 5, 4)

        est_hours, confidence, win_rate = get_estimated_time(symbol, timeframe)
        time_str = f"~{est_hours}ч" if est_hours < 24 else f"~{est_hours//24}дн"
        wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"
        tf_label = TF_LABELS.get(timeframe, timeframe)

        save_signal_db(symbol, direction, "MTF", entry, tp1, tp2, tp3, sl, timeframe, est_hours, mtf["grade"])

        emoji = "🟢" if direction == "BULLISH" else "🔴"
        conf_text = "\n".join(confluence)

        text = (
            f"{'━'*26}\n"
            f"{mtf['grade_emoji']} <b>{mtf['grade']}</b> [{tf_label}]\n"
            f"{emoji} <b>{symbol}</b> — {direction}\n"
            f"{'━'*26}\n\n"
            f"📐 <b>Таймфреймы:</b>\n{mtf['tf_status']}\n"
            f"{mtf['stars']}\n\n"
            f"💰 <b>Вход:</b> <code>{entry:.4f}</code>\n"
            f"🛑 <b>Стоп:</b> <code>{sl:.4f}</code>\n"
            f"🎯 <b>TP1:</b> <code>{tp1:.4f}</code> (+2R)\n"
            f"🎯 <b>TP2:</b> <code>{tp2:.4f}</code> (+3R)\n"
            f"🎯 <b>TP3:</b> <code>{tp3:.4f}</code> (+5R)\n\n"
            f"⏱ <b>Время отработки:</b> {time_str}\n"
            f"📊 <b>Точность:</b> {wr_str} | {confidence}\n\n"
            f"📋 <b>Confluence:</b>\n{conf_text}\n"
            f"{'━'*26}"
        )

        return {"symbol": symbol, "grade": mtf["grade"], "text": text, "direction": direction}

    except Exception as e:
        logging.error(f"full_scan_raw error {symbol}: {e}")
        return None


# ===== АВТО-ПАТЧ GITHUB =====
# Бот сам чинит код: ловит ошибку → анализирует → спрашивает разрешения → пушит коммит

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")   # например: vladislavdim/apex-smc-bot
GITHUB_FILE = os.environ.get("GITHUB_FILE", "bot.py")

# Очередь ожидающих патчей: patch_id -> {code, description, error}
pending_patches = {}
patch_counter = 0

def github_get_file():
    """Читаем текущий bot.py прямо из GitHub"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return None, None
    try:
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json"
            },
            timeout=15
        )
        data = r.json()
        if "content" in data:
            import base64
            code = base64.b64decode(data["content"]).decode("utf-8")
            sha = data["sha"]
            return code, sha
        return None, None
    except Exception as e:
        logging.error(f"GitHub get file: {e}")
        return None, None


def github_push_patch(new_code, sha, commit_message):
    """Пушим исправленный код в GitHub"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False, "GITHUB_TOKEN или GITHUB_REPO не заданы"
    try:
        import base64
        encoded = base64.b64encode(new_code.encode("utf-8")).decode("utf-8")
        r = requests.put(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github.v3+json"
            },
            json={
                "message": commit_message,
                "content": encoded,
                "sha": sha
            },
            timeout=20
        )
        if r.status_code in (200, 201):
            return True, r.json().get("commit", {}).get("sha", "")[:7]
        return False, f"GitHub API error: {r.status_code} — {r.text[:200]}"
    except Exception as e:
        return False, str(e)


async def analyze_and_patch(error_text, error_source="runtime"):
    """
    Главная функция авто-патча:
    1. Читает код из GitHub
    2. Отправляет Groq на анализ
    3. Получает исправленный код
    4. Отправляет в Telegram с кнопками ✅/❌
    """
    global patch_counter
    if not ADMIN_ID:
        return
    if not GITHUB_TOKEN or not GITHUB_REPO:
        await bot.send_message(
            ADMIN_ID,
            "⚠️ Авто-патч: GITHUB_TOKEN или GITHUB_REPO не заданы. Добавь переменные в Render.",
            parse_mode="HTML"
        )
        return

    send_text = (
        "🔧 <b>Обнаружена ошибка</b>\n\n"
        + "<code>" + error_text[:400] + "</code>\n\n"
        + "⏳ Читаю код из GitHub и анализирую..."
    )
    await bot.send_message(ADMIN_ID, send_text, parse_mode="HTML")


    # Читаем текущий код
    current_code, sha = github_get_file()
    if not current_code:
        await bot.send_message(ADMIN_ID, "❌ Не удалось прочитать код из GitHub.")
        return

    # Groq анализирует ошибку и предлагает исправление
    prompt = f"""Ты senior Python разработчик. В боте произошла ошибка.

ОШИБКА:
{error_text[:800]}

КОД (первые 3000 символов для контекста):
{current_code[:3000]}

Задача:
1. Найди причину ошибки
2. Предложи минимальное исправление
3. Верни ТОЛЬКО JSON:
{{"description": "что исправлено (1-2 предложения)", "search": "точный текст который нужно заменить", "replace": "исправленный текст", "risk": "low/medium/high"}}

Важно: search должен быть уникальным фрагментом кода который встречается ОДИН раз."""

    response = ask_groq(prompt, max_tokens=800)
    if not response:
        await bot.send_message(ADMIN_ID, "❌ Groq не смог проанализировать ошибку.")
        return

    # Парсим ответ
    try:
        clean = response.strip().replace("```json", "").replace("```", "").strip()
        start = clean.find("{")
        end = clean.rfind("}") + 1
        patch_data = json.loads(clean[start:end])
    except Exception as e:
        await bot.send_message(ADMIN_ID, "❌ Groq вернул некорректный JSON: " + str(e) + "\n\n" + str(response)[:300])

        return
        return

    search_text = patch_data.get("search", "")
    replace_text = patch_data.get("replace", "")
    description = patch_data.get("description", "нет описания")
    risk = patch_data.get("risk", "unknown")

    if not search_text or search_text not in current_code:
        await bot.send_message(
            ADMIN_ID,
            f"⚠️ <b>Groq предложил исправление, но не смог найти точный фрагмент в коде.</b>"
            f"📝 Описание: {description}"
            f"Возможно нужно исправить вручную.",
            parse_mode="HTML"
        )
        return

    # Сохраняем патч в очередь
    patch_counter += 1
    patch_id = str(patch_counter)
    new_code = current_code.replace(search_text, replace_text, 1)

    pending_patches[patch_id] = {
        "new_code": new_code,
        "sha": sha,
        "description": description,
        "error": error_text[:200],
        "risk": risk,
        "search": search_text[:150],
        "replace": replace_text[:150]
    }

    risk_emoji = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(risk, "⚪️")

    await bot.send_message(
        ADMIN_ID,
        "🔧 <b>APEX хочет исправить код</b>\n" +
        "━" * 24 + "\n\n" +
        "📋 <b>Что исправить:</b>\n" + description + "\n\n" +
        risk_emoji + " <b>Риск:</b> " + risk + "\n\n" +
        "<b>Заменить:</b>\n<code>" + search_text[:200] + "</code>\n\n" +
        "<b>На:</b>\n<code>" + replace_text[:200] + "</code>\n\n" +
        "━" * 24 + "\nПрименить изменение и задеплоить?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Применить и деплоить", callback_data=f"patch_apply_{patch_id}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"patch_cancel_{patch_id}")
            ]
        ])
    )

async def apply_patch(patch_id):
    """Применяем патч — пушим в GitHub"""
    patch = pending_patches.get(patch_id)
    if not patch:
        return False, "Патч не найден или устарел"

    success, result = github_push_patch(
        patch["new_code"],
        patch["sha"],
        f"🤖 APEX auto-fix: {patch['description'][:60]}"
    )

    del pending_patches[patch_id]
    return success, result


# Обработчик глобальных ошибок — ловим всё что падает в боте
last_error_time = {}
error_cooldown = 300  # 5 минут между одинаковыми ошибками

class ErrorCapture(logging.Handler):
    """Перехватывает ERROR логи и запускает авто-патч"""
    def emit(self, record):
        if record.levelno >= logging.ERROR:
            error_text = self.format(record)
            # Дедупликация — не спамим одной ошибкой
            error_key = error_text[:100]
            now = time.time()
            if now - last_error_time.get(error_key, 0) < error_cooldown:
                return
            last_error_time[error_key] = now

            # Запускаем авто-патч асинхронно
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(analyze_and_patch(error_text, "runtime"))
            except:
                pass


def setup_error_capture():
    """Подключаем перехватчик ошибок"""
    handler = ErrorCapture()
    handler.setLevel(logging.ERROR)
    logging.getLogger().addHandler(handler)
    logging.info("ErrorCapture активирован — авто-патч включён")


# ===== MAIN =====

async def on_startup(app):
    init_db()
    threading.Thread(target=get_top_pairs, daemon=True).start()

    WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
    if WEBHOOK_URL:
        await bot.set_webhook(f"{WEBHOOK_URL}/webhook", drop_pending_updates=True)
        logging.info(f"Webhook установлен: {WEBHOOK_URL}/webhook")
    else:
        logging.warning("WEBHOOK_URL не задан — работаем в polling режиме")

    scheduler = AsyncIOScheduler()
    scheduler.add_job(auto_scan_job, "interval", minutes=30)
    scheduler.add_job(auto_accumulation_scan, "interval", hours=1)
    scheduler.add_job(auto_research, "interval", hours=2)
    scheduler.add_job(check_alerts, "interval", minutes=5)
    scheduler.add_job(night_brain_tasks, "interval", hours=4)
    scheduler.add_job(realtime_pump_detector, "interval", minutes=15)
    scheduler.start()
    setup_error_capture()
    logging.info("APEX запущен!")


async def on_shutdown(app):
    logging.info("APEX остановлен")


def main():
    WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

    if WEBHOOK_URL:
        # Webhook режим — решает TelegramConflictError навсегда
        app = web.Application()

        # Health check
        async def health(request):
            return web.Response(text="APEX OK")
        app.router.add_get("/", health)
        app.router.add_get("/health", health)

        # Webhook handler — регистрируем ДО on_startup
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")

        # on_startup и on_shutdown вешаем ОДИН РАЗ
        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)

        port = int(os.environ.get("PORT", 10000))
        logging.info(f"Запуск в webhook режиме на порту {port}")
        web.run_app(app, host="0.0.0.0", port=port)
    else:
        # Polling режим — fallback если нет WEBHOOK_URL
        async def polling_main():
            init_db()
            threading.Thread(target=get_top_pairs, daemon=True).start()
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logging.info("Webhook удалён, запуск polling")
            except Exception as e:
                logging.warning(f"delete_webhook: {e}")
            await asyncio.sleep(2)
            scheduler = AsyncIOScheduler()
            scheduler.add_job(auto_scan_job, "interval", minutes=30)
            scheduler.add_job(auto_accumulation_scan, "interval", hours=1)
            scheduler.add_job(auto_research, "interval", hours=2)
            scheduler.add_job(check_alerts, "interval", minutes=5)
            scheduler.add_job(night_brain_tasks, "interval", hours=4)
            scheduler.add_job(realtime_pump_detector, "interval", minutes=15)
            scheduler.start()
            logging.info("APEX запущен в polling режиме")
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

        asyncio.run(polling_main())


if __name__ == "__main__":
    main()
