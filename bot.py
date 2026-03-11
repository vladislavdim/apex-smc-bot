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
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Внешние модули APEX ──────────────────────────────────────
import sys as _sys, os as _os_path
# Добавляем папку core/ в путь поиска модулей — файлы могут лежать там
_BASE_DIR = _os_path.path.dirname(_os_path.path.abspath(__file__))
for _p in [_os_path.path.join(_BASE_DIR, "core"), _BASE_DIR]:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

try:
    from smc_engine import (
        get_candles_smart, multi_tf_analysis as _smc_multi_tf,
        find_swings as _smc_find_swings, classify_swings as _smc_classify_swings,
        detect_events as _smc_detect_events, find_ob as _smc_find_ob,
        find_fvg as _smc_find_fvg, get_source_stats, get_barrier_summary,
        full_smc_analysis, detect_liquidity_sweep, find_imbalance_zones,
        get_premium_discount, detect_divergence, get_market_profile,
        find_ob_fvg_chain, check_volume_on_structure,
        calculate_cvd, detect_whale_candles, get_volume_profile,
    )
    _SMC_ENGINE_OK = True
    logging.info("smc_engine.py загружен успешно")
except ImportError as e:
    _SMC_ENGINE_OK = False
    logging.warning(f"smc_engine.py не найден: {e} — ищем в: {sys.path[:3]}")
    get_source_stats = lambda: "smc_engine.py не загружен — положи файл рядом с bot.py"
    get_barrier_summary = lambda: ""
    full_smc_analysis = lambda s, i="1h": {}
    detect_liquidity_sweep = lambda c, h, l: None
    find_imbalance_zones = lambda c: []
    get_premium_discount = lambda c: {"zone": "UNKNOWN", "pct": 50}
    detect_divergence = lambda c, d: None
    get_market_profile = lambda c: {}
    find_ob_fvg_chain = lambda c, d: None
    check_volume_on_structure = lambda c, i: {"valid": True, "signal": "UNKNOWN"}
    calculate_cvd = lambda c: {"cvd": 0, "trend": "NEUTRAL", "divergence": None, "signal": "NEUTRAL", "buy_pressure_pct": 50}
    detect_whale_candles = lambda c: {"found": False, "spike": 0, "type": "NONE", "strength": 0}
    get_volume_profile = lambda c: {"poc": 0, "high_volume_zones": [], "current_zone": "UNKNOWN"}

try:
    from learning import (
        save_signal as _learn_save_signal,
        close_signal as _learn_close_signal,
        get_min_confluence as _learn_min_confluence,
        should_skip_symbol as _learn_should_skip,
        get_signal_context as _learn_signal_ctx,
        get_best_entry_hours as _learn_best_hours,
        run_self_analysis as _learn_self_analysis,
        get_self_analysis_text as _learn_self_analysis_text,
        get_all_stats_text as _learn_all_stats,
        find_similar_patterns as _learn_patterns,
        save_pattern as _learn_save_pattern,
        decay_old_rules as _learn_decay,
        get_btc_correlation as _learn_btc_corr,
        update_streak as _learn_streak,
        get_streak_min_confluence as _learn_streak_threshold,
        update_grade_accuracy as _learn_grade_acc,
        get_grade_accuracy_text as _learn_grade_text,
        log_knowledge_gap as _learn_gap,
        get_unresolved_gaps as _learn_get_gaps,
        resolve_gap as _learn_resolve_gap,
        analyze_closed_trade as _learn_analyze_trade,
        groq_build_strategy as _learn_build_strategy,
        get_current_strategy as _learn_get_strategy,
        groq_self_diagnosis as _learn_self_diag,
        get_latest_diagnosis as _learn_latest_diag,
        get_latest_trade_analysis as _learn_trade_analysis,
        get_groq_trade_insight as _learn_trade_insight,
        groq_whale_context as _learn_whale_ctx,
        groq_news_impact as _learn_news_impact,
    )
    _LEARNING_OK = True
    logging.info("learning.py загружен успешно")
except ImportError as e:
    _LEARNING_OK = False
    logging.warning(f"learning.py не найден: {e}")
    _learn_min_confluence = lambda s: 2
    _learn_should_skip = lambda s, d: (False, "")
    _learn_signal_ctx = lambda s: ""
    _learn_best_hours = lambda: []
    _learn_self_analysis = lambda: None
    _learn_self_analysis_text = lambda: ""
    _learn_all_stats = lambda: ""
    _learn_patterns = lambda *a, **k: {"found": False, "samples": 0}
    _learn_save_pattern = lambda *a, **k: None
    _learn_decay = lambda: None
    _learn_btc_corr = lambda s: {"beta": 1.0, "samples": 0, "desc": ""}
    _learn_streak = lambda r: {"win_streak": 0, "loss_streak": 0, "extra_filter": False}
    _learn_streak_threshold = lambda: 18
    _learn_grade_acc = lambda *a: None
    _learn_grade_text = lambda: ""
    _learn_gap = lambda *a: None
    _learn_get_gaps = lambda: []
    _learn_resolve_gap = lambda *a: None
    _learn_analyze_trade = lambda *a: None
    _learn_build_strategy = lambda: ""
    _learn_get_strategy = lambda: "Стратегия не сформирована"
    _learn_self_diag = lambda: ""
    _learn_latest_diag = lambda: "Самодиагностика недоступна"
    _learn_trade_analysis = lambda n=5: "Нет анализов"
    _learn_trade_insight = lambda *a, **k: ""
    _learn_whale_ctx = lambda *a: ""
    _learn_news_impact = lambda *a: ""

TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0") or 0)
GROQ_KEY = os.environ.get("GROQ_API_KEY")
TAVILY_KEY = os.environ.get("TAVILY_API_KEY", "")
TWELVEDATA_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
MOBULA_KEY     = os.environ.get("MOBULA_API_KEY", "")
COINALYZE_KEY  = os.environ.get("COINALYZE_API_KEY", "")
LUNARCRUSH_KEY = os.environ.get("LUNARCRUSH_API_KEY", "")

_API_STATUS = {
    "twelvedata":  bool(os.environ.get("TWELVEDATA_API_KEY", "")),
    "mobula":      bool(os.environ.get("MOBULA_API_KEY", "")),
    "coinalyze":   bool(os.environ.get("COINALYZE_API_KEY", "")),
    "lunarcrush":  bool(os.environ.get("LUNARCRUSH_API_KEY", "")),
    "tavily":      bool(os.environ.get("TAVILY_API_KEY", "")),
    "groq":        bool(os.environ.get("GROQ_API_KEY", "")),
    "binance":     bool(os.environ.get("BINANCE_API_KEY", "")),
}

def get_api_status_text():
    labels = {
        "groq": "Groq AI (мозг)", "twelvedata": "TwelveData (свечи)",
        "mobula": "Mobula (DEX)", "coinalyze": "Coinalyze (OI/ликв)",
        "lunarcrush": "LunarCrush (соцсети)", "tavily": "Tavily (веб поиск)",
        "binance": "Binance (авторизован)",
    }
    lines = ["<b>Статус API:</b>"]
    for k, v in labels.items():
        lines.append(("OK " if _API_STATUS.get(k) else "NO ") + v)
    missing = [v for k, v in labels.items() if not _API_STATUS.get(k)]
    if missing:
        lines.append("Без ключей: " + ", ".join(missing))
    return "\n".join(lines)

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

    c.execute("""CREATE TABLE IF NOT EXISTS learning_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT,
        title TEXT,
        description TEXT,
        after_value TEXT,
        impact_score REAL DEFAULT 0.5,
        source TEXT,
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
            model="llama-3.1-8b-instant",
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

# Все таймфреймы: от 1м до 1М
BYBIT_INTERVALS = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15",
    "30m": "30", "1h": "60", "2h": "120", "4h": "240",
    "1d": "D", "3d": "D", "1w": "W", "1M": "M"
}

# Binance API intervals (официальные строки)
BINANCE_INTERVALS = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m",
    "30m": "30m", "1h": "1h", "2h": "2h", "4h": "4h",
    "1d": "1d", "3d": "3d", "1w": "1w", "1M": "1M"
}

# Категории таймфреймов для разных типов сделок
TF_CATEGORIES = {
    "scalp":  ["1m", "5m", "15m"],
    "swing":  ["1h", "4h"],
    "long":   ["1d", "1w", "1M"],
}

# Метки таймфреймов для отображения
TF_LABELS = {
    "1m": "1 мин", "3m": "3 мин", "5m": "5 мин", "15m": "15 мин",
    "30m": "30 мин", "1h": "1 час", "2h": "2 часа", "4h": "4 часа",
    "1d": "1 день", "3d": "3 дня", "1w": "1 неделя", "1M": "1 месяц"
}

TF_HOURS = {
    "1m": 0.1, "5m": 0.5, "15m": 4, "30m": 8,
    "1h": 12, "2h": 24, "4h": 48,
    "1d": 120, "3d": 360, "1w": 720, "1M": 2880
}

# ===== BINANCE API CLIENT (авторизованный) =====
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
_binance_client = None

def get_binance_client():
    """Возвращает авторизованный Binance клиент (lazy init)"""
    global _binance_client
    if _binance_client:
        return _binance_client
    if BINANCE_API_KEY and BINANCE_API_SECRET:
        try:
            from binance.client import Client
            _binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
            logging.info("Binance API client инициализирован ✅")
            return _binance_client
        except Exception as e:
            logging.warning(f"Binance Client init failed: {e}")
    return None


def get_full_history_binance(symbol, interval, limit=1000):
    """
    Получаем ПОЛНУЮ историю с авторизованного Binance API.
    Используется для старших ТФ (1d, 1w, 1M) и скальпа (1m, 5m).
    """
    client = get_binance_client()
    if not client:
        return []
    try:
        bi = BINANCE_INTERVALS.get(interval, interval)
        klines = client.get_klines(symbol=symbol, interval=bi, limit=limit)
        if not klines:
            return []
        candles = [{
            "open": float(k[1]), "high": float(k[2]),
            "low": float(k[3]), "close": float(k[4]),
            "volume": float(k[5])
        } for k in klines]
        logging.info(f"Binance API: {symbol} {interval} — {len(candles)} свечей")
        return candles
    except Exception as e:
        logging.warning(f"Binance API get_klines {symbol} {interval}: {e}")
        return []

# Динамический кэш топ-100 пар
pairs_cache = []
pairs_cache_time = 0
price_cache = {}
last_price_update = 0
candle_cache = {}  # {symbol_interval: (candles, timestamp)}

def get_top_pairs(limit=100):
    """Топ-N пар по объёму: Binance Futures → Binance Spot (Bybit убран — 403 на Render)"""
    global pairs_cache, pairs_cache_time
    if time.time() - pairs_cache_time < 3600 and pairs_cache:
        return pairs_cache

    # Принудительные монеты — всегда в списке первыми
    FORCED = [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "TONUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ARBUSDT",
        "ADAUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT",
        "NEARUSDT", "INJUSDT", "SUIUSDT", "APTUSDT", "OPUSDT",
        "UNIUSDT", "PEPEUSDT", "SHIBUSDT", "TRXUSDT", "XLMUSDT",
        "WLDUSDT", "TIAUSDT", "SEIUSDT", "JUPUSDT", "BONKUSDT",
    ]

    # 0. CryptoCompare — работает с Render без блокировок
    try:
        r = requests.get(
            "https://min-api.cryptocompare.com/data/top/mktcapfull",
            params={"limit": limit, "tsym": "USD"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code == 200:
            items = r.json().get("Data", [])
            cc_pairs = [i["CoinInfo"]["Name"] + "USDT" for i in items
                        if i.get("CoinInfo", {}).get("Name") and i["CoinInfo"]["Name"] != "USDT"]
            if len(cc_pairs) >= 10:
                combined = list(dict.fromkeys(FORCED + cc_pairs))[:limit]
                pairs_cache = combined
                pairs_cache_time = time.time()
                logging.info(f"Пары CryptoCompare: {len(combined)} шт")
                return pairs_cache
    except Exception as e:
        logging.warning(f"CryptoCompare top pairs: {e}")

    # 1. Binance Futures — основной источник
    try:
        r = requests.get(
            f"{BINANCE_F}/fapi/v1/ticker/24hr",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                data.sort(key=lambda x: float(x.get("quoteVolume", 0) or 0), reverse=True)
                top = [t["symbol"] for t in data if str(t.get("symbol","")).endswith("USDT")][:limit]
                if top:
                    combined = list(dict.fromkeys(FORCED + top))[:limit]
                    pairs_cache = combined
                    pairs_cache_time = time.time()
                    logging.info(f"Пары Binance Futures: {len(combined)} шт, топ: {combined[:5]}")
                    return pairs_cache
    except Exception as e:
        logging.warning(f"Binance Futures tickers: {e}")

    # 2. Binance Spot — fallback
    try:
        r = requests.get(
            f"{BINANCE}/api/v3/ticker/24hr",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                data.sort(key=lambda x: float(x.get("quoteVolume", 0) or 0), reverse=True)
                top = [t["symbol"] for t in data if str(t.get("symbol","")).endswith("USDT")][:limit]
                if top:
                    combined = list(dict.fromkeys(FORCED + top))[:limit]
                    pairs_cache = combined
                    pairs_cache_time = time.time()
                    logging.info(f"Пары Binance Spot: {len(combined)} шт")
                    return pairs_cache
    except Exception as e:
        logging.warning(f"Binance Spot tickers: {e}")

    logging.warning("get_top_pairs: используем fallback список")
    return pairs_cache if pairs_cache else FORCED

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
    # BTC
    "btc": "BTCUSDT", "биткоин": "BTCUSDT", "бтк": "BTCUSDT", "bitcoin": "BTCUSDT",
    "биток": "BTCUSDT", "битка": "BTCUSDT", "бит": "BTCUSDT",
    # ETH
    "eth": "ETHUSDT", "эфир": "ETHUSDT", "эфириум": "ETHUSDT", "ethereum": "ETHUSDT",
    "эф": "ETHUSDT", "ефир": "ETHUSDT",
    # SOL
    "sol": "SOLUSDT", "соль": "SOLUSDT", "солана": "SOLUSDT", "solana": "SOLUSDT",
    "сол": "SOLUSDT",
    # BNB
    "bnb": "BNBUSDT", "бнб": "BNBUSDT", "бинанс коин": "BNBUSDT",
    # XRP
    "xrp": "XRPUSDT", "рипл": "XRPUSDT", "ripple": "XRPUSDT", "хрп": "XRPUSDT",
    "xrpusdt": "XRPUSDT",
    # DOGE
    "doge": "DOGEUSDT", "додж": "DOGEUSDT", "dogecoin": "DOGEUSDT", "доге": "DOGEUSDT",
    # AVAX
    "avax": "AVAXUSDT", "авакс": "AVAXUSDT", "avalanche": "AVAXUSDT",
    # LINK
    "link": "LINKUSDT", "линк": "LINKUSDT", "chainlink": "LINKUSDT",
    # TON
    "ton": "TONUSDT", "тон": "TONUSDT", "toncoin": "TONUSDT", "тонкоин": "TONUSDT",
    # ARB
    "arb": "ARBUSDT", "арб": "ARBUSDT", "arbitrum": "ARBUSDT",
    # SUI
    "sui": "SUIUSDT", "суи": "SUIUSDT",
    # DOT
    "dot": "DOTUSDT", "полкадот": "DOTUSDT", "polkadot": "DOTUSDT",
    # ADA
    "ada": "ADAUSDT", "кардано": "ADAUSDT", "cardano": "ADAUSDT",
    # MATIC / POL
    "matic": "MATICUSDT", "матик": "MATICUSDT", "polygon": "MATICUSDT",
    # LTC
    "ltc": "LTCUSDT", "лайткоин": "LTCUSDT", "litecoin": "LTCUSDT",
    # ATOM
    "atom": "ATOMUSDT", "космос": "ATOMUSDT", "cosmos": "ATOMUSDT",
    # NEAR
    "near": "NEARUSDT", "ниар": "NEARUSDT",
    # PEPE
    "pepe": "PEPEUSDT", "пепе": "PEPEUSDT",
    # SHIB
    "shib": "SHIBUSDT", "шиб": "SHIBUSDT", "shiba": "SHIBUSDT",
    # TRX
    "trx": "TRXUSDT", "трон": "TRXUSDT", "tron": "TRXUSDT",
    # WIF
    "wif": "WIFUSDT",
    # RENDER
    "render": "RENDERUSDT", "рендер": "RENDERUSDT",
    # FET
    "fet": "FETUSDT", "fetch": "FETUSDT",
    # INJ
    "inj": "INJUSDT", "injective": "INJUSDT",
    # APT
    "apt": "APTUSDT", "aptos": "APTUSDT",
    # OP
    "op": "OPUSDT", "optimism": "OPUSDT",
    # UNI
    "uni": "UNIUSDT", "uniswap": "UNIUSDT", "юни": "UNIUSDT",
    # STX
    "stx": "STXUSDT", "stacks": "STXUSDT",
    # HBAR
    "hbar": "HBARUSDT", "hedera": "HBARUSDT",
    # XLM
    "xlm": "XLMUSDT", "stellar": "XLMUSDT", "стеллар": "XLMUSDT",
    # LDO
    "ldo": "LDOUSDT", "lido": "LDOUSDT",
    # AAVE
    "aave": "AAVEUSDT", "аав": "AAVEUSDT",
    # MKR
    "mkr": "MKRUSDT", "maker": "MKRUSDT",
    # CRV
    "crv": "CRVUSDT", "curve": "CRVUSDT",
    # FLOKI
    "floki": "FLOKIUSDT", "флоки": "FLOKIUSDT",
    # BONK
    "bonk": "BONKUSDT", "бонк": "BONKUSDT",
    # JUP
    "jup": "JUPUSDT", "jupiter": "JUPUSDT",
    # SEI
    "sei": "SEIUSDT",
    # TIA
    "tia": "TIAUSDT", "celestia": "TIAUSDT",
    # PYTH
    "pyth": "PYTHUSDT",
    # WLD
    "wld": "WLDUSDT", "worldcoin": "WLDUSDT",
}

def get_candles(symbol, interval="1h", limit=200):
    """
    Свечи: кэш →
      1. Binance Futures REST (без ключа, работает с Render) — PRIMARY для всех TF
      2. Binance Spot REST   (без ключа) — fallback
      3. Binance API клиент  (с ключом)  — если есть BINANCE_API_KEY
      4. CryptoCompare       — дополнительный
      5. CoinGecko           — последний резерв
      Bybit убран — даёт HTTP 403 с серверов Render
    """
    global candle_cache
    cache_key = f"{symbol}_{interval}"

    # Кэш (TTL зависит от TF)
    cache_ttl = 60 if interval in ("1m", "3m", "5m") else 180 if interval in ("15m", "30m") else 300 if interval in ("1h", "2h") else 600
    if cache_key in candle_cache:
        cached, ts = candle_cache[cache_key]
        if time.time() - ts < cache_ttl and len(cached) >= 20:
            return cached

    bi = BINANCE_INTERVALS.get(interval, interval)

    # 0. CryptoCompare — стабильно с любого IP, покрывает все ТФ включая 1m/5m
    try:
        cc = get_cryptocompare_candles(symbol, interval, limit)
        if cc and len(cc) >= 20:
            logging.info(f"Свечи CryptoCompare: {symbol} {interval} {len(cc)}шт")
            candle_cache[cache_key] = (cc, time.time())
            return cc
    except Exception as e:
        logging.warning(f"CryptoCompare candles {symbol} {interval}: {e}")

    # 1. Binance Futures REST — fallback
    try:
        r = requests.get(
            f"{BINANCE_F}/fapi/v1/klines",
            params={"symbol": symbol, "interval": bi, "limit": limit},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 5 and isinstance(data[0], list):
                candles = [{"open": float(c[1]), "high": float(c[2]),
                            "low": float(c[3]), "close": float(c[4]),
                            "volume": float(c[5])} for c in data]
                if len(candles) >= 20:
                    logging.info(f"Свечи Binance Futures: {symbol} {interval} {len(candles)}шт")
                    candle_cache[cache_key] = (candles, time.time())
                    return candles
    except Exception as e:
        logging.warning(f"Binance Futures {symbol} {interval}: {e}")

    # 2. Binance Spot REST
    try:
        r = requests.get(
            f"{BINANCE}/api/v3/klines",
            params={"symbol": symbol, "interval": bi, "limit": limit},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 5 and isinstance(data[0], list):
                candles = [{"open": float(c[1]), "high": float(c[2]),
                            "low": float(c[3]), "close": float(c[4]),
                            "volume": float(c[5])} for c in data]
                if len(candles) >= 20:
                    logging.info(f"Свечи Binance Spot: {symbol} {interval} {len(candles)}шт")
                    candle_cache[cache_key] = (candles, time.time())
                    return candles
    except Exception as e:
        logging.warning(f"Binance Spot {symbol} {interval}: {e}")

    # 3. Binance API клиент (авторизованный, если есть ключ)
    try:
        candles = get_full_history_binance(symbol, interval, limit)
        if candles and len(candles) >= 20:
            logging.info(f"Свечи Binance API: {symbol} {interval} {len(candles)}шт")
            candle_cache[cache_key] = (candles, time.time())
            return candles
    except Exception as e:
        logging.warning(f"Binance API client {symbol} {interval}: {e}")

    # 4. CryptoCompare
    try:
        cc_candles = get_cryptocompare_candles(symbol, interval, limit)
        if cc_candles and len(cc_candles) >= 20:
            logging.info(f"Свечи CryptoCompare: {symbol} {interval} {len(cc_candles)}шт")
            candle_cache[cache_key] = (cc_candles, time.time())
            return cc_candles
    except Exception as e:
        logging.warning(f"CryptoCompare {symbol} {interval}: {e}")

    # 4.5 TwelveData — если есть ключ
    if TWELVEDATA_KEY:
        try:
            td = get_twelvedata_candles(symbol, interval, limit)
            if td and len(td) >= 20:
                logging.info(f"TwelveData: {symbol} {interval} {len(td)}св")
                candle_cache[cache_key] = (td, time.time())
                return td
        except Exception as e:
            logging.debug(f"TwelveData candles {symbol}: {e}")

    # 5. CoinGecko — последний резерв
    cg_id = COINGECKO_IDS.get(symbol)
    if cg_id:
        try:
            days_map = {"1m": 1, "5m": 1, "15m": 1, "30m": 1,
                        "1h": 7, "4h": 30, "1d": 90, "1w": 365, "1M": 365}
            days = days_map.get(interval, 7)
            r = requests.get(
                f"https://api.coingecko.com/api/v3/coins/{cg_id}/ohlc",
                params={"vs_currency": "usd", "days": days},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=12
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and len(data) > 5:
                    candles = [{"open": float(c[1]), "high": float(c[2]),
                                "low": float(c[3]), "close": float(c[4]),
                                "volume": 0.0} for c in data[-limit:]]
                    if len(candles) >= 20:
                        logging.info(f"Свечи CoinGecko: {symbol} {interval} {len(candles)}шт")
                        candle_cache[cache_key] = (candles, time.time())
                        return candles
        except Exception as e:
            logging.warning(f"CoinGecko {symbol} {interval}: {e}")

    logging.warning(f"Нет свечей для {symbol} {interval}")
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


def get_twelvedata_candles(symbol, interval="1h", limit=200):
    if not TWELVEDATA_KEY:
        return []
    try:
        td_map = {"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1h","4h":"4h","1d":"1day"}
        base = symbol.replace("USDT","").replace("BUSD","")
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params={"symbol": base+"/USD","interval": td_map.get(interval,"1h"),
                    "outputsize": limit,"apikey": TWELVEDATA_KEY},
            headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        data = r.json()
        if data.get("status") == "error" or "values" not in data:
            return []
        return [{"open":float(v["open"]),"high":float(v["high"]),"low":float(v["low"]),
                 "close":float(v["close"]),"volume":float(v.get("volume",0))}
                for v in reversed(data["values"])]
    except Exception as e:
        logging.debug(f"TwelveData {symbol}: {e}")
        return []


def get_mobula_price(symbol):
    if not MOBULA_KEY:
        return {}
    try:
        base = symbol.replace("USDT","").replace("BUSD","")
        r = requests.get("https://api.mobula.io/api/1/market/data",
            params={"asset": base},
            headers={"Authorization": MOBULA_KEY,"User-Agent":"Mozilla/5.0"}, timeout=8)
        if r.status_code != 200:
            return {}
        d = r.json().get("data", {})
        return {"price":d.get("price",0),"volume_24h":d.get("volume",0),
                "change_24h":d.get("price_change_24h",0),"source":"mobula"}
    except Exception as e:
        logging.debug(f"Mobula {symbol}: {e}")
        return {}


def get_coinalyze_data(symbol):
    if not COINALYZE_KEY:
        return {}
    try:
        base = symbol.replace("USDT","")
        r = requests.get("https://api.coinalyze.net/v1/open-interest",
            params={"symbols": base+"USDT_PERP.A","api_key": COINALYZE_KEY},
            headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
        if r.status_code != 200 or not r.json():
            return {}
        d = r.json()[0] if isinstance(r.json(), list) else {}
        return {"open_interest":d.get("open_interest_usd",0),
                "oi_change_24h":d.get("open_interest_usd_change_24h_percent",0),
                "source":"coinalyze"}
    except Exception as e:
        logging.debug(f"Coinalyze {symbol}: {e}")
        return {}


def get_lunarcrush_data(symbol):
    if not LUNARCRUSH_KEY:
        return {}
    try:
        base = symbol.replace("USDT","").replace("BUSD","").lower()
        r = requests.get(f"https://lunarcrush.com/api4/public/coins/{base}/v1",
            headers={"Authorization":"Bearer "+LUNARCRUSH_KEY,"User-Agent":"Mozilla/5.0"},
            timeout=10)
        if r.status_code != 200:
            return {}
        d = r.json().get("data", {})
        gs = d.get("galaxy_score", 0)
        sent = d.get("sentiment", 50)
        return {"galaxy_score":gs,"sentiment":sent,"alt_rank":d.get("alt_rank",999),
                "signal":"BULLISH" if gs>60 and sent>60 else "BEARISH" if gs<30 else "NEUTRAL",
                "source":"lunarcrush"}
    except Exception as e:
        logging.debug(f"LunarCrush {symbol}: {e}")
        return {}


def get_historical_context(symbol, timeframe="1d"):
    """
    Анализ истории монеты — на каком уровне мы сейчас:
    - ATH / ATL за доступный период
    - Текущий уровень: где мы относительно хая/лоя (% от ATH)
    - Тренд: нисходящий / восходящий / боковик
    - Ключевые исторические уровни поддержки/сопротивления
    - Фаза рынка: накопление / распределение / рост / падение
    """
    try:
        # Берём 200 дневных свечей (~8 месяцев истории)
        candles = get_candles(symbol, "1d", 200)
        if len(candles) < 30:
            # Fallback — недельный TF
            candles = get_candles(symbol, "4h", 200)
        if len(candles) < 20:
            return None

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        current = closes[-1]

        # ATH / ATL за период
        period_high = max(highs)
        period_low = min(lows)

        # % от ATH и ATL
        pct_from_ath = round((current - period_high) / period_high * 100, 1)
        pct_from_atl = round((current - period_low) / period_low * 100, 1)

        # Тренд за последние 50 свечей
        if len(closes) >= 50:
            ma50 = sum(closes[-50:]) / 50
            ma20 = sum(closes[-20:]) / 20
            ma10 = sum(closes[-10:]) / 10
        else:
            ma50 = ma20 = ma10 = current

        # Определяем тренд
        if ma10 > ma20 > ma50:
            trend = "ВОСХОДЯЩИЙ ↗️"
            trend_key = "uptrend"
        elif ma10 < ma20 < ma50:
            trend = "НИСХОДЯЩИЙ ↘️"
            trend_key = "downtrend"
        elif abs(ma10 - ma50) / ma50 * 100 < 3:
            trend = "БОКОВИК ↔️"
            trend_key = "sideways"
        else:
            trend = "ПЕРЕХОДНЫЙ ⚡️"
            trend_key = "transition"

        # Ключевые уровни — смотрим на кластеры объёма и экстремумы
        # Разбиваем диапазон на 10 зон, ищем где больше всего свечей
        price_range = period_high - period_low
        zone_size = price_range / 10
        zones = {}
        for c in candles:
            zone = int((c["close"] - period_low) / zone_size)
            zone = max(0, min(9, zone))
            zones[zone] = zones.get(zone, 0) + 1

        # Топ-3 зоны по плотности = ключевые уровни
        top_zones = sorted(zones.items(), key=lambda x: x[1], reverse=True)[:3]
        key_levels = []
        for z, count in top_zones:
            level = period_low + z * zone_size + zone_size / 2
            key_levels.append(round(level, 4 if current < 10 else 2))

        key_levels.sort()

        # Ближайшая поддержка и сопротивление из ключевых уровней
        support = max([l for l in key_levels if l < current], default=period_low)
        resistance = min([l for l in key_levels if l > current], default=period_high)

        # Фаза рынка
        if pct_from_ath > -10:
            phase = "📈 У ХАЁВ — возможен разворот"
            phase_key = "near_high"
        elif pct_from_ath > -30:
            phase = "💪 СИЛЬНАЯ ЗОНА — выше середины"
            phase_key = "strong"
        elif pct_from_ath > -60:
            phase = "⚖️ СРЕДНЯЯ ЗОНА — середина диапазона"
            phase_key = "middle"
        elif pct_from_ath > -80:
            phase = "🔍 ЗОНА НАКОПЛЕНИЯ — возможен разворот вверх"
            phase_key = "accumulation"
        else:
            phase = "💎 ГЛУБОКИЙ ЛОУ — экстремальное значение"
            phase_key = "deep_low"

        # Последние 5 и 20 свечей — краткосрочный momentum
        change_5 = round((closes[-1] - closes[-5]) / closes[-5] * 100, 2) if len(closes) >= 5 else 0
        change_20 = round((closes[-1] - closes[-20]) / closes[-20] * 100, 2) if len(closes) >= 20 else 0

        return {
            "current": current,
            "period_high": period_high,
            "period_low": period_low,
            "pct_from_ath": pct_from_ath,
            "pct_from_atl": pct_from_atl,
            "trend": trend,
            "trend_key": trend_key,
            "phase": phase,
            "phase_key": phase_key,
            "support": support,
            "resistance": resistance,
            "key_levels": key_levels,
            "change_5": change_5,
            "change_20": change_20,
            "candles_count": len(candles),
        }
    except Exception as e:
        logging.warning(f"get_historical_context {symbol}: {e}")
        return None


def format_historical_context(symbol, hist):
    """Форматируем исторический контекст для сигнала"""
    if not hist:
        return ""
    p = hist["current"]
    fmt = lambda x: f"${x:,.4f}" if x < 1 else f"${x:,.3f}" if x < 100 else f"${x:,.2f}"
    return (
        f"📈 <b>Исторический контекст ({hist['candles_count']} свечей):</b>\n"
        f"🏔 Хай периода: <code>{fmt(hist['period_high'])}</code> ({hist['pct_from_ath']:+.1f}% от него)\n"
        f"🏔 Лоу периода: <code>{fmt(hist['period_low'])}</code> (+{hist['pct_from_atl']:.1f}% от него)\n"
        f"📊 Тренд: <b>{hist['trend']}</b>\n"
        f"🎯 Фаза: {hist['phase']}\n"
        f"🛡 Ближ. поддержка: <code>{fmt(hist['support'])}</code>\n"
        f"⚡️ Ближ. сопротивление: <code>{fmt(hist['resistance'])}</code>\n"
        f"📉 Изм. за 5 свечей: {hist['change_5']:+.2f}% | за 20: {hist['change_20']:+.2f}%"
    )

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
    """SMC анализ на одном ТФ — если smc_engine загружен использует его, иначе fallback"""
    if _SMC_ENGINE_OK:
        try:
            from smc_engine import smc_tf
            r = smc_tf(symbol, interval)
            if r and r.get("direction"):
                return r["direction"]
        except Exception:
            pass
    # Fallback — используем bot.py get_candles (Binance/CryptoCompare)
    candles = get_candles(symbol, interval, 150)
    if len(candles) < 20:
        return None
    highs, lows = find_swings(candles)
    classified = classify_swings(highs, lows)
    events = detect_events(candles, classified)
    return events[0]["direction"] if events else None

# ===== МУЛЬТИТАЙМФРЕЙМНЫЙ АНАЛИЗ =====

def multi_tf_analysis(symbol, timeframes=None):
    """
    Умный мультитаймфрейм анализ.
    Если smc_engine загружен — использует умный обход барьеров (8 источников).
    Если нет — fallback на старый код.
    """
    if _SMC_ENGINE_OK:
        return _smc_multi_tf(symbol, timeframes)

    # ── Старый код как fallback ──────────────────────────────
    if timeframes is None:
        timeframes = ["15m", "1h", "4h"]
    results = {}
    for tf in timeframes:
        results[tf] = smc_on_tf(symbol, tf)
    bullish = [tf for tf, d in results.items() if d == "BULLISH"]
    bearish = [tf for tf, d in results.items() if d == "BEARISH"]
    total = len(timeframes)
    if len(bullish) > len(bearish):
        direction, matched = "BULLISH", bullish
    elif len(bearish) > len(bullish):
        direction, matched = "BEARISH", bearish
    else:
        return None
    mc = len(matched)
    if mc == total and total >= 3:
        grade, ge, stars = "МЕГА ТОП", "🔥🔥🔥", "⭐⭐⭐⭐⭐"
    elif mc >= 3:
        grade, ge, stars = "ТОП СДЕЛКА", "🔥🔥", "⭐⭐⭐⭐"
    elif mc == 2:
        grade, ge, stars = "ХОРОШАЯ", "✅", "⭐⭐⭐"
    else:
        grade, ge, stars = "СЛАБАЯ", "⚠️", "⭐⭐"
    tf_status = ""
    for tf in timeframes:
        d = results.get(tf)
        icon = "🟢" if d == "BULLISH" else "🔴" if d == "BEARISH" else "⚪️"
        tf_status += f"{icon} {TF_LABELS.get(tf, tf)}: {d or 'нет сигнала'}\n"
    return {"direction": direction, "matched": matched, "match_count": mc,
            "total": total, "grade": grade, "grade_emoji": ge, "stars": stars,
            "tf_status": tf_status, "results": results}

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
                    asyncio.get_running_loop().call_later(1800, lambda s=symbol: pump_alerted.discard(s))

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

def analyze_trade_type(symbol, trade_type="swing"):
    """
    Анализ под конкретный тип сделки: scalp / swing / long.
    Использует соответствующие таймфреймы и формирует готовый сигнал.

    trade_type:
      scalp — 1m, 5m, 15m  (скальпинг, быстрые сделки)
      swing — 1h, 4h        (среднесрок)
      long  — 1d, 1w, 1M   (долгосрок)
    """
    try:
        tfs = TF_CATEGORIES.get(trade_type, ["1h", "4h"])

        # SMC анализ по каждому ТФ
        results = {}
        for tf in tfs:
            d = smc_on_tf(symbol, tf)
            results[tf] = d

        bullish = [tf for tf, d in results.items() if d == "BULLISH"]
        bearish = [tf for tf, d in results.items() if d == "BEARISH"]

        if len(bullish) > len(bearish):
            direction = "BULLISH"
            matched = bullish
        elif len(bearish) > len(bullish):
            direction = "BEARISH"
            matched = bearish
        else:
            return None

        match_count = len(matched)
        total = len(tfs)

        # Определяем качество сигнала
        if match_count == total:
            grade = "МЕГА ТОП" if total >= 3 else "ТОП СДЕЛКА"
            grade_emoji = "🔥🔥🔥" if total >= 3 else "🔥🔥"
        elif match_count >= 2:
            grade = "ХОРОШАЯ"
            grade_emoji = "✅"
        else:
            grade = "СЛАБАЯ"
            grade_emoji = "⚠️"

        # Свечи для уровней (выбираем средний ТФ из категории)
        main_tf = tfs[len(tfs)//2]
        candles = get_candles(symbol, main_tf, 200)
        if not candles or len(candles) < 20:
            return None

        price = candles[-1]["close"]
        ob = find_ob(candles, direction)
        fvg = find_fvg(candles, direction)
        risk = price * (0.008 if trade_type == "scalp" else 0.015 if trade_type == "swing" else 0.03)

        if direction == "BULLISH":
            entry = ob["top"] if ob else price
            sl = round(entry - risk, 6)
            tp1 = round(entry + risk * 2, 6)
            tp2 = round(entry + risk * 3, 6)
            tp3 = round(entry + risk * 5, 6)
        else:
            entry = ob["bottom"] if ob else price
            sl = round(entry + risk, 6)
            tp1 = round(entry - risk * 2, 6)
            tp2 = round(entry - risk * 3, 6)
            tp3 = round(entry - risk * 5, 6)

        # Исторический контекст
        hist = get_historical_context(symbol, "1d" if trade_type != "scalp" else "4h")

        # Долгосрочный тренд (для контекста)
        long_trend = ""
        if trade_type in ("scalp", "swing"):
            d_dir = smc_on_tf(symbol, "1d")
            w_dir = smc_on_tf(symbol, "1w")
            if d_dir or w_dir:
                long_trend = f"📅 Дневной: {d_dir or '?'} | Недельный: {w_dir or '?'}"

        # Формат цены
        fmt = lambda x: f"${x:,.6f}" if x < 0.01 else f"${x:,.4f}" if x < 1 else f"${x:,.3f}" if x < 100 else f"${x:,.2f}"

        # Время отработки по типу
        time_map = {"scalp": "15-60 мин", "swing": "4-24 ч", "long": "1-4 нед"}
        time_str = time_map.get(trade_type, "?")

        type_labels = {"scalp": "⚡️ СКАЛЬП", "swing": "🔄 СВИНГ", "long": "📈 ДОЛГОСРОК"}
        type_label = type_labels.get(trade_type, trade_type.upper())

        tf_status = ""
        for tf in tfs:
            d = results.get(tf)
            icon = "🟢" if d == "BULLISH" else "🔴" if d == "BEARISH" else "⚪️"
            tf_status += f"{icon} {TF_LABELS.get(tf, tf)}: {d or 'нет сигнала'}\n"

        hist_block = ""
        if hist:
            hist_block = (
                f"\n📊 <b>История:</b>\n"
                f"Тренд: {hist['trend']} | Фаза: {hist['phase']}\n"
                f"От хая периода: {hist['pct_from_ath']:+.1f}%\n"
                f"Поддержка: {fmt(hist['support'])} | Сопротивление: {fmt(hist['resistance'])}\n"
            )

        emoji = "🟢" if direction == "BULLISH" else "🔴"

        text = (
            f"{'━'*26}\n"
            f"{grade_emoji} {type_label} | <b>{grade}</b>\n"
            f"{emoji} <b>{symbol}</b> — {direction}\n"
            f"{'━'*26}\n\n"
            f"📐 <b>Таймфреймы ({trade_type}):</b>\n{tf_status}\n"
            f"💰 <b>Вход:</b> <code>{fmt(entry)}</code>\n"
            f"🛑 <b>Стоп:</b> <code>{fmt(sl)}</code>\n"
            f"🎯 <b>TP1:</b> <code>{fmt(tp1)}</code> (+2R)\n"
            f"🎯 <b>TP2:</b> <code>{fmt(tp2)}</code> (+3R)\n"
            f"🎯 <b>TP3:</b> <code>{fmt(tp3)}</code> (+5R)\n\n"
            f"⏱ <b>Время отработки:</b> {time_str}\n"
            f"{long_trend}\n"
            f"{hist_block}"
            f"{'━'*26}"
        )

        return {
            "symbol": symbol,
            "trade_type": trade_type,
            "direction": direction,
            "grade": grade,
            "text": text,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
        }

    except Exception as e:
        logging.error(f"analyze_trade_type {symbol} {trade_type}: {e}")
        return None


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
            try:
                rule_text = str(rule_row[0] if len(rule_row) == 1 else rule_row[1] or "")
                conf = float(rule_row[1] if len(rule_row) >= 2 else 0.5)
            except Exception:
                continue
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

        # Coinalyze — OI и ликвидации
        ca = get_coinalyze_data(symbol)
        if ca:
            oi_chg = ca.get("oi_change_24h", 0)
            if oi_chg > 5 and direction == "BULLISH":
                confluence.append(f"OI +{oi_chg:.1f}% — лонги добавляют")
                total_weight += 7
            elif oi_chg > 5 and direction == "BEARISH":
                confluence.append(f"OI +{oi_chg:.1f}% — шортисты добавляют")
                total_weight += 7
            elif oi_chg < -5:
                confluence.append(f"OI {oi_chg:.1f}% — позиции закрываются")

        # LunarCrush — социальный sentiment
        lc = get_lunarcrush_data(symbol)
        if lc:
            gs = lc.get("galaxy_score", 0)
            sig = lc.get("signal", "")
            if sig == "BULLISH" and direction == "BULLISH":
                confluence.append(f"LunarCrush Galaxy {gs} — бычий сентимент (+6)")
                total_weight += 6
            elif sig == "BEARISH" and direction == "BEARISH":
                confluence.append(f"LunarCrush Galaxy {gs} — медвежий сентимент (+6)")
                total_weight += 6
            elif sig == "BEARISH" and direction == "BULLISH":
                confluence.append(f"LunarCrush: соцсети негативные ({gs})")

        # Режим рынка
        if regime["mode"] == "TRENDING":
            confluence.append(f"✅ Рынок в тренде ({regime['direction']})")
            total_weight += 5

        # ── Новые SMC факторы ──────────────────────────────────
        if _SMC_ENGINE_OK:
            try:
                highs_l, lows_l = find_swings(candles)

                # Liquidity Sweep — сильный сигнал разворота
                sweep = detect_liquidity_sweep(candles, highs_l, lows_l)
                if sweep and sweep["direction"] == direction:
                    sw = 12 if sweep["strength"] == "HIGH" else 7
                    confluence.append(f"✅ Liquidity Sweep {sweep['type']} (вес {sw})")
                    total_weight += sw

                # OB→FVG→OB цепочка — тройное подтверждение
                chain = find_ob_fvg_chain(candles, direction)
                if chain:
                    confluence.append(f"✅ OB→FVG→OB цепочка — тройной confluence (+15)")
                    total_weight += 15

                # Divergence — расхождение RSI с ценой
                diverge = detect_divergence(candles, direction)
                if diverge and diverge["strength"] == "STRONG":
                    confluence.append(f"✅ {diverge['type']} RSI:{diverge['rsi_current']} (+8)")
                    total_weight += 8
                elif diverge:
                    confluence.append(f"➡️ {diverge['type']} слабая")
                    total_weight += 4

                # Premium/Discount zone
                pd = get_premium_discount(candles)
                if pd["bias"] == direction[:4] or                    (direction == "BULLISH" and pd["zone"] == "DISCOUNT") or                    (direction == "BEARISH" and pd["zone"] == "PREMIUM"):
                    confluence.append(f"✅ {pd['zone']} зона ({pd['pct']:.0f}%) — правильная сторона (+6)")
                    total_weight += 6
                elif pd["zone"] in ("PREMIUM", "DISCOUNT"):
                    confluence.append(f"⚠️ {pd['zone']} зона ({pd['pct']:.0f}%) — против тренда")

                # Imbalance zones как цели
                imb = find_imbalance_zones(candles)
                if imb:
                    nearest = min(imb, key=lambda z: abs(z["top"] - price))
                    confluence.append(f"📍 Имбаланс {nearest['type']}: {nearest['bottom']:.4f}–{nearest['top']:.4f}")
            except Exception as _e:
                logging.debug(f"new_smc_confluence {symbol}: {_e}")

        # ── CVD + Whale Detection ────────────────────────────────
        whale_desc = ""
        news_impact_text = ""
        try:
            cvd = calculate_cvd(candles)
            if cvd["signal"] == direction[:4] or cvd["signal"] == direction:
                confluence.append(f"✅ CVD {cvd['trend']} — давление {'покупателей' if direction=='BULLISH' else 'продавцов'} подтверждает (+7)")
                total_weight += 7
            elif cvd["divergence"]:
                if (cvd["divergence"] == "BEARISH_DIV" and direction == "BULLISH") or                    (cvd["divergence"] == "BULLISH_DIV" and direction == "BEARISH"):
                    confluence.append(f"⚠️ CVD дивергенция — цена и объём расходятся (-5)")
                    total_weight -= 5

            whale = detect_whale_candles(candles)
            if whale["found"]:
                if whale["signal"] == direction[:4] or whale["signal"] == direction:
                    sw = 10 if whale["strength"] >= 7 else 6
                    confluence.append(f"✅ {whale['description']} (+{sw})")
                    total_weight += sw
                    # Groq контекст для кита
                    if _LEARNING_OK and whale["spike"] >= 2.5:
                        whale_desc = _learn_whale_ctx(symbol, whale["spike"], direction)
                else:
                    confluence.append(f"⚠️ {whale['description']} — против сигнала")
                    total_weight -= 4

            vp = get_volume_profile(candles)
            if vp["poc"] > 0:
                poc_dist = abs(price - vp["poc"]) / price * 100
                if poc_dist < 1.0:
                    confluence.append(f"📍 Цена у POC {vp['poc']:.4f} — зона реакции")
                elif vp["current_zone"] == "ABOVE_POC" and direction == "BULLISH":
                    confluence.append(f"✅ Выше POC {vp['poc']:.4f} — бычья структура объёма (+4)")
                    total_weight += 4
                elif vp["current_zone"] == "BELOW_POC" and direction == "BEARISH":
                    confluence.append(f"✅ Ниже POC {vp['poc']:.4f} — медвежья структура объёма (+4)")
                    total_weight += 4
        except Exception as _e:
            logging.debug(f"cvd_whale {symbol}: {_e}")

        # Паттерн-матчер — историческая точность при похожих условиях
        if _LEARNING_OK:
            try:
                pat = _learn_patterns(symbol, direction, timeframe,
                                      regime.get("mode","UNKNOWN"), total_weight)
                if pat["found"] and pat["samples"] >= 5:
                    if pat["win_rate"] >= 60:
                        confluence.append(f"✅ История: {pat['win_rate']:.0f}% WR ({pat['samples']} случаев)")
                        total_weight += 5
                    elif pat["win_rate"] < 35:
                        confluence.append(f"⚠️ История: {pat['win_rate']:.0f}% WR — слабая статистика")
                        total_weight -= 5

                # Streak: если серия потерь — повышаем порог
                streak_threshold = _learn_streak_threshold()
            except Exception as _e:
                logging.debug(f"learning confluence {symbol}: {_e}")
                streak_threshold = 18
        else:
            streak_threshold = 18

        # Минимальный порог — учитывает серию потерь
        min_weight = max(streak_threshold, 18 if mtf["match_count"] >= 3 else 22)
        if total_weight < min_weight:
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

        # ── 6.5 Groq инсайт — почему эта сделка интересна (async) ──
        groq_insight = ""
        if _LEARNING_OK and total_weight >= 40:
            try:
                groq_insight = _learn_trade_insight(
                    symbol, direction, mtf.get("grade","?"),
                    total_weight, regime.get("mode","UNKNOWN"), timeframe
                )
            except Exception:
                pass

        # ── 7. Уровень инвалидации (когда сигнал отменяется) ──
        invalidation = sl  # Если цена закроется за стопом — сигнал недействителен
        inv_text = f"Сигнал отменяется если цена закроется {'ниже' if direction == 'BULLISH' else 'выше'} <code>{invalidation:.4f}</code>"

        # ── 8. Предупреждение об экономических событиях ──
        econ_warn = f"\n⚠️ <b>Макро:</b> {econ}\n" if econ else ""

        # ── 9. Исторический контекст ──
        hist = get_historical_context(symbol)
        hist_text = "\n" + format_historical_context(symbol, hist) + "\n" if hist else ""

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
            f"📋 <b>Confluence [{total_weight}/100]:</b>\n{conf_text}\n"
            f"{hist_text}\n"
            f"💬 <b>APEX думает:</b>\n<i>{signal_comment}</i>\n"
            + (f"\n🤖 <b>Groq:</b> <i>{groq_insight}</i>\n" if groq_insight else "")
            + (f"\n🐋 <b>Киты:</b> <i>{whale_desc}</i>\n" if whale_desc else "")
            + f"{'━'*26}"
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

                # Groq анализирует каждую закрытую сделку и создаёт правило
                if _LEARNING_OK:
                    asyncio.get_event_loop().run_in_executor(
                        None, _learn_analyze_trade, sig_id
                    )

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

# ===== ЖИВОЙ АНАЛИЗ — ГДЕ МЫ СЕЙЧАС =====

def live_position_analysis(symbol, timeframe="1h"):
    """Реальная цена + структура SMC → что делать прямо сейчас"""
    try:
        candles = get_candles(symbol, timeframe, 200)
        if len(candles) < 30:
            return None
        price_now  = candles[-1]["close"]
        price_open = candles[-1]["open"]
        candle_dir = "🟢" if price_now >= price_open else "🔴"
        highs, lows = find_swings(candles, lookback=5)
        classified  = classify_swings(highs, lows)
        events      = detect_events(candles, classified)
        trend       = events[0]["direction"] if events else "UNCLEAR"
        h_vals = sorted([h[1] for h in highs[-6:]], reverse=True) if highs else []
        l_vals = sorted([l[1] for l in lows[-6:]])                 if lows  else []
        nearest_res = min([h for h in h_vals if h > price_now * 1.002], default=None)
        nearest_sup = max([l for l in l_vals if l < price_now * 0.998], default=None)
        ob_bull  = find_ob(candles, "BULLISH");  ob_bear  = find_ob(candles, "BEARISH")
        fvg_bull = find_fvg(candles, "BULLISH"); fvg_bear = find_fvg(candles, "BEARISH")
        in_bull_ob  = ob_bull  and ob_bull["bottom"]  <= price_now <= ob_bull["top"]
        in_bear_ob  = ob_bear  and ob_bear["bottom"]  <= price_now <= ob_bear["top"]
        in_bull_fvg = fvg_bull and fvg_bull["bottom"] <= price_now <= fvg_bull["top"]
        in_bear_fvg = fvg_bear and fvg_bear["bottom"] <= price_now <= fvg_bear["top"]
        last3 = candles[-3:]
        bulls = sum(1 for c in last3 if c["close"] > c["open"])
        momentum = "🚀 растём" if bulls >= 2 else "💥 падаем" if bulls == 0 else "😐 боковик"
        vols = [c["volume"] for c in candles[-20:]]
        avg_vol = sum(vols[:-1]) / max(len(vols)-1, 1)
        vol_ratio = candles[-1]["volume"] / avg_vol if avg_vol > 0 else 1
        vol_tag = "🔥 высокий" if vol_ratio > 1.5 else "📉 низкий" if vol_ratio < 0.6 else "➡️ средний"
        dist_res = ((nearest_res - price_now) / price_now * 100) if nearest_res else None
        dist_sup = ((price_now - nearest_sup) / price_now * 100) if nearest_sup else None
        if trend == "BULLISH":
            if in_bull_ob or in_bull_fvg:   action, reason, risk = "✅ ВХОДИТЬ ЛОНГ",    "В зоне Bull OB/FVG — идеальная точка", "низкий"
            elif nearest_sup and dist_sup and dist_sup < 1.0: action, reason, risk = "✅ ЛОНГ у поддержки", "Тренд ↑, цена у поддержки", "низкий"
            elif nearest_res and dist_res and dist_res < 0.5: action, reason, risk = "⚠️ ЖДАТЬ пробоя",   "У сопротивления — жди пробой", "высокий"
            else: action, reason, risk = "⏳ ЖДАТЬ", "Тренд бычий, нет точки входа", "средний"
        elif trend == "BEARISH":
            if in_bear_ob or in_bear_fvg:   action, reason, risk = "🔴 ВХОДИТЬ ШОРТ",    "В зоне Bear OB/FVG — точка на продажу", "низкий"
            elif nearest_res and dist_res and dist_res < 1.0: action, reason, risk = "🔴 ШОРТ у сопр.",   "Тренд ↓, цена у сопротивления", "низкий"
            elif nearest_sup and dist_sup and dist_sup < 0.5: action, reason, risk = "⚠️ ЖДАТЬ пробоя",   "У поддержки — жди пробой", "высокий"
            else: action, reason, risk = "⏳ ЖДАТЬ", "Тренд медвежий, нет точки", "средний"
        else:
            action, reason, risk = "😴 НЕТ СИГНАЛА", "Боковик или смена тренда", "высокий"
        def fmt(p):
            if p is None: return "—"
            return f"${p:,.4f}" if p < 1 else f"${p:,.3f}" if p < 10 else f"${p:,.2f}"
        lines = [
            f"📍 <b>{symbol}</b> [{TF_LABELS.get(timeframe,timeframe)}] — СЕЙЧАС",
            f"{'━'*26}",
            f"{candle_dir} Цена: <code>{fmt(price_now)}</code>",
            f"⚡️ {momentum}  |  📊 Объём: {vol_tag} (×{vol_ratio:.1f})",
            f"",
            f"📐 Структура:  {'🟢' if trend=='BULLISH' else '🔴' if trend=='BEARISH' else '⚪️'} <b>{trend}</b>",
        ]
        if nearest_res: lines.append(f"🔴 Сопротивление: <code>{fmt(nearest_res)}</code> (+{dist_res:.1f}%)")
        if nearest_sup: lines.append(f"🟢 Поддержка:     <code>{fmt(nearest_sup)}</code> (-{dist_sup:.1f}%)")
        lines.append(f"\n<b>🗺 Зоны:</b>")
        if ob_bull:  lines.append(f"🟦 Bull OB: <code>{fmt(ob_bull['bottom'])}–{fmt(ob_bull['top'])}</code>" + (" ← ТЫ ЗДЕСЬ" if in_bull_ob else ""))
        if ob_bear:  lines.append(f"🟥 Bear OB: <code>{fmt(ob_bear['bottom'])}–{fmt(ob_bear['top'])}</code>" + (" ← ТЫ ЗДЕСЬ" if in_bear_ob else ""))
        if fvg_bull: lines.append(f"🔵 Bull FVG: <code>{fmt(fvg_bull['bottom'])}–{fmt(fvg_bull['top'])}</code>" + (" ← ТЫ ЗДЕСЬ" if in_bull_fvg else ""))
        if fvg_bear: lines.append(f"🟠 Bear FVG: <code>{fmt(fvg_bear['bottom'])}–{fmt(fvg_bear['top'])}</code>" + (" ← ТЫ ЗДЕСЬ" if in_bear_fvg else ""))
        sl_hint = ""
        if "ЛОНГ" in action and nearest_sup:
            sl = nearest_sup * 0.997; tp = price_now + (price_now - sl) * 2
            sl_hint = f"\n🛡 SL: <code>{fmt(sl)}</code>  |  🎯 TP: <code>{fmt(tp)}</code>  (RR 1:2)"
        elif "ШОРТ" in action and nearest_res:
            sl = nearest_res * 1.003; tp = price_now - (sl - price_now) * 2
            sl_hint = f"\n🛡 SL: <code>{fmt(sl)}</code>  |  🎯 TP: <code>{fmt(tp)}</code>  (RR 1:2)"
        lines += [f"\n{'━'*26}", f"🎯 <b>{action}</b>", f"<i>{reason}</i>", f"⚠️ Риск: {risk}{sl_hint}"]
        return "\n".join(lines)
    except Exception as e:
        logging.error(f"live_position_analysis {symbol}: {e}")
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
    """Лог событий — пишем в brain_log и learning_history"""
    try:
        conn = sqlite3.connect("brain.db")
        conn.execute(
            "INSERT INTO brain_log VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)",
            (event_type, description[:200], impact[:100])
        )
        titles = {
            "rule_added":       "📌 Новое правило",
            "rule_strengthened":"💪 Правило укреплено",
            "rule_weakened":    "⚠️ Правило ослаблено",
            "error_analyzed":   "🔍 Ошибка разобрана",
            "web_learned":      "🌐 Узнал из интернета",
            "signal_win":       "✅ Сигнал выиграл",
            "signal_loss":      "❌ Сигнал проиграл",
            "self_synthesis":   "🧠 Синтез знаний",
            "auto_patch":       "🔧 Авто-исправление",
        }
        scores = {"rule_added":0.7,"rule_strengthened":0.6,"rule_weakened":0.5,
                  "error_analyzed":0.8,"signal_win":0.6,"signal_loss":0.7,
                  "web_learned":0.5,"self_synthesis":0.9,"auto_patch":0.9}
        conn.execute(
            "INSERT INTO learning_history (event_type,title,description,after_value,impact_score,source) VALUES (?,?,?,?,?,?)",
            (event_type, titles.get(event_type, f"📎 {event_type}"),
             description[:300], impact[:200], scores.get(event_type,0.5), "auto")
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

# Трекер суточного расхода токенов
_groq_tokens_used = 0
_groq_tokens_reset_ts = 0
_GROQ_DAILY_LIMIT = 480_000  # Оставляем 20k буфер от 500k

def _track_tokens(count: int):
    global _groq_tokens_used, _groq_tokens_reset_ts
    now = time.time()
    # Сбрасываем счётчик каждые 24ч
    if now - _groq_tokens_reset_ts > 86400:
        _groq_tokens_used = 0
        _groq_tokens_reset_ts = now
    _groq_tokens_used += count

def _tokens_available() -> bool:
    return _groq_tokens_used < _GROQ_DAILY_LIMIT

def ask_groq(prompt, max_tokens=800):
    """
    Умный запрос к Groq:
    - Трекинг суточного лимита токенов
    - Retry при rate limit (до 3 попыток)
    - Fallback на модели с отдельными лимитами
    - Сокращает промпт если он слишком большой
    """
    # Сокращаем промпт если больше 6000 символов — экономим токены
    if len(prompt) > 6000:
        prompt = prompt[:5000] + "\n[промпт сокращён для экономии токенов]"

    models = [
        "llama-3.1-8b-instant",       # основная — 500k TPD/день
        "llama-3.1-70b-specdec",      # fallback — 100k TPD но быстрее 70b-versatile
        "gemma2-9b-it",               # резерв — отдельный лимит
    ]

    for attempt in range(3):
        model = models[min(attempt, len(models) - 1)]
        try:
            r = groq_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                timeout=30,
            )
            _track_tokens(len(prompt) // 4 + max_tokens)  # приблизительный подсчёт
            return r.choices[0].message.content
        except Exception as e:
            err_str = str(e).lower()
            if "rate_limit" in err_str or "429" in err_str or "rate limit" in err_str:
                wait = 15 * (attempt + 1)  # 15, 30, 45 сек
                logging.warning(f"Groq rate limit (попытка {attempt+1}), жду {wait}с...")
                time.sleep(wait)
                continue
            elif "model" in err_str and "not found" in err_str:
                logging.warning(f"Модель {model} недоступна, пробую следующую")
                continue
            else:
                logging.error(f"Groq error (attempt {attempt+1}): {e}")
                if attempt < 2:
                    time.sleep(5)
                    continue
                return None

    logging.error("Groq: все попытки исчерпаны")
    return None

# ===== APEX BRAIN v2 — АВТОНОМНОЕ САМООБУЧЕНИЕ =====
# Бот постоянно растёт: читает рынок, запоминает паттерны, строит модель мира

# Кэш для тяжёлых вызовов — не грузим Groq каждый раз
_groq_cache = {}
_groq_cache_time = {}
GROQ_CACHE_TTL = 300  # 5 минут

def ask_groq_cached(prompt, max_tokens=400, cache_key=None):
    """ask_groq с кэшированием — одинаковые запросы не дублируются"""
    key = cache_key or prompt[:80]
    now = time.time()
    if key in _groq_cache and now - _groq_cache_time.get(key, 0) < GROQ_CACHE_TTL:
        return _groq_cache[key]
    result = ask_groq(prompt, max_tokens)
    if result:
        _groq_cache[key] = result
        _groq_cache_time[key] = now
    return result


def fetch_url_text(url, timeout=8, max_chars=2000):
    """
    Читает страницу и возвращает чистый текст без HTML тегов.
    Работает без внешних библиотек — только стандартный re.
    """
    import re
    try:
        r = requests.get(url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=timeout)
        if r.status_code != 200:
            return ""
        html = r.text
        # Убираем скрипты, стили, мета-теги
        html = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>',  ' ', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<[^>]+>', ' ', html)
        # Убираем лишние пробелы
        text = re.sub(r'\s+', ' ', html).strip()
        # Берём только первые max_chars символов (самое важное в начале)
        return text[:max_chars]
    except Exception as e:
        logging.warning(f"fetch_url_text {url}: {e}")
        return ""


def search_web_free(query, limit=5):
    """
    НАСТОЯЩИЙ поиск в интернете без API ключей:
    1. CoinTelegraph / CoinDesk — читаем ПОЛНЫЙ ТЕКСТ статей (не только заголовки)
    2. CryptoCompare News API — бесплатный, без ключа
    3. Alternative.me Fear & Greed — реальный индекс страха
    4. Messari free API — данные по монете
    5. DuckDuckGo Instant Answer — энциклопедический контекст
    """
    results = []
    query_lower = query.lower()
    words = [w for w in query_lower.split() if len(w) > 3][:4]

    # ── 1. CryptoCompare News API — бесплатный, даёт реальные новости с текстом ──
    try:
        r = requests.get(
            "https://min-api.cryptocompare.com/data/v2/news/",
            params={"lang": "EN", "sortOrder": "latest"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            news = data.get("Data", [])
            # Ищем релевантные по ключевым словам
            relevant = [n for n in news if any(w in (n.get("title","") + n.get("body","")).lower() for w in words)]
            # Если нет релевантных — берём просто свежие
            to_use = relevant[:3] if relevant else news[:3]
            for n in to_use:
                title = n.get("title", "")
                body  = n.get("body", "")[:400]
                src   = n.get("source_info", {}).get("name", "CryptoCompare")
                results.append(f"[{src}] {title}\n{body}")
    except Exception as e:
        logging.warning(f"CryptoCompare news: {e}")

    # ── 2. RSS с чтением ПОЛНОГО ТЕКСТА статей ──
    rss_sources = [
        ("https://cointelegraph.com/rss",                  "CoinTelegraph"),
        ("https://www.coindesk.com/arc/outboundfeeds/rss/","CoinDesk"),
        ("https://decrypt.co/feed",                        "Decrypt"),
        ("https://cryptonews.com/news/feed/",              "CryptoNews"),
    ]
    fetched_count = 0
    for feed_url, source_name in rss_sources:
        if fetched_count >= 3:
            break
        try:
            items = parse_rss(feed_url, source_name, limit=10)
            # Фильтруем по теме если есть ключевые слова
            if words:
                items = [i for i in items if any(w in i["title"].lower() for w in words)] or items[:2]
            else:
                items = items[:2]

            for item in items[:2]:
                title = item["title"]
                url   = item.get("url", "")
                # Читаем полный текст статьи если есть URL
                body = ""
                if url:
                    body = fetch_url_text(url, timeout=6, max_chars=800)
                if body and len(body) > 100:
                    results.append(f"[{source_name} FULL] {title}\n{body}")
                else:
                    results.append(f"[{source_name}] {title}")
                fetched_count += 1
        except Exception as e:
            logging.warning(f"RSS fetch {source_name}: {e}")

    # ── 3. Alternative.me Fear & Greed Index ──
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=3", timeout=5)
        if r.status_code == 200:
            fg = r.json().get("data", [])
            if fg:
                val   = fg[0].get("value", "?")
                label = fg[0].get("value_classification", "?")
                prev  = fg[1].get("value", "?") if len(fg) > 1 else "?"
                results.append(f"[Fear&Greed] Сейчас: {val} ({label}), вчера: {prev}. " +
                               ("Рынок жадный — возможна коррекция." if int(val) > 70 else
                                "Рынок в страхе — возможный разворот вверх." if int(val) < 30 else
                                "Нейтральный рынок."))
    except:
        pass

    # ── 4. Messari free API для монет ──
    coin_map = {"bitcoin":"BTC","ethereum":"ETH","solana":"SOL","btc":"BTC","eth":"ETH","sol":"SOL",
                "bnb":"BNB","xrp":"XRP","doge":"DOGE","avax":"AVAX","link":"LINK","ton":"TON"}
    for word in words:
        sym = coin_map.get(word)
        if sym:
            try:
                r = requests.get(
                    f"https://data.messari.io/api/v1/assets/{sym.lower()}/metrics",
                    headers={"User-Agent": "Mozilla/5.0"}, timeout=6
                )
                if r.status_code == 200:
                    d = r.json().get("data", {}).get("market_data", {})
                    price = d.get("price_usd", 0)
                    chg24 = d.get("percent_change_usd_last_24_hours", 0)
                    chg7  = d.get("percent_change_usd_last_7_days", 0)
                    vol   = d.get("volume_last_24_hours", 0)
                    results.append(f"[Messari {sym}] Цена: ${price:.4f} | 24ч: {chg24:+.1f}% | 7д: {chg7:+.1f}% | Объём: ${vol:,.0f}")
                    break
            except:
                pass

    # ── 5. DuckDuckGo Instant Answer (энциклопедический контекст) ──
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": query + " cryptocurrency 2025", "format": "json", "no_html": 1, "skip_disambig": 1},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=6
        )
        data = r.json()
        if data.get("AbstractText"):
            results.append(f"[DDG] {data['AbstractText'][:400]}")
    except:
        pass

    return results[:8]


def learn_from_web(topic, save=True):
    """
    Реальный цикл обучения:
    1. CryptoCompare News API — полный текст новостей
    2. Читает статьи по URL из RSS
    3. Fear & Greed index
    4. Messari данные по монете
    5. AI извлекает торговые факты с confidence
    """
    try:
        web_results = search_web_free(topic)
        if not web_results:
            return None

        total_chars = sum(len(r) for r in web_results)
        facts_text = "\n\n".join(web_results)
        old_knowledge = get_knowledge(topic)

        prompt = f"""Ты APEX — AI трейдер. Прочитай данные и извлеки торговые знания.

ТЕМА: {topic}
ИСТОЧНИКИ ({len(web_results)} шт, {total_chars} символов):
{facts_text[:3000]}

{f"ЧТО УЖЕ ЗНАЛ: {old_knowledge[:200]}" if old_knowledge else "Изучаю впервые."}

Извлеки КОНКРЕТНЫЕ факты. Верни JSON:
{{
  "key_facts": ["факт с цифрами/датой", "факт 2", "факт 3"],
  "market_impact": "влияние на BTC/альты прямо сейчас (1-2 предл.)",
  "trading_signal": "ЛОНГ/ШОРТ/ЖДАТЬ — конкретно и почему",
  "timeframe": "сегодня/эта неделя/месяц",
  "confidence": 0.0-1.0,
  "new_vs_old": "что изменилось"
}}

Только JSON."""

        response = ask_groq(prompt, max_tokens=500)
        if not response:
            return None

        try:
            clean = response.strip().replace("```json", "").replace("```", "")
            start = clean.find("{")
            end   = clean.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(clean[start:end])

                if save:
                    confidence = float(data.get("confidence", 0.5))
                    summary = (
                        f"[{topic}] "
                        f"Факты: {'; '.join(data.get('key_facts', [])[:3])}. "
                        f"Влияние: {data.get('market_impact', '')}. "
                        f"Сигнал: {data.get('trading_signal', '')}. "
                        f"Горизонт: {data.get('timeframe', '')}."
                    )
                    save_knowledge(topic, summary, "web-learning")
                    log_brain_event(
                        "web_learned",
                        f"Тема: {topic} ({len(web_results)} источников) — {data.get('market_impact', '')[:100]}",
                        f"Сигнал: {data.get('trading_signal', '')[:80]}"
                    )
                    signal = data.get("trading_signal", "").lower()
                    if any(w in signal for w in ["лонг", "покупать", "buy"]):
                        save_self_rule("market", f"[{topic}] {data.get('market_impact', '')[:100]}", min(0.7, confidence), "web-learning")
                    elif any(w in signal for w in ["шорт", "продавать", "sell"]):
                        save_self_rule("avoid",  f"[{topic}] осторожно: {data.get('market_impact', '')[:80]}", min(0.7, confidence), "web-learning")

                return data

        except json.JSONDecodeError:
            if save and response:
                save_knowledge(topic, response[:500], "web-learning-raw")
                log_brain_event("web_learned", f"Тема: {topic} (raw)", "")
            return response

    except Exception as e:
        logging.error(f"learn_from_web {topic}: {e}")
        return None


# Темы для автономного изучения — бот сам выбирает по времени суток
LEARN_TOPICS_MORNING = [
    "bitcoin price prediction today",
    "crypto market open analysis",
    "BTC technical analysis",
    "altcoin season indicators",
]
LEARN_TOPICS_EVENING = [
    "crypto market summary today",
    "DXY dollar index impact crypto",
    "federal reserve interest rates crypto",
    "ethereum network activity",
]
LEARN_TOPICS_ALWAYS = [
    "solana ecosystem news",
    "defi total value locked",
    "bitcoin dominance trend",
    "crypto fear greed index analysis",
]


def self_diagnose_and_grow():
    """
    Groq анализирует что боту не хватает и дописывает мозг самостоятельно.
    Запускается каждые 6 часов. Результат: новые правила + знания в brain.db.
    """
    try:
        conn = sqlite3.connect("brain.db")
        # Собираем контекст: ошибки источников
        try:
            src_errors = conn.execute(
                "SELECT source, error FROM barrier_log WHERE success=0 ORDER BY id DESC LIMIT 8"
            ).fetchall()
            errors_txt = " | ".join([f"{s}:{e[:40]}" for s,e in src_errors]) or "нет"
        except:
            errors_txt = "нет"
        # Статистика правил
        rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        obs_count  = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        # Последние SL — что шло не так
        try:
            sl_rows = conn.execute(
                "SELECT symbol, timeframe FROM signals WHERE result='sl' ORDER BY id DESC LIMIT 5"
            ).fetchall()
            sl_txt = " ".join([f"{r[0]}/{r[1]}" for r in sl_rows]) or "нет"
        except:
            sl_txt = "нет"
        conn.close()

        # Статус API
        available_apis = [k for k,v in _API_STATUS.items() if v]
        missing_apis   = [k for k,v in _API_STATUS.items() if not v]

        prompt = (
            "Ты APEX — SMC торговый бот на Python. Проанализируй свои возможности и найди пробелы."
            f" Доступные API: {', '.join(available_apis)}."
            f" Недоступные API: {', '.join(missing_apis)}."
            f" Ошибки источников свечей: {errors_txt}."
            f" Последние SL сигналы: {sl_txt}."
            f" Правил в мозге: {rule_count}. Наблюдений: {obs_count}."
            " Ответь JSON без markdown:"
            ' {"gaps":["чего не хватает для лучшего анализа"],'
            '"free_apis":["бесплатные API которые стоит добавить с URL"],'
            '"rules":["конкретные торговые правила для улучшения точности"],'
            '"priority":"самое важное улучшение прямо сейчас"}'
        )

        response = ask_groq(prompt, max_tokens=500)
        if not response:
            return

        import json as _j
        try:
            clean = response.strip().replace("```json","").replace("```","")
            analysis = _j.loads(clean)
        except:
            save_knowledge("self_diagnosis_raw", response[:500], "self-groq")
            logging.info("[SelfGrow] Сохранён сырой анализ")
            return

        conn2 = sqlite3.connect("brain.db")
        saved = 0

        # Сохраняем пробелы как знания
        for gap in analysis.get("gaps", [])[:5]:
            save_knowledge("gap", gap[:200], "self-diagnosis")

        # Бесплатные API которые стоит добавить — записываем в мозг
        for api in analysis.get("free_apis", [])[:3]:
            save_knowledge("suggested_api", api[:200], "self-diagnosis")
            logging.info(f"[SelfGrow] Предложен API: {api[:80]}")

        # Торговые правила — добавляем в self_rules
        for rule in analysis.get("rules", [])[:5]:
            try:
                ex = conn2.execute("SELECT id FROM self_rules WHERE rule=?", (rule[:200],)).fetchone()
                if not ex:
                    conn2.execute(
                        "INSERT INTO self_rules (category,rule,confidence,source) VALUES (?,?,?,?)",
                        ("self_improve", rule[:200], 0.65, "self-diagnosis")
                    )
                    saved += 1
            except: pass

        # Приоритет
        priority = analysis.get("priority", "")
        if priority:
            save_knowledge("priority_action", priority[:300], "self-diagnosis")

        conn2.commit()
        conn2.close()
        logging.info(f"[SelfGrow] Самодиагностика: +{saved} правил. Приоритет: {priority[:60]}")

        # Уведомление если нашли новые бесплатные API
        free_apis = analysis.get("free_apis", [])
        if free_apis and ADMIN_ID:
            import asyncio as _a
            try:
                loop = _a.get_event_loop()
                msg = "🔬 <b>APEX нашёл новые источники данных:</b>\n" + "\n".join([f"• {a[:100]}" for a in free_apis[:3]])
                loop.call_soon_threadsafe(
                    loop.create_task,
                    bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
                )
            except: pass

    except Exception as e:
        logging.error(f"self_diagnose_and_grow: {e}")


def auto_fill_knowledge_gaps():
    """Закрывает пробелы в знаниях через Groq"""
    try:
        if not _LEARNING_OK:
            return
        gaps = _learn_get_gaps(limit=5)
        if not gaps:
            return
        for gap_id, query, context in gaps:
            answer = ask_groq(
                f"Ты торговый бот APEX. Ответь кратко: {query}. Контекст: {context}. Практический ответ для трейдинга.",
                max_tokens=150
            ) or ""
            if answer:
                _learn_resolve_gap(gap_id, answer)
                save_knowledge("gap_resolved", answer[:300], "auto-research")
    except Exception as e:
        logging.debug(f"auto_fill_knowledge_gaps: {e}")


async def autonomous_learning_cycle():
    """
    Главный цикл автономного обучения — запускается каждые 2 часа.
    Бот сам выбирает что изучить, ищет в интернете, сохраняет знания.
    """
    try:
        hour = datetime.now().hour
        if 6 <= hour < 12:
            topics = LEARN_TOPICS_MORNING
        elif 18 <= hour < 24:
            topics = LEARN_TOPICS_EVENING
        else:
            topics = LEARN_TOPICS_ALWAYS

        # Изучаем 2 случайные темы
        import random
        chosen = random.sample(topics, min(2, len(topics)))

        new_insights = []
        for topic in chosen:
            result = await asyncio.get_running_loop().run_in_executor(
                None, learn_from_web, topic, True
            )
            if isinstance(result, dict) and result.get("market_impact"):
                new_insights.append(f"📌 {topic}: {result['market_impact'][:100]}")
            await asyncio.sleep(3)  # пауза между запросами

        # Самоанализ — что бот узнал за последние 24 часа
        conn = sqlite3.connect("brain.db")
        recent_knowledge = conn.execute(
            "SELECT topic, content FROM knowledge WHERE created_at > datetime('now', '-24 hours') ORDER BY id DESC LIMIT 10"
        ).fetchall()
        rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        conn.close()

        if recent_knowledge:
            # AI строит сводку из всего что узнал
            knowledge_text = "\n".join([f"• {r[0]}: {r[1][:100]}" for r in recent_knowledge])
            synthesis = await asyncio.get_running_loop().run_in_executor(
                None, ask_groq,
                f"""Ты APEX. Синтезируй что ты узнал за последние 24 часа и сформулируй торговый вывод.

НОВЫЕ ЗНАНИЯ:
{knowledge_text}

Дай:
1. Общая картина рынка (1 предложение)
2. Лучшая возможность прямо сейчас (1 предложение)
3. Главный риск (1 предложение)""",
                300
            )
            if synthesis:
                save_knowledge("daily_synthesis", synthesis, "self-synthesis")

        logging.info(f"Автономное обучение: изучено {len(chosen)} тем, правил: {rule_count}")

        if new_insights and ADMIN_ID:
            await bot.send_message(
                ADMIN_ID,
                f"🧠 <b>APEX учится</b>\n\n" + "\n".join(new_insights),
                parse_mode="HTML"
            )

    except Exception as e:
        logging.error(f"autonomous_learning_cycle: {e}")


def build_market_worldview():
    """
    Строит текущее понимание рынка из всех накопленных знаний.
    Используется в ask_ai для умных ответов.
    """
    try:
        conn = sqlite3.connect("brain.db")
        # Последние знания за 48 часов
        recent = conn.execute(
            "SELECT topic, content FROM knowledge WHERE created_at > datetime('now', '-48 hours') ORDER BY id DESC LIMIT 15"
        ).fetchall()
        # Топ правила
        top_rules = conn.execute(
            "SELECT rule FROM self_rules WHERE confidence >= 0.6 ORDER BY confidence DESC LIMIT 5"
        ).fetchall()
        # Модели монет
        models = conn.execute(
            "SELECT symbol, trend, behavior_notes FROM market_model ORDER BY last_updated DESC LIMIT 5"
        ).fetchall()
        conn.close()

        parts = []
        if recent:
            facts = "\n".join([f"• {r[0]}: {r[1][:80]}" for r in recent[:8]])
            parts.append(f"ЧТО Я УЗНАЛ ЗА 48Ч:\n{facts}")
        if top_rules:
            rules = "\n".join([f"• {r[0][:80]}" for r in top_rules])
            parts.append(f"МОИ ПРАВИЛА:\n{rules}")
        if models:
            models_text = "\n".join([f"• {m[0]}: {m[1]} — {m[2][:60]}" for m in models])
            parts.append(f"МОДЕЛИ МОНЕТ:\n{models_text}")

        return "\n\n".join(parts)
    except:
        return ""


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

        # Бэкап brain.db в GitHub
        await backup_db_to_github()

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
    Binance Futures — основной (Bybit убран — 403 на Render).
    """
    import concurrent.futures
    result = {}

    def fetch_binance_futures():
        try:
            r = requests.get(
                f"{BINANCE_F}/fapi/v1/ticker/24hr",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            if r.status_code == 200:
                out = {}
                for t in r.json():
                    sym = t.get("symbol", "")
                    if sym.endswith("USDT") and t.get("lastPrice"):
                        out[sym] = {
                            "price": float(t["lastPrice"]),
                            "change": round(float(t.get("priceChangePercent", 0)), 2),
                            "source": "Binance"
                        }
                return out
        except:
            return {}

    def fetch_binance_spot():
        try:
            r = requests.get(
                f"{BINANCE}/api/v3/ticker/24hr",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            if r.status_code == 200:
                out = {}
                for t in r.json():
                    sym = t.get("symbol", "")
                    if sym.endswith("USDT") and t.get("lastPrice"):
                        out[sym] = {
                            "price": float(t["lastPrice"]),
                            "change": round(float(t.get("priceChangePercent", 0)), 2),
                            "source": "Binance Spot"
                        }
                return out
        except:
            return {}

    def fetch_cryptocompare():
        return get_cryptocompare_prices()

    def fetch_yahoo():
        return get_yahoo_finance_prices()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        f_bf = ex.submit(fetch_binance_futures)
        f_bs = ex.submit(fetch_binance_spot)
        f_cc = ex.submit(fetch_cryptocompare)
        f_yf = ex.submit(fetch_yahoo)

        bf_data = f_bf.result()
        bs_data = f_bs.result()
        cc_data = f_cc.result()
        yf_data = f_yf.result()

    # Binance Futures — приоритет (самый точный для фьючерсов)
    result.update(cc_data)
    result.update(yf_data)
    result.update(bs_data)
    result.update(bf_data)  # Binance Futures побеждает

    logging.info(f"Merged prices: {len(result)} монет (BF:{len(bf_data)} BS:{len(bs_data)} CC:{len(cc_data)} YF:{len(yf_data)})")
    return result


def get_multiple_prices_realtime():
    """
    Агрегатор цен со всех бирж.
    Источники: CoinGecko(600+ бирж) + CoinPaprika + CryptoCompare + Binance + Bybit
    Возвращает до 250 монет.
    """
    result = {}

    # ── 1. CoinGecko Markets — агрегирует 600+ бирж, топ-250 по объёму ──
    try:
        for page in [1, 2]:
            r = requests.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "volume_desc",
                    "per_page": 125,
                    "page": page,
                    "price_change_percentage": "24h",
                    "sparkline": "false"
                },
                headers={"User-Agent": "Mozilla/5.0 (compatible; APEX/1.0)"},
                timeout=12
            )
            if r.status_code == 200:
                for coin in r.json():
                    sym = coin.get("symbol", "").upper() + "USDT"
                    price = coin.get("current_price")
                    change = coin.get("price_change_percentage_24h") or 0
                    if price and price > 0:
                        result[sym] = {
                            "price": float(price),
                            "change": round(float(change), 2),
                            "volume": coin.get("total_volume") or 0,
                            "source": "CoinGecko"
                        }
            elif r.status_code == 429:
                break  # Rate limit — останавливаемся
        if len(result) >= 20:
            logging.info(f"Живые цены: CoinGecko ({len(result)} монет, 600+ бирж)")
    except Exception as e:
        logging.warning(f"get_prices CoinGecko: {e}")

    # ── 2. CoinPaprika — независимый агрегатор, топ-500 ──
    if len(result) < 50:
        try:
            r = requests.get(
                "https://api.coinpaprika.com/v1/tickers",
                params={"quotes": "USD", "limit": 200},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=12
            )
            if r.status_code == 200:
                for coin in r.json():
                    sym = coin.get("symbol", "").upper() + "USDT"
                    quotes = coin.get("quotes", {}).get("USD", {})
                    price = quotes.get("price")
                    change = quotes.get("percent_change_24h") or 0
                    if price and price > 0 and sym not in result:
                        result[sym] = {
                            "price": float(price),
                            "change": round(float(change), 2),
                            "volume": quotes.get("volume_24h") or 0,
                            "source": "CoinPaprika"
                        }
            logging.info(f"Живые цены: +CoinPaprika (итого {len(result)} монет)")
        except Exception as e:
            logging.warning(f"get_prices CoinPaprika: {e}")

    # ── 3. Binance Futures — прямо с биржи, все USDT пары ──
    try:
        r = requests.get(f"{BINANCE_F}/fapi/v1/ticker/24hr", timeout=8)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                data.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
                added = 0
                for t in data:
                    sym = t.get("symbol", "")
                    if sym.endswith("USDT"):
                        price = float(t["lastPrice"])
                        if price > 0:
                            if sym not in result:
                                result[sym] = {
                                    "price": price,
                                    "change": round(float(t["priceChangePercent"]), 2),
                                    "volume": float(t.get("quoteVolume", 0)),
                                    "source": "Binance"
                                }
                            added += 1
                    if added >= 200:
                        break
                logging.info(f"Живые цены: +Binance (итого {len(result)} монет)")
    except Exception as e:
        logging.warning(f"get_prices Binance: {e}")

    # ── 4. CryptoCompare — агрегирует Kraken/OKX/Coinbase/Huobi ──
    if len(result) < 30:
        try:
            r = requests.get(
                "https://min-api.cryptocompare.com/data/top/totalvolfull",
                params={"limit": 50, "tsym": "USD"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            if r.status_code == 200:
                data = r.json()
                for item in data.get("Data", []):
                    coin = item.get("CoinInfo", {})
                    raw = item.get("RAW", {}).get("USD", {})
                    sym = coin.get("Name", "").upper() + "USDT"
                    price = raw.get("PRICE")
                    change = raw.get("CHANGEPCT24HOUR") or 0
                    if price and price > 0 and sym not in result:
                        result[sym] = {
                            "price": float(price),
                            "change": round(float(change), 2),
                            "volume": raw.get("TOTALVOLUME24HTO") or 0,
                            "source": "CryptoCompare"
                        }
            logging.info(f"Живые цены: +CryptoCompare (итого {len(result)} монет)")
        except Exception as e:
            logging.warning(f"get_prices CryptoCompare: {e}")

    logging.info(f"Итого агрегировано: {len(result)} монет со всех бирж")
    return result


def get_all_market_pairs():
    """
    Возвращает список всех торговых пар для глубокого сканирования.
    Берёт из кэша get_multiple_prices_realtime.
    """
    prices = get_multiple_prices_realtime()
    # Сортируем по объёму
    sorted_pairs = sorted(
        prices.items(),
        key=lambda x: x[1].get("volume", 0),
        reverse=True
    )
    return [sym for sym, _ in sorted_pairs]



def ask_ai(user_id, user_name, user_message):
    mem = get_user_memory(user_id)
    history_rows = get_chat_history(user_id, limit=15)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    history_text = ""
    for row in history_rows:
        role_label = "Ты" if row[0] == "user" else "APEX"
        history_text += f"{role_label}: {row[1]}\n"

    msg_lower = user_message.lower()

    # ── Триггер глубокого скана всего рынка ──
    deep_scan_triggers = [
        "есть сделки", "какие сделки", "что торговать", "что покупать",
        "что брать", "что входить", "найди сделки", "найди сигналы",
        "есть сигналы", "какие сигналы", "сканируй рынок", "просканируй",
        "лучшие монеты", "что памп", "что иксанет", "что даст иксы",
        "какие монеты", "где входить", "есть ли сделки"
    ]
    if any(t in msg_lower for t in deep_scan_triggers):
        try:
            loop = asyncio.get_event_loop()
            signals, accumulations = loop.run_until_complete(deep_market_scan(limit=150))
            total = 150
            result_text = format_deep_scan_result(signals, accumulations, total)
            return result_text
        except Exception as e:
            logging.error(f"deep_market_scan in ask_ai: {e}")
            # Продолжаем как обычный запрос если скан упал

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

    # Живые цены — ВСЕГДА берём полный агрегатор (199+ монет)
    live_prices_text = ""
    prices = get_multiple_prices_realtime()
    if not prices:
        prices = get_live_prices()

    if prices:
        # Приоритетные монеты показываем первыми
        priority = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
                    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "TONUSDT", "ARBUSDT",
                    "NEARUSDT", "INJUSDT", "SUIUSDT", "APTUSDT", "OPUSDT",
                    "ADAUSDT", "DOTUSDT", "ATOMUSDT", "LTCUSDT", "XLMUSDT"]
        ordered = [(s, prices[s]) for s in priority if s in prices]
        others = [(s, d) for s, d in prices.items() if s not in priority]
        all_prices = ordered + others

        lines = []
        for sym, d in all_prices[:50]:  # Показываем 50 монет в промпте
            p = d["price"]
            ps = f"${p:,.2f}" if p >= 100 else f"${p:,.4f}" if p >= 1 else f"${p:.6f}"
            emoji = "🟢" if d["change"] >= 0 else "🔴"
            lines.append(f"{emoji} {sym.replace('USDT','')}: {ps} ({d['change']:+.2f}%)")
        total = len(prices)
        live_prices_text = (
            f"ЖИВЫЕ ЦЕНЫ — {total} монет (CoinGecko+CoinPaprika+CryptoCompare), {datetime.now().strftime('%H:%M')}:\n"
            + "\n".join(lines)
            + f"\n...и ещё {max(0, total-50)} монет в базе"
        )
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
    # ✅ НОВОЕ: Полная картина мира — всё что бот узнал за 48 часов
    worldview = build_market_worldview()

    user_context = ""
    if mem["name"] or mem["profile"]:
        user_context = f"ПОЛЬЗОВАТЕЛЬ:\nИмя: {mem['name'] or user_name} | Сообщений: {mem['messages']}\nПрофиль: {mem['profile'] or 'нет'}\nМонеты: {mem['coins'] or 'нет'}\nДепозит: ${mem['deposit']} | Риск: {mem['risk']}%"

    # Определяем тип вопроса для точного routing
    q = user_message.lower()
    is_list_q  = any(w in q for w in ["список", "какие монеты", "по каким", "мониторинг", "отслеживаешь", "какие пары", "видишь монеты", "доступ", "какие данные"])
    is_deal_q  = any(w in q for w in ["сделк", "сигнал", "вход", "выход", "лонг", "шорт", "купить", "продать", "tp", "стоп"])
    is_price_q = any(w in q for w in ["цена", "курс", "сколько стоит", "почём", "стоимость"])

    # ── Триггер анализа конкретной монеты ──
    # "разбор btc", "анализ ton", "посмотри на sol", "что по eth"
    analysis_triggers = [
        "разбор", "анализ", "посмотри", "проверь", "что по", "что с",
        "дай разбор", "дай анализ", "сигнал по", "вход по", "смотри",
        "analyse", "analyze", "check", "scan"
    ]
    found_symbol = None
    if any(t in q for t in analysis_triggers) or is_deal_q:
        # Ищем упоминание монеты в сообщении
        for alias, sym in SYMBOL_ALIASES.items():
            if alias in q:
                found_symbol = sym
                break
        # Если нашли монету — запускаем full_scan вместо болтовни
        if found_symbol:
            try:
                scan_result = full_scan(found_symbol)
                if scan_result:
                    return scan_result
                else:
                    # Нет сигнала — объясняем почему
                    price_data = prices.get(found_symbol)
                    price_str = f"${price_data['price']:,.4f}" if price_data else "нет данных"
                    # Auto-Discovery: логируем что анализ не получился
                    if _LEARNING_OK:
                        _learn_gap(f"no_signal:{found_symbol}", "full_scan returned None")
                    return (
                        f"📊 <b>{found_symbol}</b> | {price_str}\n\n"
                        f"😴 Чёткого SMC сетапа нет прямо сейчас.\n"
                        f"Таймфреймы конфликтуют или рынок в боковике.\n\n"
                        f"<i>Попробуй через 15-30 мин — рынок меняется.</i>"
                    )
            except Exception as e:
                logging.error(f"ask_ai full_scan {found_symbol}: {e}")

    # Считаем реальное количество монет в ценах
    prices_count = len(prices) if prices else 0

    prompt = f"""Ты APEX — торговый бот. Отвечаешь ТОЛЬКО по делу. Дата: {now}

ДАННЫЕ:
- Цен в базе: {prices_count} монет
- SMC анализ по любой монете доступен через кнопки меню

{user_context}

{live_prices_text}

{f"РЕСЁРЧ:{chr(10)}{research_result}" if research_result else ""}
{f"НОВОСТИ:{chr(10)}{recent_news[:300]}" if recent_news and not research_result else ""}
{f"ЗНАНИЯ:{chr(10)}{knowledge[:200]}" if knowledge else ""}
{f"ФУНДАМЕНТАЛ:{chr(10)}{messari_context}" if messari_context else ""}
{f"ОПЫТ:{chr(10)}{worldview[:400]}" if worldview else f"ОПЫТ:{chr(10)}{brain_context[:300]}" if brain_context else ""}

ИСТОРИЯ (последние):
{history_text[-800:] if history_text else "—"}

ПРАВИЛА — СТРОГО:
1. Отвечай ТОЛЬКО на заданный вопрос — 2-4 предложения максимум
2. Если спросили цену — дай цену из ЖИВЫЕ ЦЕНЫ, ничего лишнего
3. Если спросили список монет — дай список из ЖИВЫЕ ЦЕНЫ
4. НЕ давай сигналы если не просили
5. НЕ спрашивай "что ты хочешь" — отвечай на то что спросили
6. НЕ начинай с "Привет", "Конечно", "Отличный" — сразу ответ
7. НЕ придумывай цены — только из блока ЖИВЫЕ ЦЕНЫ
8. Стиль: короткий, конкретный, как опытный трейдер другу

{user_name}: {user_message}
APEX:"""

    return ask_groq(prompt, max_tokens=600)


# ===== KEYBOARDS =====

def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Найти сделки", callback_data="menu_find_deals"),
         InlineKeyboardButton(text="📊 Рынок сейчас", callback_data="menu_market")],
        [InlineKeyboardButton(text="🔍 Сканировать рынок", callback_data="menu_scan"),
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
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_backtest")]
    ])

def live_tf_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="15м — где мы?", callback_data="live_15m"),
         InlineKeyboardButton(text="1ч — где мы?",  callback_data="live_1h"),
         InlineKeyboardButton(text="4ч — где мы?",  callback_data="live_4h")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_backtest")]
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
    crypto_news = await asyncio.get_running_loop().run_in_executor(None, get_crypto_news)
    macro_news = await asyncio.get_running_loop().run_in_executor(None, get_market_impact_news)
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
        pairs = get_top_pairs(50)
        await callback.message.edit_text(
            f"🔍 Сканирую топ-50 на {TF_LABELS.get(tf, tf)}...\n⏳ ~20 сек"
        )
        signals = []
        for symbol in pairs:
            try:
                sig = await asyncio.get_running_loop().run_in_executor(
                    None, full_scan_raw, symbol, tf
                )
                if sig:
                    signals.append(sig)
                await asyncio.sleep(0.1)
            except:
                pass

        if not signals:
            text = f"😴 На {TF_LABELS.get(tf, tf)} чётких сетапов нет.\nПопробуй другой таймфрейм."
            await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_tf")]
            ]))
            return

        # Сортируем: МЕГА ТОП первые
        grade_order = {"МЕГА ТОП": 0, "ТОП СДЕЛКА": 1, "ХОРОШАЯ": 2}
        signals.sort(key=lambda x: grade_order.get(x.get("grade", ""), 3))

        # Отправляем первый сигнал в текущее сообщение
        top = signals[0]
        direction = top.get("direction", "")
        emoji = "🟢" if direction == "BULLISH" else "🔴"

        # Показываем краткую сводку всех + полный топ сигнал
        summary_lines = []
        for s in signals[:8]:
            d = s.get("direction", "")
            ic = "🟢" if d == "BULLISH" else "🔴"
            grade_short = s.get("grade", "")
            fire = "🔥🔥🔥" if grade_short == "МЕГА ТОП" else "🔥🔥" if grade_short == "ТОП СДЕЛКА" else "✅"
            summary_lines.append(f"{fire} {ic} {s['symbol'].replace('USDT','')} — {d}")

        summary = "\n".join(summary_lines)
        header = (
            f"⏱ <b>Скан {TF_LABELS.get(tf, tf)}</b> | найдено: {len(signals)}\n"
            f"{'━'*22}\n\n"
            f"{summary}\n\n"
            f"{'━'*22}\n"
            f"<b>Лучший сигнал:</b>\n\n"
            + top["text"]
        )

        if len(header) > 4000:
            header = header[:3990] + "..."

        await callback.message.edit_text(
            header,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data=data)],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_tf")]
            ])
        )

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
            diag = await asyncio.get_running_loop().run_in_executor(None, scan_diagnostics, symbol)
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
            "🔬 <b>Анализ монеты</b>\n\n"
            "• <b>Бектест</b> — историческая точность стратегии\n"
            "• <b>Где мы сейчас</b> — живой анализ: OB, FVG, уровни, что делать",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔬 Бектест (история)", callback_data="menu_bt_select")],
                [InlineKeyboardButton(text="📍 Где мы сейчас (live)", callback_data="menu_live_select")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")],
            ])
        )

    elif data == "menu_bt_select":
        await callback.message.edit_text(
            "🔬 <b>Бектест</b>\n\nВыбери таймфрейм:",
            parse_mode="HTML", reply_markup=backtest_tf_keyboard()
        )

    elif data == "menu_live_select":
        await callback.message.edit_text(
            "📍 <b>Живой анализ — где мы сейчас?</b>\n\nВыбери таймфрейм:",
            parse_mode="HTML", reply_markup=live_tf_keyboard()
        )

    elif data.startswith("live_"):
        parts = data.split("_")
        # live_15m / live_1h / live_4h — выбор таймфрейма, дальше просят монету
        if len(parts) == 2:
            tf = parts[1]
            user_states[user_id] = {"action": "live_analysis", "tf": tf}
            await callback.message.edit_text(
                f"📍 Анализ на {TF_LABELS.get(tf, tf)} — напиши монету (BTC, SOL, ETHUSDT...):"
            )
        # live_now_BTCUSDT_1h или live_refresh_BTCUSDT_1h — прямой показ
        elif len(parts) >= 4:
            symbol = parts[2]; tf = parts[3]
            await callback.message.edit_text(f"📍 Обновляю {symbol} {TF_LABELS.get(tf,tf)}...")
            result = await asyncio.get_running_loop().run_in_executor(None, live_position_analysis, symbol, tf)
            if result:
                await callback.message.edit_text(result, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"live_refresh_{symbol}_{tf}")],
                        [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
                    ]))
            else:
                await callback.message.edit_text(f"Нет данных по {symbol}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_backtest")]]))

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
            # Данные из основной БД
            conn = sqlite3.connect("brain.db")
            rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
            top_rules = conn.execute(
                "SELECT category, rule, confidence FROM self_rules ORDER BY confidence DESC LIMIT 5"
            ).fetchall()
            obs_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            model_count = conn.execute("SELECT COUNT(*) FROM market_model").fetchone()[0]
            avoid_count = conn.execute("SELECT COUNT(*) FROM self_rules WHERE category='avoid'").fetchone()[0]
            conn.close()

            # Данные из brain_builder
            brain = get_brain_summary() if BRAIN_BUILDER_AVAILABLE else {}
            knowledge_count = brain.get("knowledge_count", 0)
            pattern_count = brain.get("pattern_count", 0)
            coin_count = brain.get("coin_count", 0)
            macro_summary = brain.get("macro_summary", "")[:200]
            macro_time = brain.get("macro_time", "")
            # Объединяем правила из обеих БД
            bb_rules = brain.get("top_rules", "")
        except Exception as e:
            rule_count = obs_count = model_count = avoid_count = 0
            knowledge_count = pattern_count = coin_count = 0
            top_rules = []
            macro_summary = bb_rules = macro_time = ""

        cat_emoji = {
            "entry": "🎯", "exit": "🏁", "filter": "🔍",
            "timing": "⏱", "risk": "💰", "avoid": "⛔️",
            "best_setup": "🌟", "market": "📊"
        }
        rules_text = "\n".join([
            f"{cat_emoji.get(r[0], '📌')} {r[1][:65]} — {r[2]:.0%}"
            for r in top_rules
        ]) or bb_rules or "Пока нет правил"

        macro_block = (
            f"\n📊 <b>Последний макро анализ</b> ({macro_time}):\n"
            f"<i>{macro_summary}</i>\n"
        ) if macro_summary else ""

        await callback.message.edit_text(
            f"🧠 <b>Мозг APEX</b>\n"
            f"{'━'*24}\n\n"
            f"📌 Торговых правил: <b>{rule_count}</b>\n"
            f"⛔️ Антипаттернов: <b>{avoid_count}</b>\n"
            f"👁 Наблюдений: <b>{obs_count}</b>\n"
            f"🗂 Моделей монет: <b>{model_count}</b>\n"
            f"📚 Знаний (Groq): <b>{knowledge_count}</b>\n"
            f"📈 SMC паттернов: <b>{pattern_count}</b>\n"
            f"🪙 Правил по монетам: <b>{coin_count}</b>\n\n"
            f"<b>Топ правила:</b>\n{rules_text}\n"
            f"{macro_block}\n"
            f"<i>Groq обучает мозг каждый час: SMC, макро, новости, история сделок</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚡️ Обучить сейчас", callback_data="brain_learn_now"),
                 InlineKeyboardButton(text="🌍 Макро анализ", callback_data="brain_macro")],
                [InlineKeyboardButton(text="📡 Источники данных", callback_data="brain_sources"),
                 InlineKeyboardButton(text="📊 Самоанализ", callback_data="brain_self_analysis")],
                [InlineKeyboardButton(text="🏅 Точность грейдов", callback_data="brain_grade_accuracy")],
                [InlineKeyboardButton(text="🔑 Статус API", callback_data="brain_api_status"),
                 InlineKeyboardButton(text="🔬 Самодиагностика", callback_data="brain_self_diagnose")],
                [InlineKeyboardButton(text="📋 Анализ сделок", callback_data="brain_trade_analysis"),
                 InlineKeyboardButton(text="📈 Стратегия", callback_data="brain_strategy")],
                [InlineKeyboardButton(text="🔍 Диагноз ошибок", callback_data="brain_diagnosis")],
                [InlineKeyboardButton(text="📚 История обучения", callback_data="menu_evolution")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )

    elif data == "brain_sources":
        await callback.message.edit_text("📡 Загружаю статистику источников...")
        try:
            if _SMC_ENGINE_OK:
                stats_text = get_source_stats()
                barrier_text = get_barrier_summary()
                full_text = stats_text + (f"\n\n{barrier_text}" if barrier_text else "")
            else:
                full_text = (
                    "📡 <b>Статус источников данных</b>\n\n"
                    "<b>Свечи (приоритет):</b>\n"
                    "1️⃣ CryptoCompare — ✅ основной\n"
                    "2️⃣ Binance Futures REST — ✅ fallback\n"
                    "3️⃣ Binance Spot REST — ✅ fallback\n"
                    "4️⃣ Binance API (авторизованный) — ✅\n"
                    "5️⃣ TwelveData — ✅ (если есть ключ)\n"
                    "6️⃣ CoinGecko — ✅ последний резерв\n\n"
                    "<b>Цены:</b> Binance Futures + Spot + CryptoCompare + Yahoo Finance\n"
                    "<b>Пары:</b> Binance Futures топ-100 → Spot fallback\n"
                    "<b>Новости:</b> CoinTelegraph, CoinDesk, Decrypt, Reuters RSS\n\n"
                    "<i>ℹ️ smc_engine.py не в корне — используется встроенный SMC движок бота.</i>"
                )
        except Exception as e:
            full_text = f"Ошибка: {e}"
        await callback.message.edit_text(
            full_text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_self_analysis":
        # Последний самоанализ точности
        await callback.message.edit_text("📊 Загружаю самоанализ...")
        try:
            if _LEARNING_OK:
                analysis_text = _learn_self_analysis_text()
                stats_text = _learn_all_stats()
                full = analysis_text
                if stats_text:
                    full += "\n\n" + stats_text
            else:
                # learning.py нет — показываем статистику из brain.db напрямую
                try:
                    conn = sqlite3.connect("brain.db")
                    total_sig = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
                    wins = conn.execute("SELECT COUNT(*) FROM signals WHERE result='win'").fetchone()[0]
                    losses = conn.execute("SELECT COUNT(*) FROM signals WHERE result='loss'").fetchone()[0]
                    pending = conn.execute("SELECT COUNT(*) FROM signals WHERE result='pending'").fetchone()[0]
                    conn.close()
                    wr = round(wins/(wins+losses)*100) if (wins+losses) > 0 else 0
                    full = (
                        f"📊 <b>Статистика сигналов</b>\n\n"
                        f"Всего сигналов: {total_sig}\n"
                        f"✅ Победы: {wins} | ❌ Потери: {losses} | ⏳ Открыты: {pending}\n"
                        f"Win Rate: {wr}%\n\n"
                        f"<i>Для расширенного самоанализа добавь learning.py в корень репо.</i>"
                    )
                except Exception as db_e:
                    full = f"Нет данных: {db_e}"
        except Exception as e:
            full = f"Ошибка: {e}"
        await callback.message.edit_text(
            full or "Данных пока нет", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Запустить анализ", callback_data="brain_run_analysis")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_grade_accuracy":
        try:
            grade_text = _learn_grade_text() if _LEARNING_OK else "learning.py не загружен"
        except Exception as e:
            grade_text = f"Ошибка: {e}"
        await callback.message.edit_text(
            grade_text or "Данных пока нет — нужно больше закрытых сигналов",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_run_analysis":
        await callback.message.edit_text("🧠 Запускаю самоанализ...")
        try:
            if _LEARNING_OK:
                import asyncio as _a
                await _a.get_event_loop().run_in_executor(None, _learn_self_analysis)
                text = _learn_self_analysis_text()
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Нет данных", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="brain_self_analysis")]
            ])
        )

    elif data == "brain_api_status":
        await callback.message.edit_text(
            get_api_status_text(), parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_self_diagnose":
        await callback.message.edit_text("🔬 Запускаю самодиагностику...")
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self_diagnose_and_grow)
            await loop.run_in_executor(None, auto_fill_knowledge_gaps)
            conn = sqlite3.connect("brain.db")
            priority_row = conn.execute(
                "SELECT content FROM knowledge WHERE topic='priority_action' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            new_rules = conn.execute(
                "SELECT COUNT(*) FROM self_rules WHERE category='self_improve'"
            ).fetchone()[0]
            suggested = conn.execute(
                "SELECT COUNT(*) FROM knowledge WHERE topic='suggested_api'"
            ).fetchone()[0]
            conn.close()
            priority_txt = priority_row[0][:200] if priority_row else "нет"
            result_text = (
                "<b>Самодиагностика завершена</b>\n\n"
                f"Правил самоулучшения: {new_rules}\n"
                f"Предложено новых API: {suggested}\n\n"
                f"<b>Приоритет:</b> <i>{priority_txt}</i>"
            )
        except Exception as e:
            result_text = f"Ошибка: {e}"
        await callback.message.edit_text(
            result_text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔑 Статус API", callback_data="brain_api_status")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_trade_analysis":
        await callback.message.edit_text("📋 Загружаю анализ сделок от Groq...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                text = await loop.run_in_executor(None, _learn_trade_analysis, 7)
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Анализов пока нет — нужно закрыть несколько сделок",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_strategy":
        await callback.message.edit_text("📈 Загружаю стратегию...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                # Сначала показываем текущую, потом обновляем
                text = await loop.run_in_executor(None, _learn_get_strategy)
                if "не сформирована" in text:
                    text = await loop.run_in_executor(None, _learn_build_strategy)
                    if not text:
                        text = "Недостаточно данных (нужно минимум 10 закрытых сделок)"
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Стратегия пока не сформирована",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить стратегию", callback_data="brain_strategy_refresh")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_strategy_refresh":
        await callback.message.edit_text("⏳ Groq анализирует паттерны и формулирует стратегию...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                text = await loop.run_in_executor(None, _learn_build_strategy)
                if not text:
                    text = "Недостаточно данных (нужно минимум 10 закрытых сделок)"
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Не удалось сформировать стратегию",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_diagnosis":
        await callback.message.edit_text("🔍 Groq анализирует ошибки и паттерны потерь...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                # Пробуем получить последний или запустить новый
                text = await loop.run_in_executor(None, _learn_latest_diag)
                if "не запускалась" in text:
                    text = await loop.run_in_executor(None, _learn_self_diag)
                    if not text:
                        text = "Недостаточно потерь для анализа (нужно минимум 3)"
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Диагноз недоступен",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Запустить диагноз", callback_data="brain_diagnosis_run")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_diagnosis_run":
        await callback.message.edit_text("⏳ Groq проводит глубокий самоанализ ошибок...")
        try:
            if _LEARNING_OK:
                loop = asyncio.get_running_loop()
                text = await loop.run_in_executor(None, _learn_self_diag)
                if not text:
                    text = "Недостаточно потерь для анализа (нужно минимум 3 sl)"
            else:
                text = "learning.py не загружен"
        except Exception as e:
            text = f"Ошибка: {e}"
        await callback.message.edit_text(
            text or "Не удалось запустить диагноз",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
            ])
        )

    elif data == "brain_macro":
        await callback.message.edit_text("🌍 Запрашиваю макро анализ...")
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: __import__('brain_builder').learn_macro_trends())
            brain = get_brain_summary()
            macro = brain.get("macro_summary", "Нет данных")[:500]
            macro_time = brain.get("macro_time", "")
            await callback.message.edit_text(
                f"🌍 <b>Макро анализ</b> ({macro_time})\n{'━'*24}\n\n{macro}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
                ])
            )
        except Exception as e:
            await callback.message.edit_text(
                f"❌ Ошибка макро анализа: {e}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_brain")]
                ])
            )

    elif data == "brain_learn_now":
        await callback.message.edit_text(
            "🧠 Запускаю полное обучение...\n"
            "⏳ Groq анализирует: макро + новости + SMC + история сделок\n"
            "Займёт ~30 секунд"
        )
        await run_brain_builder_async()
        await autonomous_learning_cycle()
        brain = get_brain_summary() if BRAIN_BUILDER_AVAILABLE else {}
        conn = sqlite3.connect("brain.db")
        rule_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        conn.close()
        await callback.message.edit_text(
            f"✅ <b>Обучение завершено</b>\n\n"
            f"📌 Торговых правил: <b>{rule_count}</b>\n"
            f"📚 Знаний Groq: <b>{brain.get('knowledge_count', 0)}</b>\n"
            f"🪙 Правил по монетам: <b>{brain.get('coin_count', 0)}</b>\n"
            f"📈 SMC паттернов: <b>{brain.get('pattern_count', 0)}</b>\n\n"
            f"<i>База сохранена в GitHub — знания не пропадут при рестарте</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🧠 Открыть мозг", callback_data="menu_brain")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
            ])
        )


    elif data == "menu_evolution":
        try:
            conn = sqlite3.connect("brain.db")
            history      = conn.execute("SELECT title, description, after_value, impact_score, created_at FROM learning_history ORDER BY id DESC LIMIT 20").fetchall()
            total_events = conn.execute("SELECT COUNT(*) FROM learning_history").fetchone()[0]
            rules_total  = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
            rules_strong = conn.execute("SELECT COUNT(*) FROM self_rules WHERE confidence >= 0.7").fetchone()[0]
            errors_fixed = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=1").fetchone()[0]
            knowledge_cnt= conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]
            conn.close()
        except:
            history = []; total_events = rules_total = rules_strong = errors_fixed = knowledge_cnt = 0

        if not history:
            evo_text = ("📚 <b>Эволюция APEX</b>\n" + "━"*26 + "\n\n🆕 История пуста.\n\n<i>Нажми «Запустить обучение» чтобы бот начал читать интернет и накапливать знания</i>")
        else:
            lines_evo = [
                "📚 <b>Эволюция APEX</b>", "━"*26,
                f"📊 Событий: <b>{total_events}</b>  |  📌 Правил: <b>{rules_total}</b>  |  💪 Сильных: <b>{rules_strong}</b>",
                f"🔧 Исправлено ошибок: <b>{errors_fixed}</b>  |  📖 Знаний: <b>{knowledge_cnt}</b>",
                "━"*26, "<b>Хронология:</b>", "",
            ]
            for title, desc, after, score, created in history[:15]:
                ts  = (created or "")[:16]
                bar = "█" * int((score or 0.5) * 5)
                lines_evo += [
                    f"<b>{title}</b>  <code>{ts}</code>",
                    f"  {(desc or '')[:90]}",
                    *([ f"  → {after[:60]}" ] if after else []),
                    f"  {bar} {int((score or 0.5)*100)}%",
                    "",
                ]
            evo_text = "\n".join(lines_evo)

        if len(evo_text) > 4000:
            evo_text = evo_text[:4000] + "\n\n<i>...показаны последние события</i>"

        await callback.message.edit_text(evo_text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_evolution")],
                [InlineKeyboardButton(text="🧠 Запустить обучение", callback_data="brain_learn_now")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")],
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
        crypto_news = await asyncio.get_running_loop().run_in_executor(None, get_crypto_news)
        # Макро новости
        macro_news = await asyncio.get_running_loop().run_in_executor(None, get_market_impact_news)

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

    elif data in ("menu_find_deals", "menu_find_deals_refresh"):
        await callback.message.edit_text(
            "🎯 <b>Ищу сделки...</b>\n\n"
            "⏳ Сканирую топ-40 пар по SMC: OB, FVG, мультитаймфрейм\n"
            "<i>~20-30 секунд</i>",
            parse_mode="HTML"
        )
        signals = await asyncio.get_running_loop().run_in_executor(None, scan_all_for_deals, 40)

        if not signals:
            await callback.message.edit_text(
                "🎯 <b>Сделок нет</b>\n\n"
                "😴 Прошёлся по топ-40 монетам — чётких сетапов не нашёл.\n"
                "Рынок в боковике или сигналы ещё не сформировались.\n\n"
                "<i>Обычно сигналы появляются после пробоя уровней или выхода новостей</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="menu_find_deals_refresh")],
                    [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
                ])
            )
            return

        grade_icons = {"🔥🔥🔥 МЕГА ТОП": "🔥🔥🔥", "🔥🔥 ТОП СДЕЛКА": "🔥🔥", "✅ ХОРОШАЯ": "✅"}
        lines = [f"🎯 <b>Найдено сделок: {len(signals)}</b>", "━"*24, ""]
        for s in signals:
            emoji = "🟢" if s["direction"] == "BULLISH" else "🔴"
            icon  = grade_icons.get(s["grade"], "✅")
            lines.append(f"{icon} {emoji} <b>{s['symbol'].replace('USDT','')}</b> — {s['direction']}")
        lines += ["", "<i>Выбери монету для полного сигнала 👇</i>"]

        buttons = []
        row = []
        for s in signals[:12]:
            emoji = "🟢" if s["direction"] == "BULLISH" else "🔴"
            fire  = "🔥" if "ТОП" in s["grade"] else ""
            row.append(InlineKeyboardButton(
                text=f"{fire}{emoji} {s['symbol'].replace('USDT','')}",
                callback_data=f"deal_open_{s['symbol']}"
            ))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([
            InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_find_deals_refresh"),
            InlineKeyboardButton(text="🔙 Меню",     callback_data="menu_back"),
        ])
        await callback.message.edit_text(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )

    elif data.startswith("deal_open_"):
        symbol = data.replace("deal_open_", "")
        await callback.message.edit_text(f"📊 Загружаю сигнал по <b>{symbol}</b>...", parse_mode="HTML")
        result = await asyncio.get_running_loop().run_in_executor(None, full_scan_raw, symbol, "1h")
        if result:
            mem = get_user_memory(user_id)
            risk_block = ""
            if mem["deposit"] > 0:
                try:
                    rc = calc_risk(mem["deposit"], mem["risk"], result.get("entry", 0), result.get("sl", 0))
                    if rc:
                        risk_block = (
                            f"\n💰 <b>Риск-менеджмент:</b>\n"
                            f"Размер позиции: <b>{rc['position_size']:.2f}</b> USDT\n"
                            f"Риск в $: <b>${rc['risk_amount']:.2f}</b> ({mem['risk']}%)\n"
                        )
                except:
                    pass
            await callback.message.edit_text(
                result["text"] + risk_block,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить сигнал", callback_data=data)],
                    [InlineKeyboardButton(text="🔙 К списку сделок", callback_data="menu_find_deals")],
                ])
            )
        else:
            await callback.message.edit_text(
                f"😴 <b>{symbol}</b> — сигнал пропал.\n<i>Рынок изменился пока ты смотрел список</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 К списку сделок", callback_data="menu_find_deals")],
                ])
            )

    elif data.startswith("menu_trade_") or data.startswith("trade_scalp_") or data.startswith("trade_swing_") or data.startswith("trade_long_"):
        # Старые хендлеры — редиректим на новый
        await callback.message.edit_text("🔄", parse_mode="HTML")
        await asyncio.sleep(0.1)
        # Имитируем нажатие menu_find_deals
        signals = await asyncio.get_running_loop().run_in_executor(None, scan_all_for_deals, 40)
        await callback.message.edit_text(
            f"🎯 Найдено сделок: {len(signals)}" if signals else "😴 Сигналов нет",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎯 Найти сделки", callback_data="menu_find_deals")],
                [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
            ])
        )

    elif data == "menu_pump":
        await callback.message.edit_text("📦 Сканирую топ-50 на накопление перед пампом...\n⏳ ~30 секунд")
        pairs = await asyncio.get_running_loop().run_in_executor(None, get_top_pairs, 50)
        found = []
        for symbol in pairs:
            try:
                acc = await asyncio.get_running_loop().run_in_executor(None, detect_accumulation, symbol)
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
        acc = await asyncio.get_running_loop().run_in_executor(None, detect_accumulation, symbol)
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


@dp.message(Command("trade"))
async def cmd_trade(message: types.Message):
    """
    /trade BTC          — все типы сделок (скальп + свинг + долгосрок)
    /trade BTC scalp    — только скальп
    /trade BTC swing    — только свинг
    /trade BTC long     — только долгосрок
    """
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "📊 <b>Анализ по типу сделки</b>\n\n"
            "Использование:\n"
            "/trade BTC — все типы\n"
            "/trade BTC scalp — скальп (1m/5m/15m)\n"
            "/trade BTC swing — свинг (1h/4h)\n"
            "/trade BTC long — долгосрок (1d/1w/1M)\n\n"
            "<i>Примеры: /trade TON, /trade ETH swing, /trade SOL long</i>",
            parse_mode="HTML"
        )
        return

    # Распознаём символ через алиасы
    raw = args[1].lower()
    symbol = SYMBOL_ALIASES.get(raw, raw.upper())
    if not symbol.endswith("USDT"):
        symbol = symbol.upper() + "USDT"

    trade_type = args[2].lower() if len(args) >= 3 else "all"
    valid_types = {"scalp", "swing", "long", "all"}
    if trade_type not in valid_types:
        trade_type = "all"

    types_to_run = ["scalp", "swing", "long"] if trade_type == "all" else [trade_type]

    type_labels = {"scalp": "⚡️ Скальп", "swing": "🔄 Свинг", "long": "📈 Долгосрок"}
    await message.answer(
        f"🔍 Анализирую <b>{symbol}</b>\n"
        f"Типы: {' | '.join([type_labels[t] for t in types_to_run])}\n"
        f"⏳ Подожди...",
        parse_mode="HTML"
    )

    found_any = False
    for tt in types_to_run:
        result = await asyncio.get_running_loop().run_in_executor(
            None, analyze_trade_type, symbol, tt
        )
        if result:
            found_any = True
            await message.answer(result["text"], parse_mode="HTML")
            await asyncio.sleep(0.5)
        else:
            await message.answer(
                f"{type_labels[tt]}: нет чёткого сигнала по {symbol} на таймфреймах {', '.join(TF_CATEGORIES[tt])}"
            )

    if not found_any:
        await message.answer(
            f"😴 <b>{symbol}</b> — нет сигналов ни по одному типу сделки.\n"
            f"Рынок, возможно, в боковике или данных недостаточно.",
            parse_mode="HTML"
        )

@dp.message(Command("think"))
async def cmd_think(message: types.Message):
    """Бот думает вслух — глубокий ресёрч по теме"""
    args = message.text.split(maxsplit=1)
    topic = args[1].strip() if len(args) > 1 else "bitcoin market analysis"
    await message.answer(f"🧠 Думаю над темой: <b>{topic}</b>...\n⏳ Ищу в интернете, анализирую...", parse_mode="HTML")
    result = await asyncio.get_running_loop().run_in_executor(None, deep_research, topic)
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

    # Проверяем состояние (ожидаем ввод монеты)
    if user_id in user_states:
        state = user_states.pop(user_id)

        if state.get("action") == "live_analysis":
            symbol = text.upper().replace("USDT", "") + "USDT"
            tf = state.get("tf", "1h")
            thinking = await message.answer(f"📍 Анализирую {symbol} {TF_LABELS.get(tf, tf)}...")
            result = await asyncio.get_running_loop().run_in_executor(None, live_position_analysis, symbol, tf)
            try: await thinking.delete()
            except: pass
            if result:
                await message.answer(result, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"live_refresh_{symbol}_{tf}")],
                        [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
                    ]))
            else:
                await message.answer(f"Нет данных по {symbol}. Попробуй: BTC, ETH, SOL, BNB")
            return

        if state.get("action") == "backtest":
            symbol = text.upper().replace("USDT", "") + "USDT"
            tf = state.get("tf", "1h")
            thinking = await message.answer(f"🔬 Запускаю бектест {symbol} {TF_LABELS.get(tf, tf)}...")
            result = await asyncio.get_running_loop().run_in_executor(None, backtest, symbol, tf)
            try: await thinking.delete()
            except: pass
            if result:
                grade = "🔥 Отличная" if result["win_rate"] >= 60 else "✅ Рабочая" if result["win_rate"] >= 50 else "⚠️ Слабая"
                await message.answer(
                    f"🔬 <b>Бектест {symbol} [{TF_LABELS.get(tf, tf)}]</b>\n\n"
                    f"Сигналов: {result['total']}\n"
                    f"✅ Выигрыши: {result['wins']}\n"
                    f"❌ Проигрыши: {result['losses']}\n"
                    f"🎯 Win Rate: <b>{result['win_rate']}%</b>\n"
                    f"Оценка: {grade}",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📍 Где мы сейчас?", callback_data=f"live_now_{symbol}_{tf}")],
                        [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
                    ])
                )
            else:
                await message.answer("Недостаточно данных для бектеста")
            return

    save_chat_log(user_id, "user", text)
    thinking = await message.answer("⚡️")
    # ✅ ФИКС: run_in_executor — ask_ai не блокирует event loop
    reply = await asyncio.get_running_loop().run_in_executor(None, ask_ai, user_id, user_name, text)
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

async def deep_market_scan(limit=200):
    """
    Глубокий скан всего рынка по запросу пользователя.
    Проверяет 200 монет со всех бирж, ищет сигналы + накопления.
    Возвращает отсортированный список сигналов.
    """
    all_pairs = get_all_market_pairs()
    scan_pairs = all_pairs[:limit]

    signals = []
    accumulations = []

    # Сканируем батчами по 10 монет параллельно
    async def scan_one(symbol):
        try:
            loop = asyncio.get_running_loop()
            # Таймаут 8 сек на монету — не зависаем
            sig = await asyncio.wait_for(
                loop.run_in_executor(None, full_scan_raw, symbol, "1h"),
                timeout=8.0
            )
            if sig and sig.get("grade") in ("МЕГА ТОП", "ТОП СДЕЛКА", "ХОРОШАЯ",
                                             "🔥🔥🔥 МЕГА ТОП", "🔥🔥 ТОП СДЕЛКА", "✅ ХОРОШАЯ"):
                signals.append(sig)
            # Накопление — отдельный таймаут
            acc = await asyncio.wait_for(
                loop.run_in_executor(None, detect_accumulation, symbol),
                timeout=6.0
            )
            if acc and acc.get("score", 0) >= 55:  # снижен порог с 60 до 55
                accumulations.append({
                    "symbol": symbol,
                    "score": acc["score"],
                    "signal": acc.get("signal", ""),
                    "price": acc.get("price", 0)
                })
        except asyncio.TimeoutError:
            logging.debug(f"deep_scan timeout: {symbol}")
        except Exception as e:
            logging.debug(f"deep_scan error {symbol}: {e}")

    # Батчи по 10
    for i in range(0, min(len(scan_pairs), limit), 10):
        batch = scan_pairs[i:i+10]
        await asyncio.gather(*[scan_one(sym) for sym in batch])
        await asyncio.sleep(0.5)  # Не перегружаем API

    # Сортируем по приоритету
    grade_order = {"🔥🔥🔥 МЕГА ТОП": 0, "🔥🔥 ТОП СДЕЛКА": 1, "✅ ХОРОШАЯ": 2}
    signals.sort(key=lambda x: grade_order.get(x.get("grade", ""), 3))
    accumulations.sort(key=lambda x: x["score"], reverse=True)

    return signals, accumulations


def format_deep_scan_result(signals, accumulations, total_scanned):
    """Форматирует результат глубокого скана для Telegram"""
    if not signals and not accumulations:
        return (
            f"🔍 <b>Глубокий скан завершён</b>\n"
            f"Проверено монет: {total_scanned}\n\n"
            f"😴 Рынок спокоен — нет чётких сетапов\n"
            f"Попробуй позже или смени таймфрейм"
        )

    parts = [f"🔍 <b>Глубокий скан</b> | {total_scanned} монет со всех бирж\n{'━'*24}\n"]

    # Сигналы SMC
    if signals:
        parts.append(f"\n📡 <b>Найдено сигналов: {len(signals)}</b>\n")
        for s in signals[:5]:  # Топ-5
            sym = s.get("symbol", "")
            direction = s.get("direction", "")
            entry = s.get("entry", 0)
            tp1 = s.get("tp1", 0)
            tp2 = s.get("tp2", 0)
            tp3 = s.get("tp3", 0)
            sl = s.get("sl", 0)
            grade = s.get("grade", "")
            emoji = "🟢" if "BULL" in direction else "🔴"

            parts.append(
                f"\n{grade}\n"
                f"{emoji} <b>{sym}</b> — {direction}\n"
                f"💰 Вход: <code>{entry:.4f}</code>\n"
                f"🛑 Стоп: <code>{sl:.4f}</code>\n"
                f"🎯 TP1: <code>{tp1:.4f}</code>\n"
                f"🎯 TP2: <code>{tp2:.4f}</code>\n"
                f"🎯 TP3: <code>{tp3:.4f}</code>\n"
            )

    # Накопления (потенциальные памп кандидаты)
    if accumulations:
        parts.append(f"\n📦 <b>Накопление (Wyckoff) — {len(accumulations)} монет:</b>\n")
        for a in accumulations[:5]:
            score = a["score"]
            fire = "🔥🔥🔥" if score >= 80 else "🔥🔥" if score >= 70 else "🔥"
            parts.append(
                f"{fire} <b>{a['symbol']}</b> — скор {score}/100\n"
                f"   {a['signal']}\n"
            )

    return "".join(parts)


async def auto_scan_job():
    """Каждые 15 мин: SMC сканирование топ-100 пар — присылаем только МЕГА ТОП (3/3 ТФ)"""
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

    pairs = get_top_pairs(100)
    mega_signals = []

    # Сканируем все пары — ищем только МЕГА ТОП
    for symbol in pairs:
        try:
            sig_data = full_scan_raw(symbol, "1h")
            if sig_data and sig_data.get("grade") in ("МЕГА ТОП", "ТОП СДЕЛКА"):
                # Не дублируем символ
                existing = [s for s in mega_signals if s["symbol"] == symbol]
                if not existing:
                    mega_signals.append(sig_data)
            await asyncio.sleep(0.2)
        except:
            pass

    logging.info(f"Скан 15мин: {len(pairs)} пар | 🔥🔥🔥 МЕГА ТОП: {len(mega_signals)}")

    if not ADMIN_ID:
        return

    # Отправляем только МЕГА ТОП — без спама
    for sd in mega_signals[:5]:
        try:
            await bot.send_message(ADMIN_ID, sd["text"], parse_mode="HTML")
            await asyncio.sleep(1)
        except:
            pass


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


def scan_all_for_deals(limit=40):
    """
    Сканирует топ пары и возвращает ВСЕ найденные сигналы.
    Используется кнопкой "🎯 Найти сделки".
    Возвращает список: [{symbol, direction, grade, grade_emoji, entry, sl, tp1, tp2, tp3, text}]
    """
    import concurrent.futures
    pairs = get_top_pairs(limit)
    found = []

    def scan_one(symbol):
        try:
            return full_scan_raw(symbol, "1h")
        except:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(scan_one, pairs))

    for r in results:
        if r and r.get("grade") in ("🔥🔥🔥 МЕГА ТОП", "🔥🔥 ТОП СДЕЛКА", "✅ ХОРОШАЯ"):
            found.append(r)

    # Сортируем: лучшие первыми
    grade_order = {"🔥🔥🔥 МЕГА ТОП": 0, "🔥🔥 ТОП СДЕЛКА": 1, "✅ ХОРОШАЯ": 2}
    found.sort(key=lambda x: grade_order.get(x.get("grade",""), 3))
    return found


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
    Авто-патч без разрешения — тихо чинит и деплоит.
    Высокий риск — пропускает. Низкий/средний — применяет автоматически.
    """
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return

    try:
        current_code, sha = github_get_file()
        if not current_code:
            return

        prompt = f"""Ты senior Python разработчик. В боте произошла ошибка.

ОШИБКА:
{error_text[:800]}

КОД (первые 3000 символов):
{current_code[:3000]}

Верни ТОЛЬКО JSON:
{{"description": "что исправлено (1 предложение)", "search": "точный уникальный фрагмент для замены", "replace": "исправленный фрагмент", "risk": "low/medium/high"}}"""

        response = ask_groq(prompt, max_tokens=800)
        if not response:
            return

        clean = response.strip().replace("```json", "").replace("```", "").strip()
        start = clean.find("{")
        end = clean.rfind("}") + 1
        if start < 0 or end <= start:
            return
        patch_data = json.loads(clean[start:end])

        search_text = patch_data.get("search", "")
        replace_text = patch_data.get("replace", "")
        description = patch_data.get("description", "auto-fix")
        risk = patch_data.get("risk", "high")

        # Высокий риск — не применяем автоматически
        if risk == "high":
            logging.warning(f"Auto-patch skipped (high risk): {description}")
            return

        if not search_text or search_text not in current_code:
            return

        new_code = current_code.replace(search_text, replace_text, 1)
        success, commit_sha = github_push_patch(
            new_code, sha,
            f"🤖 APEX auto-fix: {description[:60]}"
        )
        if success:
            logging.info(f"Auto-patch OK: {description} | {commit_sha}")
        else:
            logging.error(f"Auto-patch push failed: {commit_sha}")

    except Exception as e:
        logging.error(f"analyze_and_patch error: {e}")

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
    """Перехватывает ERROR логи и запускает авто-патч — только реальные ошибки кода"""

    # Эти сообщения — не ошибки кода, игнорируем
    IGNORE_PATTERNS = [
        "нет свечей", "no candles", "свечей для", "klines",
        "bybit klines", "binance futures", "binance spot", "coingecko",
        "cryptocompare candles", "yahoo finance", "messari",
        "tavily", "rss parse", "pump detector",
        "накопление", "accumulation detect",
    ]

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            error_text = self.format(record)
            error_lower = error_text.lower()

            # Игнорируем не-ошибки (проблемы с внешними API — это нормально)
            if any(pattern in error_lower for pattern in self.IGNORE_PATTERNS):
                return

            # Только реальные ошибки Python — Traceback, Exception
            if not any(kw in error_text for kw in ["Traceback", "Exception", "Error:", "raise ", "line "]):
                return

            # Дедупликация — не спамим одной ошибкой
            error_key = error_text[:100]
            now = time.time()
            if now - last_error_time.get(error_key, 0) < error_cooldown:
                return
            last_error_time[error_key] = now

            # Запускаем авто-патч асинхронно
            try:
                loop = asyncio.get_running_loop()
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

async def restore_db_from_github():
    """При старте скачиваем brain.db из GitHub"""
    try:
        gh_token = os.environ.get("GITHUB_TOKEN", "")
        gh_repo = os.environ.get("GITHUB_REPO", "")
        if not gh_token or not gh_repo:
            logging.info("GH_TOKEN/GH_REPO не заданы — пропускаем восстановление DB")
            return
        import base64
        r = requests.get(
            f"https://api.github.com/repos/{gh_repo}/contents/brain.db",
            headers={"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"},
            timeout=10
        )
        if r.status_code == 200:
            content = base64.b64decode(r.json()["content"])
            with open("brain.db", "wb") as f:
                f.write(content)
            logging.info(f"brain.db восстановлен из GitHub ({len(content)//1024}KB)")
        else:
            logging.info("brain.db в GitHub не найден — начинаем с чистой базы")
    except Exception as e:
        logging.warning(f"restore_db_from_github: {e}")


async def backup_db_to_github():
    """Сохраняем brain.db в GitHub"""
    try:
        gh_token = os.environ.get("GITHUB_TOKEN", "")
        gh_repo = os.environ.get("GITHUB_REPO", "")
        if not gh_token or not gh_repo:
            return
        import base64
        with open("brain.db", "rb") as f:
            content = f.read()
        encoded = base64.b64encode(content).decode()
        # Получаем SHA для обновления
        r = requests.get(
            f"https://api.github.com/repos/{gh_repo}/contents/brain.db",
            headers={"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"},
            timeout=10
        )
        sha = r.json().get("sha", "") if r.status_code == 200 else ""
        payload = {
            "message": f"brain.db backup {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": encoded,
            "branch": "main"
        }
        if sha:
            payload["sha"] = sha
        r2 = requests.put(
            f"https://api.github.com/repos/{gh_repo}/contents/brain.db",
            headers={"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"},
            json=payload,
            timeout=20
        )
        if r2.status_code in (200, 201):
            logging.info(f"brain.db сохранён в GitHub ({len(content)//1024}KB)")
        else:
            logging.warning(f"backup_db_to_github: {r2.status_code}")
    except Exception as e:
        logging.warning(f"backup_db_to_github: {e}")


# ===== BRAIN BUILDER ИНТЕГРАЦИЯ =====
try:
    from brain_builder import (
        run_brain_builder, get_brain_summary,
        init_brain_db, DB_PATH as BRAIN_DB_PATH
    )
    BRAIN_BUILDER_AVAILABLE = True
    logging.info("brain_builder.py подключён ✅")
except ImportError:
    BRAIN_BUILDER_AVAILABLE = False
    logging.warning("brain_builder.py не найден — мозг работает в базовом режиме")

    def run_brain_builder(full=False):
        return {}

    def get_brain_summary():
        return {}


async def run_brain_builder_async():
    """Быстрый цикл brain builder (каждый час)"""
    try:
        loop = asyncio.get_running_loop()
        stats = await loop.run_in_executor(None, run_brain_builder, False)
        if stats:
            logging.info(f"🧠 Brain Builder (быстрый): знаний={stats.get('knowledge',0)} правил={stats.get('rules',0)}")
        # Бэкап БД в GitHub после обучения
        await backup_db_to_github()
    except Exception as e:
        logging.error(f"run_brain_builder_async: {e}")


async def run_brain_builder_full_async():
    """Полный цикл brain builder (раз в сутки в 3:00)"""
    try:
        loop = asyncio.get_running_loop()
        stats = await loop.run_in_executor(None, run_brain_builder, True)
        if stats:
            logging.info(
                f"🧠 Brain Builder (полный): "
                f"знаний={stats.get('knowledge',0)} правил={stats.get('rules',0)} "
                f"паттернов={stats.get('patterns',0)} монет={stats.get('coins',0)}"
            )
        await backup_db_to_github()
    except Exception as e:
        logging.error(f"run_brain_builder_full_async: {e}")


async def on_startup(app):
    init_db()
    await restore_db_from_github()
    threading.Thread(target=get_top_pairs, daemon=True).start()

    WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
    if WEBHOOK_URL:
        await bot.set_webhook(f"{WEBHOOK_URL}/webhook", drop_pending_updates=True)
        logging.info(f"Webhook установлен: {WEBHOOK_URL}/webhook")
    else:
        logging.warning("WEBHOOK_URL не задан — работаем в polling режиме")

    scheduler = AsyncIOScheduler(job_defaults={"misfire_grace_time": 60, "coalesce": True, "max_instances": 1})
    scheduler.add_job(auto_scan_job, "interval", minutes=15)
    scheduler.add_job(auto_accumulation_scan, "interval", hours=1)
    scheduler.add_job(auto_research, "interval", hours=2)
    scheduler.add_job(check_alerts, "interval", minutes=5)
    scheduler.add_job(night_brain_tasks, "interval", hours=4)
    scheduler.add_job(realtime_pump_detector, "interval", minutes=15)
    scheduler.add_job(autonomous_learning_cycle, "interval", hours=2, jitter=300)
    async def _self_diagnose_job():
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self_diagnose_and_grow)
        await loop.run_in_executor(None, auto_fill_knowledge_gaps)
    scheduler.add_job(_self_diagnose_job, "interval", hours=6, jitter=1200)
    # Самоанализ точности + обновление авто-правил
    async def _run_self_analysis():
        if _LEARNING_OK:
            import asyncio as _a
            await _a.get_event_loop().run_in_executor(None, _learn_self_analysis)
    scheduler.add_job(_run_self_analysis, "interval", hours=3, jitter=600)

    # Decay правил — раз в сутки ослабляем устаревшие правила
    async def _run_decay():
        if _LEARNING_OK:
            import asyncio as _a
            await _a.get_event_loop().run_in_executor(None, _learn_decay)
    scheduler.add_job(_run_decay, "cron", hour=4, minute=30)

    # Groq стратегия — обновляется раз в день в 5:00
    async def _run_strategy_update():
        if _LEARNING_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_build_strategy)
            logging.info("[Scheduler] Стратегия Groq обновлена")
    scheduler.add_job(_run_strategy_update, "cron", hour=5, minute=0)

    # Groq самодиагностика ошибок — каждые 12 часов
    async def _run_groq_diagnosis():
        if _LEARNING_OK:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _learn_self_diag)
            logging.info("[Scheduler] Groq самодиагностика завершена")
    scheduler.add_job(_run_groq_diagnosis, "interval", hours=12, jitter=600)
    # Brain Builder — каждые 3ч быстрый цикл (экономим токены), раз в сутки полный
    scheduler.add_job(run_brain_builder_async, "interval", hours=3, jitter=600)
    scheduler.add_job(run_brain_builder_full_async, "cron", hour=3, minute=0)
    scheduler.start()
    setup_error_capture()
    # Первый цикл обучения — через 60 сек после старта
    asyncio.get_running_loop().call_later(300, lambda: asyncio.create_task(run_brain_builder_async()))  # 5 мин после старта
    logging.info("APEX запущен!")


async def on_startup_diagnose(app):
    """Первая самодиагностика через 8 мин после старта"""
    await asyncio.sleep(480)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, self_diagnose_and_grow)
    logging.info("[SelfGrow] Стартовая диагностика завершена")

async def on_shutdown(app):
    logging.info("APEX остановлен")


def main():
    WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

    if WEBHOOK_URL:
        # Webhook — ручная реализация, работает с любой версией aiogram 3.x
        app = web.Application()

        async def health(request):
            # Включаем статистику токенов в health endpoint
            token_pct = round(_groq_tokens_used / _GROQ_DAILY_LIMIT * 100) if _GROQ_DAILY_LIMIT > 0 else 0
            return web.Response(text=f"APEX OK | tokens: {_groq_tokens_used}/{_GROQ_DAILY_LIMIT} ({token_pct}%)")
        app.router.add_get("/", health)
        app.router.add_get("/health", health)
        app.router.add_head("/", health)  # Render шлёт HEAD запросы

        async def handle_webhook(request):
            try:
                import json as _json
                data = await request.read()
                update = types.Update(**_json.loads(data))
                await dp.feed_update(bot, update)
            except Exception as e:
                logging.error(f"Webhook error: {e}")
            return web.Response(text="OK")

        async def token_stats(request):
            token_pct = round(_groq_tokens_used / _GROQ_DAILY_LIMIT * 100) if _GROQ_DAILY_LIMIT > 0 else 0
            return web.json_response({
                "tokens_used": _groq_tokens_used,
                "tokens_limit": _GROQ_DAILY_LIMIT,
                "percent": token_pct,
                "available": _tokens_available()
            })
        app.router.add_post("/webhook", handle_webhook)
        app.router.add_get("/tokens", token_stats)
        app.on_startup.append(on_startup)
        app.on_startup.append(on_startup_diagnose)
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
            scheduler = AsyncIOScheduler(job_defaults={"misfire_grace_time": 60, "coalesce": True, "max_instances": 1})
            scheduler.add_job(auto_scan_job, "interval", minutes=15)
            scheduler.add_job(auto_accumulation_scan, "interval", hours=1)
            scheduler.add_job(auto_research, "interval", hours=2)
            scheduler.add_job(check_alerts, "interval", minutes=5)
            scheduler.add_job(night_brain_tasks, "interval", hours=4)
            scheduler.add_job(realtime_pump_detector, "interval", minutes=15)
            scheduler.add_job(autonomous_learning_cycle, "interval", hours=2, jitter=300)
            scheduler.start()
            asyncio.get_running_loop().call_later(30, lambda: asyncio.create_task(autonomous_learning_cycle()))
            logging.info("APEX запущен в polling режиме")
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

        asyncio.run(polling_main())


if __name__ == "__main__":
    main()
