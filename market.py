Warning: truncated output (original token count: 117112)
Total output lines: 10215

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

# ── WAL патч — решает "database is locked" для всех connect в bot.py ──
# WAL патч — только здесь, один раз для всего процесса
if not getattr(sqlite3, '_wal_patched', False):
    _orig_connect = sqlite3.connect
    def _wal_connect(db, timeout=30, **kw):
        kw.setdefault("check_same_thread", False)
        conn = _orig_connect(db, timeout=timeout, **kw)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")
            conn.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            pass
        return conn
    sqlite3.connect = _wal_connect
    sqlite3._wal_patched = True

from groq import Groq
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
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
        find_supply_demand, detect_wyckoff_phase, check_multi_coin_correlation,
        get_fibonacci_levels, get_session_volume_profile, detect_mm_accumulation,
        detect_rsi_macd_divergence, calculate_vwap, get_liquidity_heatmap, detect_breaker_block,
        detect_smart_money_divergence, detect_inducement,
    )
    _SMC_ENGINE_OK = True
    logging.info("smc_engine.py загружен успешно")
except Exception as e:
    _SMC_ENGINE_OK = False
    logging.warning(f"smc_engine.py не найден: {e} — ищем в: {_sys.path[:3]}")
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
    get_fibonacci_levels = lambda c, d: {}
    get_session_volume_profile = lambda c: {}
    detect_mm_accumulation = lambda c: {"score": 0, "signal": "NEUTRAL", "signals": [], "pre_pump": False}
    detect_rsi_macd_divergence = lambda c, d: {"found": False, "score": 0, "weight": 0, "signals": [], "rsi": 0}
    calculate_vwap = lambda c: {"vwap": 0, "signal": "NEUTRAL", "deviation_pct": 0, "near_vwap": False}
    get_liquidity_heatmap = lambda c: {"levels": [], "nearest_buy_stops": None, "nearest_sell_stops": None}
    detect_breaker_block = lambda c, d: None
    detect_smart_money_divergence = lambda c, o, f, d: {"score": 0, "signals": []}
    detect_inducement = lambda c, d: None
    find_supply_demand = lambda c, d: None
    detect_wyckoff_phase = lambda c: {"phase": "UNKNOWN", "score": 0, "signals": []}
    check_multi_coin_correlation = lambda s, d, fn: {"confirmed": 0, "total": 0, "score": 0, "strong": False}

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
        groq_weekly_report as _learn_weekly_report,
        groq_review_old_rules as _learn_review_rules,
        groq_ab_test_rules as _learn_ab_test,
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
except Exception as e:
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
    _learn_weekly_report = lambda: ""
    _learn_review_rules = lambda: None
    _learn_ab_test = lambda: None
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

# Web Learner — автономный поиск знаний
try:
    from web_learner import (
        run_web_learning_cycle as _web_learn_cycle,
        get_web_knowledge_summary as _web_knowledge_summary,
        groq_decide_learning_agenda as _web_groq_agenda,
        init_web_learner_db as _web_init_db,
        groq_self_improve as _web_self_improve,
    )
    _WEB_LEARNER_OK = True
    logging.info("web_learner.py загружен успешно")
except Exception as e:
    _WEB_LEARNER_OK = False
    logging.warning(f"web_learner.py не найден: {e}")
    _web_learn_cycle = lambda: []
    _web_knowledge_summary = lambda: "WebLearner недоступен"
    _web_groq_agenda = lambda: []
    _web_init_db = lambda: None
    _web_self_improve = lambda: []

# Groq Extensions — изолированный плагин, который Groq может свободно менять
try:
    from groq_extensions import (
        run_all_filters as _ext_run_filters,
        run_confluence_boosters as _ext_run_boosters,
        analyze_session_timing as _ext_session,
        get_extensions_summary as _ext_summary,
    )
    _EXT_OK = True
    logging.info("groq_extensions.py загружен успешно")
except Exception as e:
    _EXT_OK = False
    logging.warning(f"groq_extensions.py не найден: {e}")
    _ext_run_filters = lambda *a, **kw: (True, "")
    _ext_run_boosters = lambda *a: (0, [])
    _ext_session = lambda: {}
    _ext_summary = lambda: {}

# Brain Router — умный диспетчер источников данных и самообучение
try:
    import sys as _sys2, os as _os2
    _bd = _os2.path.dirname(_os2.path.abspath(__file__))
    if _bd not in _sys2.path:
        _sys2.path.insert(0, _bd)
    from brain_router import router as _brain_router
    _ROUTER_OK = True
    logging.info("brain_router.py загружен успешно")
except Exception as e:
    _ROUTER_OK = False
    logging.warning(f"brain_router.py не найден: {e}")
    class _DummyRouter:
        def candles(self, s, i="1h", l=200): return []
        def signal_context(self, *a, **k): return ""
        def accumulation(self, s): return {"score":0,"phase":"UNKNOWN","signals":[]}
        def contradictions(self, *a, **k): return {"conflicts":[],"warnings":[],"severity":"LOW","verdict":"","has_conflicts":False}
        def learn(self, *a, **k): pass
        def daily_review(self): pass
        def strategy(self): return ""
        def insights(self): return "brain_router.py не загружен"
        def source_stats(self): return "brain_router.py не загружен"
        def oi(self, s): return {"oi":0,"oi_change_4h":0,"signal":"NEUTRAL"}
        def funding(self, s): return {"rate":0,"signal":"NEUTRAL","warning":""}
        def social(self, s): return {"galaxy_score":0,"social_volume":0,"signal":"NEUTRAL"}
        def session(self): return {"session":"unknown","hour_utc":0,"quality":"?","day_of_week":0}
        def seasonality(self): return {"month":0,"btc_return":0,"bias":"NEUTRAL","notes":""}
    _brain_router = _DummyRouter()

# Autopilot — автономный самообучающийся мозг
try:
    from apex_autopilot import (
        run_autopilot_cycle as _autopilot_fast,
        run_deep_autopilot as _autopilot_deep,
        on_trade_closed as _autopilot_on_close,
        get_autopilot_status as _autopilot_status,
    )
    _AUTOPILOT_OK = True
    logging.info("apex_autopilot.py загружен успешно")
except Exception as e:
    _AUTOPILOT_OK = False
    logging.warning(f"apex_autopilot.py не найден: {e}")
    _autopilot_fast   = lambda: None
    _autopilot_deep   = lambda: None
    _autopilot_on_close = lambda *a, **kw: None
    _autopilot_status = lambda: "Автопилот недоступен"

TOKEN = os.environ.get("TELEGRAM_TOKEN")
_admin_raw = os.environ.get("ADMIN_ID", "0") or "0"
ADMIN_IDS = [int(x.strip()) for x in _admin_raw.split(",") if x.strip().isdigit()]
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0
SIGNAL_CHANNEL = int(os.environ.get("SIGNAL_CHANNEL_ID", "-1003122576951"))  # TG канал сигналов
SIGNAL_CHANNEL_MAIN = -1003614593530   # Все стратегии MTF+SWING+WYCKOFF
SIGNAL_CHANNEL_SWING = -1003122576951  # Канал с ветками
SWING_THREAD_ID = 262                  # Ветка Swing
FAST_DEAL_THREAD_ID = 264              # Ветка Fast deal
GROQ_KEY = os.environ.get("GROQ_API_KEY")
GROQ_KEYS = [k for k in [
    os.environ.get("GROQ_API_KEY", ""),
    *[os.environ.get(f"GROQ_API_KEY_{i}", "") for i in range(2, 20)]
] if k]
_groq_key_index = 0
TAVILY_KEY = os.environ.get("TAVILY_API_KEY", "")
TWELVEDATA_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
MOBULA_KEY     = os.environ.get("MOBULA_API_KEY", "")
COINALYZE_KEY  = os.environ.get("COINALYZE_API_KEY", "")
LUNARCRUSH_KEY = os.environ.get("LUNARCRUSH_API_KEY", "")
COINGLASS_KEY  = os.environ.get("COINGLASS_API_KEY", "")
SANTIMENT_KEY  = os.environ.get("SANTIMENT_API_KEY", "")

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

from aiohttp import ClientSession as _ClientSession, ClientTimeout as _ClientTimeout
_timeout = _ClientTimeout(total=30, connect=10)
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
    conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA cache_size=10000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA mmap_size=268435456")
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, direction TEXT, signal_type TEXT,
        entry REAL, tp1 REAL, tp2 REAL, tp3 REAL, sl REAL,
        timeframe TEXT, estimated_hours INTEGER, grade TEXT,
        result TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        closed_at TEXT,
        learning_id INTEGER DEFAULT NULL)""")

    # timing_queue — сигналы ожидающие подтверждения тайминга
    c.execute("""CREATE TABLE IF NOT EXISTS signal_cooldown (
        cache_key TEXT PRIMARY KEY,
        sent_at REAL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS timing_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, direction TEXT, timeframe TEXT,
        entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
        grade TEXT, signal_text TEXT,
        timing_score INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        expires_at TEXT,
        status TEXT DEFAULT 'waiting'
    )""")

    # signal_log — детальный лог (используется learning.py и autopilot)
    c.execute("""CREATE TABLE IF NOT EXISTS signal_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol      TEXT,
        direction   TEXT,
        grade       TEXT,
        entry       REAL,
        sl          REAL,
        tp1         REAL,
        tp2         REAL,
        tp3         REAL,
        timeframe   TEXT,
        result      TEXT    DEFAULT 'PENDING',
        hit_tp      INTEGER DEFAULT 0,
        rr_achieved REAL    DEFAULT 0,
        hours_open  REAL    DEFAULT 0,
        confluence  INTEGER DEFAULT 0,
        regime      TEXT,
        source      TEXT,
        notes       TEXT    DEFAULT '',
        created_at  TEXT    DEFAULT CURRENT_TIMESTAMP,
        closed_at   TEXT)""")

    # observations — наблюдения бота о рынке
    c.execute("""CREATE TABLE IF NOT EXISTS observations (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol      TEXT,
        observation TEXT,
        context     TEXT,
        created_at  TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # brain_log — лог всех событий мозга (autopilot, diagnoses, fixes)
    c.execute("""CREATE TABLE IF NOT EXISTS brain_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type  TEXT,
        title       TEXT,
        description TEXT,
        source      TEXT,
        created_at  TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # alerts
    c.execute("""CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, symbol TEXT, price REAL, direction TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        triggered INTEGER DEFAULT 0)""")

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

    # Миграция alerts — добавляем price_level если таблица создана со старой схемой (price)
    try:
        c.execute("ALTER TABLE alerts ADD COLUMN price_level REAL")
    except Exception:
        pass  # колонка уже есть

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

    # Миграция bot_errors — добавляем недостающие колонки в существующую таблицу
    try:
        _be_cols = [row[1] for row in c.execute("PRAGMA table_info(bot_errors)").fetchall()]
        if _be_cols:
            for _be_col, _be_type in [
                ("signal_id", "INTEGER"),
                ("symbol", "TEXT"),
                ("direction", "TEXT"),
                ("entry", "REAL"),
                ("sl", "REAL"),
                ("result", "TEXT"),
                ("error_type", "TEXT"),
                ("error_description", "TEXT"),
                ("ai_analysis", "TEXT"),
                ("ai_lesson", "TEXT"),
                ("ai_next_time", "TEXT"),
                ("fixed", "INTEGER DEFAULT 0"),
                ("fix_description", "TEXT"),
                ("hours_in_trade", "REAL"),
                ("market_context", "TEXT"),
                ("created_at", "TEXT DEFAULT CURRENT_TIMESTAMP"),
                ("fixed_at", "TEXT"),
            ]:
                if _be_col not in _be_cols:
                    try:
                        c.execute(f"ALTER TABLE bot_errors ADD COLUMN {_be_col} {_be_type}")
                    except Exception:
                        pass
    except Exception as _be_e:
        logging.warning(f"bot_errors migration in init_db: {_be_e}")

    # Таблицы из learning.py — создаём здесь тоже чтобы close_signal не падал
    c.execute("""CREATE TABLE IF NOT EXISTS signal_stats (
        symbol       TEXT PRIMARY KEY,
        total        INTEGER DEFAULT 0,
        wins         INTEGER DEFAULT 0,
        losses       INTEGER DEFAULT 0,
        tp1_hits     INTEGER DEFAULT 0,
        tp2_hits     INTEGER DEFAULT 0,
        tp3_hits     INTEGER DEFAULT 0,
        sl_hits      INTEGER DEFAULT 0,
        expired      INTEGER DEFAULT 0,
        win_rate     REAL    DEFAULT 0.0,
        avg_rr       REAL    DEFAULT 0.0,
        last_updated TEXT)""")
    # Миграция для существующих БД
    for col, typedef in [
        ("tp1_hits", "INTEGER DEFAULT 0"), ("tp2_hits", "INTEGER DEFAULT 0"),
        ("tp3_hits", "INTEGER DEFAULT 0"), ("sl_hits",  "INTEGER DEFAULT 0"),
        ("expired",  "INTEGER DEFAULT 0"), ("avg_rr",   "REAL DEFAULT 0.0"),
    ]:
        try: c.execute(f"ALTER TABLE signal_stats ADD COLUMN {col} {typedef}")
        except: pass

    c.execute("""CREATE TABLE IF NOT EXISTS auto_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_type TEXT, target TEXT, condition TEXT,
        confidence REAL DEFAULT 0.5, confirmed INTEGER DEFAULT 0,
        violated INTEGER DEFAULT 0, active INTEGER DEFAULT 1,
        last_check TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    c.execute("""CREATE TABLE IF NOT EXISTS self_analysis (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        period TEXT, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
        win_rate REAL DEFAULT 0, patterns TEXT, recommendations TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    c.execute("""CREATE TABLE IF NOT EXISTS pattern_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pattern_type TEXT, symbol TEXT, direction TEXT,
        result TEXT, hours_open REAL, timeframe TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # Добавляем confluence и regime в signals если нет
    for _col, _type in [("confluence", "INTEGER DEFAULT 0"), ("regime", "TEXT DEFAULT 'UNKNOWN'")]:
        try:
            c.execute(f"ALTER TABLE signals ADD COLUMN {_col} {_type}")
        except Exception:
            pass

    # Миграция signals — пересоздаём если нет колонки id (старые БД)
    try:
        # Сначала убираем signals_old если осталась от прошлой неудачной миграции
        try:
            c.execute("DROP TABLE IF EXISTS signals_old")
        except Exception:
            pass
        cols = [row[1] for row in c.execute("PRAGMA table_info(signals)").fetchall()]
        if "id" not in cols:
            c.execute("ALTER TABLE signals RENAME TO signals_old")
            c.execute("""CREATE TABLE signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, direction TEXT, signal_type TEXT,
                entry REAL, tp1 REAL, tp2 REAL, tp3 REAL, sl REAL,
                timeframe TEXT, estimated_hours INTEGER, grade TEXT,
                result TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT,
                learning_id INTEGER DEFAULT NULL,
                confluence INTEGER DEFAULT 0,
                regime TEXT DEFAULT 'UNKNOWN')""")
            # Копируем данные — только колонки которые точно есть
            old_cols = [row[1] for row in c.execute("PRAGMA table_info(signals_old)").fetchall()]
            copy_cols = [col for col in ["symbol","direction","signal_type","entry",
                         "tp1","tp2","tp3","sl","timeframe","estimated_hours",
                         "grade","result","created_at","closed_at"] if col in old_cols]
            cols_str = ", ".join(copy_cols)
            c.execute(f"INSERT INTO signals ({cols_str}) SELECT {cols_str} FROM signals_old")
            c.execute("DROP TABLE signals_old")
            conn.commit()
            logging.info("signals table migrated: added id column")
        else:
            logging.info("signals table OK: id column exists")
    except Exception as e:
        logging.error(f"signals migration: {e}")
        # Аварийный вариант — просто дропаем и создаём заново (теряем старые данные)
        try:
            c.execute("DROP TABLE IF EXISTS signals_old")
            c.execute("DROP TABLE IF EXISTS signals")
            c.execute("""CREATE TABLE signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, direction TEXT, signal_type TEXT,
                entry REAL, tp1 REAL, tp2 REAL, tp3 REAL, sl REAL,
                timeframe TEXT, estimated_hours INTEGER, grade TEXT,
                result TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT,
                learning_id INTEGER DEFAULT NULL,
                confluence INTEGER DEFAULT 0,
                regime TEXT DEFAULT 'UNKNOWN')""")
            conn.commit()
            logging.warning("signals table recreated (emergency)")
        except Exception as e2:
            logging.error(f"signals emergency recreate: {e2}")

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
        rule_type TEXT,
        rule_text TEXT,
        confidence REAL DEFAULT 0.5,
        confirmed_by INTEGER DEFAULT 0,
        contradicted_by INTEGER DEFAULT 0,
        source TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    # Миграция — добавляем колонки если их нет (для существующих БД)
    for col, typedef in [
        ("rule_type",       "TEXT"),
        ("rule_text",       "TEXT UNIQUE"),
        ("source",          "TEXT"),
        ("category",        "TEXT"),
        ("rule",            "TEXT"),
        ("confirmed_by",    "INTEGER DEFAULT 0"),
        ("contradicted_by", "INTEGER DEFAULT 0"),
        ("updated_at",      "TEXT DEFAULT CURRENT_TIMESTAMP"),
        ("active",          "INTEGER DEFAULT 1"),
        ("symbol",          "TEXT DEFAULT ''"),
        ("direction",       "TEXT DEFAULT ''"),
        ("strategy",        "TEXT DEFAULT ''"),
    ]:
        try:
            c.execute(f"ALTER TABLE self_rules ADD COLUMN {col} {typedef}")
        except Exception:
            pass
    # Активируем все старые записи у которых active=NULL
    try:
        c.execute("UPDATE self_rules SET active=1 WHERE active IS NULL")
    except Exception:
        pass

    # Миграция alerts — добавляем price_level если нет
    try:
        c.execute("ALTER TABLE alerts ADD COLUMN price_level REAL")
    except Exception:
        pass

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

    # Миграция signals — добавляем колонки если нет
    for col, typedef in [
        ("learning_id", "INTEGER DEFAULT NULL"),
        ("confluence",  "INTEGER DEFAULT 0"),
        ("regime",      "TEXT DEFAULT 'UNKNOWN'"),
        ("tp1_hit",     "INTEGER DEFAULT 0"),
        ("trailing_sl", "REAL DEFAULT NULL"),
        ("best_price",  "REAL DEFAULT NULL"),
    ]:
        try:
            c.execute(f"ALTER TABLE signals ADD COLUMN {col} {typedef}")
        except Exception:
            pass

    # Миграция brain_log — добавляем колонки title и source если нет
    for col, typedef in [("title", "TEXT"), ("source", "TEXT"), ("impact", "TEXT")]:
        try:
            c.execute(f"ALTER TABLE brain_log ADD COLUMN {col} {typedef}")
        except Exception:
            pass

    # Миграция web_knowledge и learning_agenda — добавляем query если нет
    for tbl_col in [("web_knowledge", "query", "TEXT"),
                    ("learning_agenda", "query", "TEXT")]:
        try:
            c.execute(f"ALTER TABLE {tbl_col[0]} ADD COLUMN {tbl_col[1]} {tbl_col[2]}")
        except Exception:
            pass

    # symbol_stats — статистика по монетам для web_learner
    c.execute("""CREATE TABLE IF NOT EXISTS symbol_stats (
        symbol TEXT PRIMARY KEY,
        win_rate REAL DEFAULT 0,
        total INTEGER DEFAULT 0,
        avg_rr REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    conn.commit()
    conn.close()

def get_user_memory(user_id):
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
                """INSERT INTO user_memory
                (user_id, name, profile, preferences, coins_mentioned, deposit, risk_percent, total_messages, first_seen, last_seen)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (user_id, name, profile or "", preferences or "", coins or "", deposit or 0, 1.0, 0, now, now)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Memory error: {e}")

def save_chat_log(user_id, role, content):
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        conn.execute("INSERT INTO chat_log (user_id, role, content, created_at) VALUES (?,?,?,CURRENT_TIMESTAMP)", (user_id, role, content[:2000]))
        conn.commit()
        conn.close()
    except:
        pass

def get_chat_history(user_id, limit=15):
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
    "1d": 336, "3d": 360, "1w": 720, "1M": 2880
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
    # python-binance заблокирован на Render (geo restriction) — используем только REST
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

# ── In-memory caches ──
import threading as _threading
import queue as _queue
_CACHE_LOCK = _threading.Lock()

# ── Единая очередь записей в БД ──
_DB_WRITE_QUEUE = _queue.Queue()
_DB_WRITER_RUNNING = False

def _db_writer_thread():
    """Единый поток записи в БД — исключает параллельные записи"""
    import sqlite3 as _sq3
    while True:
        try:
            task = _DB_WRITE_QUEUE.get(timeout=1)
            if task is None:
                break
            sql, params, callback = task
            try:
                conn = _sq3.connect("brain.db", timeout=30)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=30000")
                conn.execute(sql, params or [])
                conn.commit()
                conn.close()
                if callback:
                    callback(True)
            except Exception as e:
                logging.warning(f"[DB Writer] {e}")
                if callback:
                    callback(False)
        except _queue.Empty:
            continue
        except Exception as e:
            logging.error(f"[DB Writer] Fatal: {e}")

def start_db_writer():
    """Запустить поток записи БД"""
    global _DB_WRITER_RUNNING
    if not _DB_WRITER_RUNNING:
        t = _threading.Thread(target=_db_writer_thread, daemon=True)
        t.start()
        _DB_WRITER_RUNNING = True
        logging.info("[DB Writer] Запущен")

def db_write_async(sql: str, params: tuple = None):
    """Добавить запись в очередь (не блокирует)"""
    _DB_WRITE_QUEUE.put((sql, params, None))

def get_db_conn(path: str = "brain.db", timeout: int = 30) -> "sqlite3.Connection":
    """Получить соединение с БД с WAL + busy_timeout"""
    conn = sqlite3.connect(path, timeout=timeout, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn
_INDICATORS_CACHE: dict = {}       # {symbol:tf: (timestamp, indicators)}
_btc_corr_cache: dict = {}         # {symbol: (timestamp, result)}
_adaptive_params_cache: dict = {}  # {symbol:tf: (timestamp, params)}
_liquidity_cache: dict = {}        # {symbol: (timestamp, result)}
_INDICATORS_TTL = 60               # секунд

# ── Global candles storage (in-memory) ──
_GLOBAL_CANDLES: dict = {}         # {symbol:tf: candles}
_GLOBAL_CANDLES_TS: dict = {}      # {symbol:tf: timestamp}
_GLOBAL_CANDLES_TTL = 60           # секунд


def update_global_candles(symbol: str, timeframe: str, candles: list):
    """Обновить глобальный кеш свечей"""
    _key = f"{symbol}:{timeframe}"
    import time as _t
    _GLOBAL_CANDLES[_key] = candles
    _GLOBAL_CANDLES_TS[_key] = _t.time()


def get_global_candles(symbol: str, timeframe: str) -> list:
    """Получить свечи из глобального кеша"""
    import time as _t
    _key = f"{symbol}:{timeframe}"
    if _key in _GLOBAL_CANDLES:
        if _t.time() - _GLOBAL_CANDLES_TS.get(_key, 0) < _GLOBAL_CANDLES_TTL:
            return _GLOBAL_CANDLES[_key]
    return []



# ═══════════════════════════════════════════════════════════════
# OPEN INTEREST + FUNDING RATE + LIQUIDATION DATA
# ═══════════════════════════════════════════════════════════════

def get_open_interest(symbol: str) -> dict:
    """Open Interest с Binance Futures — накопление позиций крупных игроков"""
    try:
        r = requests.get(
            f"{BINANCE_F}/fapi/v1/openInterest",
            params={"symbol": symbol},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            oi = float(data.get("openInterest", 0))
            return {"oi": oi, "symbol": symbol, "ok": True}
    except Exception as e:
        logging.debug(f"OI {symbol}: {e}")
    return {"oi": 0, "ok": False}


def get_funding_rate(symbol: str) -> dict:
    """Funding Rate с Binance Futures — перегрев рынка"""
    try:
        r = requests.get(
            f"{BINANCE_F}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            fr = float(data.get("lastFundingRate", 0))
            # Интерпретация
            if fr > 0.001:
                signal = "BEARISH"  # Лонги перегреты — скоро выбьют
                desc = f"Funding +{fr*100:.3f}% — лонги перегреты"
            elif fr < -0.001:
                signal = "BULLISH"  # Шорты перегреты — скоро выбьют
                desc = f"Funding {fr*100:.3f}% — шорты перегреты"
            else:
                signal = "NEUTRAL"
                desc = f"Funding {fr*100:.3f}% — нейтрально"
            return {"rate": fr, "signal": signal, "desc": desc, "ok": True}
    except Exception as e:
        logging.debug(f"Funding {symbol}: {e}")
    return {"rate": 0, "signal": "NEUTRAL", "desc": "", "ok": False}


def get_liquidation_ratio(symbol: str) -> dict:
    """Long/Short ratio — соотношение лонгов и шортов"""
    try:
        r = requests.get(
            "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": "1h", "limit": 3},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            if data and len(data) > 0:
                latest = data[0]
                long_pct  = float(latest.get("longAccount", 0.5))
                short_pct = float(latest.get("shortAccount", 0.5))
                ratio = long_pct / short_pct if short_pct > 0 else 1.0
                if ratio > 1.5:
                    signal = "BEARISH"  # Слишком много лонгов — будут выбивать
                    desc = f"L/S={ratio:.2f} — толпа в лонгах (контрариан BEARISH)"
                elif ratio < 0.67:
                    signal = "BULLISH"  # Слишком много шортов — будут выбивать
                    desc = f"L/S={ratio:.2f} — толпа в шортах (контрариан BULLISH)"
                else:
                    signal = "NEUTRAL"
                    desc = f"L/S={ratio:.2f} — сбалансировано"
                return {
                    "long_pct": long_pct,
                    "short_pct": short_pct,
                    "ratio": ratio,
                    "signal": signal,
                    "desc": desc,
                    "ok": True,
                }
    except Exception as e:
        logging.debug(f"LiqRatio {symbol}: {e}")
    return {"ratio": 1.0, "signal": "NEUTRAL", "desc": "", "ok": False}

def get_top_pairs(limit=100):
    """Фиксированный список топ-80 монет — только проверенные пары с ликвидностью"""
    global pairs_cache, pairs_cache_time
    if time.time() - pairs_cache_time < 3600 and pairs_cache:
        return pairs_cache[:limit]

    FIXED_60 = [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
        "TONUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ARBUSDT",
        "ADAUSDT", "DOTUSDT", "POLUSDT", "LTCUSDT", "ATOMUSDT",
        "NEARUSDT", "INJUSDT", "SUIUSDT", "APTUSDT", "OPUSDT",
        "UNIUSDT", "PEPEUSDT", "SHIBUSDT", "TRXUSDT", "XLMUSDT",
        "WLDUSDT", "SEIUSDT", "JUPUSDT", "BONKUSDT", "BCHUSDT",
        "ICPUSDT", "ETCUSDT", "FILUSDT", "HBARUSDT", "STXUSDT",
        "LDOUSDT", "RENDERUSDT", "FETUSDT", "APEUSDT", "FLOKIUSDT",
        "WIFUSDT", "AAVEUSDT", "CRVUSDT", "GRTUSDT", "SNXUSDT",
        "RUNEUSDT", "ENAUSDT", "TAOUSDT", "NOTUSDT", "CATIUSDT",
        "VIRTUALUSDT", "DYMUSDT", "VANAUSDT", "PENGUUSDT", "BOMEUSDT",
        "POPCATUSDT", "BBUSDT", "SATSUSDT", "WUSDT", "ONDOUSDT",
    ]

    pairs_cache = FIXED_60[:limit]
    pairs_cache_time = time.time()
    return pairs_cache


def get_live_prices():
    global price_cache, last_price_update
    if time.time() - last_price_update < 20 and price_cache:
        return price_cache

    # 0. KuCoin allTickers — работает на Render, все пары одним запросом
    try:
        r = requests.get(
            "https://api.kucoin.com/api/v1/market/allTickers",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        tickers = r.json().get("data", {}).get("ticker", [])
        if tickers:
            market = {}
            for t in tickers:
                sym = t.get("symbol", "").replace("-", "")
                if sym.endswith("USDT"):
                    try:
                        price = float(t.get("last") or 0)
                        change = float(t.get("changeRate") or 0) * 100
                        vol = float(t.get("volValue") or 0)
                        if price > 0:
                            market[sym] = {"price": price, "change": round(change, 2), "volume": vol}
                    except Exception:
                        pass
            if len(market) >= 10:
                price_cache = market
                last_price_update = time.time()
                logging.info(f"Цены: KuCoin ({len(market)} пар)")
                return price_cache
    except Exception as e:
        logging.warning(f"KuCoin prices: {e}")

    # 0b. …92112 tokens truncated…   f"Funding: {_wyd_fund_str} | Fear&Greed: {_wyd_fg_str}\n"
                f"{_wyd_ob_str} | {_wyd_fvg_str}"
                f"{_wyd_pat_str}"
                f"{_self_rules}"
                f"{_recent_errors}"
            )
            groq_resp = ask_groq(groq_prompt, max_tokens=120)
            if groq_resp:
                import json as _j, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                m = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if m:
                    parsed = _j.loads(m.group())
                    # Groq как фильтр — если valid=false, блокируем
                    if not parsed.get("valid", True):
                        logging.info(f"[WYCKOFF Groq] {symbol} SHORT: Groq отклонил сигнал")
                        return None
                    if parsed.get("logic"):
                        logic = str(parsed["logic"]).strip()
        except Exception:
            pass

        if not logic:
            logic = f"UTAD после BC+AR+ST — дистрибуция Wyckoff"

        # Structural target only; Groq is not allowed to modify it.
        tp = min(tp, entry * 0.95)
        tp = max(tp, entry * 0.50)
        tp = round(tp, 8)

        risk   = abs(sl - entry)
        reward = abs(entry - tp)
        if risk == 0 or not 2.0 <= reward / risk <= 4.0:
            return None

        rr     = round(reward / risk, 2)
        tp_pct = round((entry - tp) / entry * 100, 1)
        sl_pct = round((sl - entry) / entry * 100, 1)

        phase_names = [p for p in ["BC", "AR", "ST", "UTAD", "SOW"] if p in phases and (p not in ["UTAD","SOW"] or phases[p].get("found"))]

        _wyk_d_tp2 = smart_round(entry - abs(entry - tp) * 1.5)

        return {
            "symbol": symbol, "direction": "BEARISH",
            "timeframe": "1d", "entry": entry,
            "sl": sl, "tp": tp, "tp2": _wyk_d_tp2,
            "sl_pct": sl_pct, "tp_pct": tp_pct, "rr": rr,
            "logic": logic, "score": min(score, 100),
            "pump_pct": pump_pct, "dist_range": dist_range_pct,
            "utad": utad_found, "sow": sow_found,
            "phases": " → ".join(phase_names),
            "dist_low": dist_low, "dist_high": dist_high,
            "ob": _wyk_ob, "fvg": _wyk_fvg,
            "scan_type": "wyckoff",
        }

    except Exception as e:
        logging.debug(f"detect_wyckoff_distribution {symbol}: {e}")
        return None


def detect_wyckoff_reaccumulation(symbol: str) -> dict | None:
    """
    Re-accumulation: боковик после коррекции + higher lows + ликвидность выше
    Работает чаще чем классический Wyckoff (раз в неделю vs раз в полгода)
    """
    try:
        _skip_symbol, _skip_reason = _learn_should_skip(symbol, "BULLISH")
        if _skip_symbol:
            logging.info(f"[WYCKOFF] {symbol}: {_skip_reason}")
            return None
        candles_1d = get_candles(symbol, "1d", 60)
        candles_4h = get_candles(symbol, "4h", 100)
        if not candles_1d or len(candles_1d) < 30: return None
        if not candles_4h or len(candles_4h) < 50: return None

        price_now = candles_1d[-1]["close"]

        # ── 1. Коррекция от пика (5% для BTC/ETH/BNB, 8% для остальных) ──
        price_peak = max(c["high"] for c in candles_1d[-40:-10])
        drawdown_pct = (price_peak - price_now) / price_peak * 100
        _min_drawdown = 3 if symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"] else 5
        if drawdown_pct < _min_drawdown:
            return None

        # ── 2. Боковик последние 10-30 дней (range < 15%) ──
        acc_candles = candles_1d[-30:]
        acc_high = max(c["high"] for c in acc_candles)
        acc_low = min(c["low"] for c in acc_candles)
        acc_range_pct = (acc_high - acc_low) / acc_low * 100
        if acc_range_pct > 15:
            return None

        # ── 3. Higher lows — покупатели давят снизу ──
        lows_20 = [c["low"] for c in acc_candles]
        local_lows = []
        for i in range(1, len(lows_20)-1):
            if lows_20[i] < lows_20[i-1] and lows_20[i] < lows_20[i+1]:
                local_lows.append(lows_20[i])
        higher_lows = len(local_lows) >= 2 and local_lows[-1] > local_lows[-2]
        if not higher_lows:
            return None

        # ── 4. Volume compression — объём снижается в боковике ──
        avg_vol_before = sum(c["volume"] for c in candles_1d[-40:-20]) / 20
        avg_vol_acc = sum(c["volume"] for c in acc_candles) / len(acc_candles)
        vol_compressed = avg_vol_acc < avg_vol_before * 0.8
        if not vol_compressed:
            return None

        # ── 5. Volume expansion — первый взрыв объёма после compression ──
        last_vol = candles_1d[-1]["volume"]
        avg_vol_acc_last = sum(c["volume"] for c in candles_1d[-10:-1]) / 9
        vol_expanding = last_vol > avg_vol_acc_last * 1.5

        # ── 6. Ликвидность выше — EQH или swing high ──
        highs_acc = [c["high"] for c in acc_candles]
        eqh_levels = [h for h in highs_acc if abs(h - acc_high) / acc_high < 0.005]
        liquidity_target = acc_high if len(eqh_levels) >= 2 else price_peak * 0.95

        # ── 7. BTC фильтр ──
        if symbol != "BTCUSDT":
            btc_ok, _ = btc_allows_signal("BULLISH")
            if not btc_ok: return None

        # ── 8. Расчёт уровней ──
        entry = smart_round(price_now)
        sl = smart_round(acc_low * 0.98)
        tp = smart_round(liquidity_target)

        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk == 0: return None
        rr = round(reward / risk, 2)
        if rr < 2.5: return None

        signals = ["Higher Lows", "Vol Compression", "Liquidity Above"]
        if vol_expanding:
            signals.append("Vol Expansion")

        # Range tightening — сужение диапазона последних 10 дней
        _ranges_wy = [c["high"] - c["low"] for c in candles_1d[-10:]]
        _avg_range_early = sum(_ranges_wy[:5]) / 5 if len(_ranges_wy) >= 5 else 1
        _avg_range_late = sum(_ranges_wy[5:]) / 5 if len(_ranges_wy) >= 10 else _avg_range_early
        if _avg_range_late < _avg_range_early * 0.8:
            signals.append("Range Tightening")

        # ── 9. Groq анализ ──
        try:
            _self_rules = get_relevant_rules(symbol, "BULLISH", "WYCKOFF")
            _recent_errors = get_recent_errors(symbol)
            _wyk_prompt = (
                "Ты SMC трейдер эксперт по накоплению Вайкоффа.\n"
                'Отвечай СТРОГО JSON: {"logic": "макс 15 слов", "valid": true/false}\n\n'
                "КАК ДУМАТЬ:\n"
                "1. Higher lows = покупатели накапливают позиции\n"
                "2. Volume compression = умные деньги поглощают продажи тихо\n"
                "3. Ликвидность выше (EQH) = цель для выноса стопов\n"
                "4. Стоп ЗА acc_low — ниже всей зоны накопления\n"
                "5. TP на ликвидности (EQH/swing high)\n\n"
                "БЛОКИРУЙ если:\n"
                "- Higher lows слабые или нет compression\n"
                f"- RR={rr} < 2.5\n"
                "- BTC в нисходящем тренде\n"
                "- Нет чёткой ликвидности выше для TP\n"
                "- SL выставлен математически (entry ± X%), а не за структуру\n\n"
                "ПОДТВЕРЖДАЙ если:\n"
                "- Чёткие higher lows + volume compression\n"
                "- Коррекция 8%+ от пика завершена\n"
                "- Ликвидность (EQH) чётко видна выше\n\n"
                "УРОВНИ УЖЕ РАССЧИТАНЫ СТРАТЕГИЕЙ. НИКОГДА НЕ МЕНЯЙ entry, SL или TP.\n\n"
                f"Данные: drawdown={round(drawdown_pct,1)}% range={round(acc_range_pct,1)}% "
                f"higher_lows={higher_lows} vol_compressed={vol_compressed} "
                f"vol_expanding={vol_expanding} (объём растёт = выход начался)\n"
                f"entry={smart_price_fmt(entry)} sl={smart_price_fmt(sl)} tp={smart_price_fmt(tp)} RR={rr}"
                f"{_self_rules}"
                f"{_recent_errors}"
            )
            _resp = ask_groq(_wyk_prompt, max_tokens=100)
            if _resp:
                import json as _j, re as _re
                _m = _re.search(r'\{[^}]+\}', _resp, _re.DOTALL)
                if _m:
                    _p = _j.loads(_m.group())
                    if not _p.get("valid", True): return None
        except Exception:
            pass

        # TP remains the structural liquidity target calculated above.
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk == 0:
            return None
        rr = round(reward / risk, 2)
        if not 2.5 <= rr <= 4.0:
            return None
        tp_pct = round((tp - entry) / entry * 100, 1)
        sl_pct = round((entry - sl) / entry * 100, 1)
        _reac_tp2 = smart_round(entry + abs(tp - entry) * 1.5)

        return {
            "symbol": symbol, "direction": "BULLISH",
            "timeframe": "1d", "entry": entry, "sl": sl, "tp": tp, "tp2": _reac_tp2,
            "sl_pct": sl_pct, "tp_pct": tp_pct, "rr": rr,
            "score": 75, "signals": signals,
            "logic": f"Re-accumulation: higher lows + liquidity {smart_price_fmt(liquidity_target)}",
            "drawdown_pct": drawdown_pct, "acc_range": acc_range_pct,
            "phases": "Re-accumulation",
        }
    except Exception as e:
        logging.warning(f"detect_wyckoff_reaccumulation {symbol}: {e}")
        return None


# ===== СТРАТЕГИЯ 4: FAST DEAL 5M СКАЛЬПИНГ =====

FAST_PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT", "DOTUSDT",
    "MATICUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT",
    "SUIUSDT", "INJUSDT", "FETUSDT", "WIFUSDT", "PEPEUSDT"
]

def detect_fast_deal(symbol: str) -> dict | None:
    """
    SMC скальпинг на 5m:
    1. BTC направление — синхронизация с рынком
    2. 1d тренд — торгуем только по тренду
    3. 4h OB/FVG — цена в зоне интереса
    4. 1h импульсная свеча — подтверждение
    5. 5m sweep + возврат — точный вход
    Горизонт: 15-30 мин | Стоп: -0.5% | TP: +1%
    """
    try:
        from datetime import datetime as _dt
        _now_dt = _dt.utcnow()
        _hour = _now_dt.hour
        _minute = _now_dt.minute
        _time_minutes = _hour * 60 + _minute
        # Kill Zone блокируется в bot.py; здесь те же окна оставлены для
        # согласованности при ручном запуске detector.
        _in_london_kz = 510 <= _time_minutes <= 690
        _in_ny_kz     = 990 <= _time_minutes <= 1170
        if not (_in_london_kz or _in_ny_kz):
            return None

        # ── 1. BTC направление ──
        btc_candles_1h = get_candles("BTCUSDT", "1h", 10)
        btc_trend = "BULLISH" if btc_candles_1h and btc_candles_1h[-1]["close"] > btc_candles_1h[-3]["close"] else "BEARISH"

        # ── 2. 4h+1h consensus (мягкий — один из двух достаточно) ──
        direction_4h = smc_on_tf(symbol, "4h")
        direction_1h = smc_on_tf(symbol, "1h")
        if not direction_4h and not direction_1h:
            return None
        # Берём направление: приоритет 4h, fallback 1h
        direction_1d = direction_4h or direction_1h
        # Для редкого скальпа не берём конфликтующие 4h/1h направления.
        if direction_4h and direction_1h and direction_4h != direction_1h:
            return None

        # BTC фильтр: только для альткоинов — BTCUSDT не фильтруем через себя
        if symbol != "BTCUSDT":
            if direction_1d == "BULLISH" and btc_trend == "BEARISH":
                return None
            if direction_1d == "BEARISH" and btc_trend == "BULLISH":
                try:
                    btc_change = (btc_candles_1h[-1]["close"] - btc_candles_1h[-4]["close"]) / btc_candles_1h[-4]["close"] * 100
                    if btc_change > 1.0:
                        return None  # BTC растёт >1% — шорт альт опасен
                except Exception:
                    pass

        direction = direction_1d

        try:
            _skip_symbol, _skip_reason = _learn_should_skip(symbol, direction)
            if _skip_symbol:
                logging.info(f"[FAST] {symbol}: {_skip_reason}")
                return None
        except Exception:
            pass

        # ── 2.5. FR hard block для FAST ──
        try:
            _fast_funding = get_funding_rate(symbol)
            if _fast_funding is not None and abs(_fast_funding) > 0.2:
                if (direction == "BULLISH" and _fast_funding > 0.2) or (direction == "BEARISH" and _fast_funding < -0.2):
                    return None
        except Exception:
            pass

        # ── 3. 4h OB/FVG зона ──
        candles_4h = get_candles(symbol, "4h", 50)
        if not candles_4h or len(candles_4h) < 20:
            return None

        price_now = candles_4h[-1]["close"]
        ob_4h  = find_ob(candles_4h, direction)
        fvg_4h = find_fvg(candles_4h, direction)

        # Проверяем что цена в зоне 4h OB или FVG
        in_zone = False
        zone_desc = ""
        atr_4h = sum(c["high"] - c["low"] for c in candles_4h[-14:]) / 14
        _ap_fast = get_adaptive_params(symbol, candles_4h)
        _zone_tol = atr_4h * _ap_fast["volatility_factor"] * 0.5

        if ob_4h:
            zone_bottom = ob_4h["bottom"]
            zone_top    = ob_4h["top"]
            # Цена должна быть рядом с зоной (±ATR×0.5)
            if direction == "BULLISH" and zone_bottom - _zone_tol <= price_now <= zone_top + _zone_tol:
                in_zone = True
                zone_desc = f"4h OB ${zone_bottom:.4f}–${zone_top:.4f}"
            elif direction == "BEARISH" and zone_bottom - _zone_tol <= price_now <= zone_top + _zone_tol:
                in_zone = True
                zone_desc = f"4h OB ${zone_bottom:.4f}–${zone_top:.4f}"

        if not in_zone and fvg_4h:
            zone_bottom = fvg_4h["bottom"]
            zone_top    = fvg_4h["top"]
            if zone_bottom - _zone_tol <= price_now <= zone_top + _zone_tol:
                in_zone = True
                zone_desc = f"4h FVG ${zone_bottom:.4f}–${zone_top:.4f}"

        if not in_zone:
            return None

        # Не скальпим из середины диапазона: LONG только из discount,
        # SHORT только из premium.
        _range_high = max(c["high"] for c in candles_4h[-20:])
        _range_low = min(c["low"] for c in candles_4h[-20:])
        _range_mid = (_range_high + _range_low) / 2
        _range_size = _range_high - _range_low
        _in_premium = price_now > _range_mid + _range_size * 0.1
        _in_discount = price_now < _range_mid - _range_size * 0.1
        _no_middle_ok = (direction == "BULLISH" and _in_discount) or \
                        (direction == "BEARISH" and _in_premium)
        if not _no_middle_ok:
            return None

        # ── 4. 15m импульсная свеча (подтверждение на младшем ТФ) ──
        candles_15m_imp = get_candles(symbol, "15m", 20)
        if not candles_15m_imp or len(candles_15m_imp) < 3:
            return None

        last_15m = candles_15m_imp[-1]

        # Volume check на 15m impulse — должен быть выше среднего
        _avg_vol_15m_imp = sum(c.get("volume", 0) for c in candles_15m_imp[:-1]) / max(len(candles_15m_imp) - 1, 1)
        if _avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1:
            return None  # Импульс без объёма — ненадёжный

        # ── 5. 15m Engulfing + Displacement + Volume Spike ──
        candles_15m = get_candles(symbol, "15m", 30)
        if not candles_15m or len(candles_15m) < 10:
            return None

        atr_15m = sum(c["high"] - c["low"] for c in candles_15m[-14:]) / 14
        engulfing_found = False
        entry = None
        sl = None

        for i in range(1, 11):  # смотрим 10 свечей назад
            if i >= len(candles_15m): break
            curr = candles_15m[-i]
            prev = candles_15m[-i-1]

            curr_body = abs(curr["close"] - curr["open"])
            curr_range = curr["high"] - curr["low"]
            prev_body = abs(prev["close"] - prev["open"])

            # Подтверждённый displacement для точного входа.
            if curr_range > 0 and curr_body / curr_range < 0.65:
                continue

            # Engulfing паттерн
            if direction == "BULLISH":
                bull_eng = (curr["close"] > curr["open"] and
                           curr["open"] <= prev["close"] and
                           curr["close"] >= prev["open"] and
                           curr_body > prev_body * 1.1)
                if not bull_eng: continue
                entry = smart_round(curr["close"])
                sl = smart_round(curr["low"] - atr_15m * 0.5)
            else:
                bear_eng = (curr["close"] < curr["open"] and
                           curr["open"] >= prev["close"] and
                           curr["close"] <= prev["open"] and
                           curr_body > prev_body * 1.1)
                if not bear_eng: continue
                entry = smart_round(curr["close"])
                sl = smart_round(curr["high"] + atr_15m * 0.5)

            # Для FAST нужен заметный институциональный объём.
            _vol_threshold = 2.0
            avg_vol_15m = sum(c["volume"] for c in candles_15m[-20:-1]) / 19
            if avg_vol_15m > 0 and curr["volume"] < avg_vol_15m * _vol_threshold:
                continue

            engulfing_found = True
            _sweep_candles_ago = i
            break

        if not engulfing_found or entry is None:
            return None

        # ── Acceptance — проверяем на свече engulfing (не текущей) ──
        _eng_idx = _sweep_candles_ago if '_sweep_candles_ago' in dir() else 1
        _eng_candle = candles_15m[-_eng_idx] if _eng_idx < len(candles_15m) else candles_15m[-1]
        if ob_4h and direction == "BULLISH":
            _acceptance = _eng_candle["close"] > ob_4h["top"]
        elif ob_4h and direction == "BEARISH":
            _acceptance = _eng_candle["close"] < ob_4h["bottom"]
        elif fvg_4h and direction == "BULLISH":
            _acceptance = _eng_candle["close"] > fvg_4h["top"]
        elif fvg_4h and direction == "BEARISH":
            _acceptance = _eng_candle["close"] < fvg_4h["bottom"]
        else:
            _acceptance = False

        if not _acceptance:
            logging.debug(f"[FAST] {symbol}: нет acceptance — цена не закрылась за зоной")
            return None

        # ── TP = следующий EQH/FVG на 15m ──
        if direction == "BULLISH":
            tp1 = smart_round(entry + atr_15m * 2.5)
            tp2 = smart_round(entry + atr_15m * 4.0)
        else:
            tp1 = smart_round(entry - atr_15m * 2.5)
            tp2 = smart_round(entry - atr_15m * 4.0)
        tp = tp2  # основной TP для RR расчёта

        # ── RR проверка ──
        risk   = abs(entry - sl)
        reward = abs(tp1 - entry)
        if risk == 0:
            return None
        rr = round(reward / risk, 2)
        if not 2.0 <= rr <= 4.0:
            return None

        sl_pct = round(abs(entry - sl) / entry * 100, 2)
        tp_pct = round(abs(tp1 - entry) / entry * 100, 2)
        tp2_pct = round(abs(tp2 - entry) / entry * 100, 2)

        # ── Groq анализирует ──
        logic = ""
        try:
            # Дополнительный контекст для Groq
            _ob_4h_desc = f"OB: {ob_4h['bottom']:.4f}–{ob_4h['top']:.4f}" if ob_4h else "OB: нет"
            _fvg_4h_desc = f"FVG: {fvg_4h['bottom']:.4f}–{fvg_4h['top']:.4f}" if fvg_4h else "FVG: нет"
            _eng_c = candles_15m[-_sweep_candles_ago]
            _avg_vol_15m_g = sum(c.get("volume", 0) for c in candles_15m[-20:-1]) / 19 if len(candles_15m) >= 20 else 0
            _eng_vol_desc = f"Vol engulfing: {_eng_c.get('volume', 0):.0f}, avg: {_avg_vol_15m_g:.0f}" if _avg_vol_15m_g > 0 else ""

            # Fear&Greed, Funding, Market Regime
            _fast_fg = get_fear_greed()
            _fast_funding = get_funding_rate(symbol)
            _fast_regime = get_market_regime(symbol)
            _fast_fg_str = f"{_fast_fg['value']} ({_fast_fg['label']})" if _fast_fg else "N/A"
            _fast_fund_str = f"{_fast_funding:+.4f}%" if _fast_funding is not None else "N/A"
            _fast_regime_str = _fast_regime.get("mode", "?") if isinstance(_fast_regime, dict) else str(_fast_regime)

            # Pattern history для Groq
            _fast_pat_str = ""
            try:
                _fast_pat = _learn_patterns(symbol, direction, "15m", _fast_regime_str, 0)
                if _fast_pat.get("found") and _fast_pat.get("samples", 0) >= 3:
                    _fast_pat_str = (f"\nИстория похожих: {_fast_pat['samples']} сделок, "
                                     f"WR: {_fast_pat['win_rate']:.0f}%, avg RR: {_fast_pat['avg_rr']:.1f}, "
                                     f"вердикт: {_fast_pat.get('verdict', '?')}")
            except Exception:
                pass

            _fast_sl_pct = round(abs(entry - sl) / entry * 100, 2) if entry > 0 else 0
            _self_rules = get_relevant_rules(symbol, direction, "FAST")
            _recent_errors = get_recent_errors(symbol)
            groq_prompt = (
                "Ты Kill Zone скальпер — торгуешь ТОЛЬКО в London (07-11 UTC) и NY (15-19 UTC) сессии.\n"
                'Отвечай СТРОГО JSON: {"logic": "макс 10 слов", "valid": true/false}\n\n'
                "КАК ДУМАТЬ:\n"
                "1. 15m engulfing + displacement — тело > 65% range, поглощение предыдущей свечи\n"
                "2. 4h OB или FVG подтверждает зону — институционалы там входили\n"
                "3. Volume spike 2.0x — реальный интерес на engulfing свече\n"
                "4. Acceptance — цена закрылась за зоной OB/FVG\n"
                "5. BTC и 1d тренд совпадают — не иди против рынка\n\n"
                "БЛОКИРУЙ если:\n"
                f"- RR={rr} < 1.5\n"
                f"- Стоп {_fast_sl_pct}% > 1.5% от входа (скальп = узкий стоп)\n"
                "- Нет OB и нет FVG на 4h — вход без подтверждения зоны\n"
                "- 1d тренд ПРОТИВ направления\n"
                "- BTC тренд ПРОТИВ направления\n"
                "- Вне Kill Zone (London 07-11, NY 15-19 UTC)\n"
                "- SL выставлен математически (entry ± X%), а не за структуру\n\n"
                "ПОДТВЕРЖДАЙ если:\n"
                "- Engulfing чёткий с объёмом 2.0x+\n"
                "- 4h OB или FVG подтверждает зону входа\n"
                f"- RR={rr} >= 2.0\n"
                "- 1d тренд и BTC в том же направлении\n"
                "- Сейчас Kill Zone\n\n"
                "ПРАВИЛА ВЫСТАВЛЕНИЯ УРОВНЕЙ:\n"
                "- SL ТОЛЬКО за структурный уровень (OB edge, FVG edge, engulfing low/high)\n"
                "- ЗАПРЕЩЕНО: SL = entry ± X% (математические стопы не работают)\n"
                "- TP ТОЛЬКО на структурный уровень (OB, FVG, swing point)\n"
                "- Если нет структуры для SL — НЕ ВХОДИТЬ\n\n"
                f"ДАННЫЕ СЕТАПА:\n"
                f"Пара: {symbol} Направление: {direction}\n"
                f"15m engulfing ({_sweep_candles_ago} свечей назад) | Acceptance: {_acceptance}\n"
                f"4h зона: {zone_desc} | {_ob_4h_desc} | {_fvg_4h_desc}\n"
                f"Тренд: {direction_1d} | BTC: {btc_trend}\n"
                f"Funding: {_fast_fund_str} | Fear&Greed: {_fast_fg_str} | Режим: {_fast_regime_str}\n"
                f"{_eng_vol_desc}\n"
                f"Вход: {entry} SL: {sl} TP1: {tp1} TP2: {tp2}\n"
                f"RR: {rr} | Стоп: {_fast_sl_pct}%"
                f"{_fast_pat_str}"
                f"{_self_rules}"
                f"{_recent_errors}"
            )
            groq_resp = ask_groq(groq_prompt, max_tokens=80)
            if groq_resp:
                import json as _j, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                m = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if m:
                    parsed = _j.loads(m.group())
                    # Groq как фильтр — блокируем только если явно valid=false
                    if not parsed.get("valid", True):
                        return None
                    if parsed.get("logic"):
                        logic = str(parsed["logic"]).strip()
            else:
                logging.debug(f"[FAST Groq] {symbol}: Groq не ответил — fallback")
        except Exception as _fast_ge:
            logging.debug(f"[FAST Groq] {symbol}: {_fast_ge}")

        if not logic:
            logic = f"Engulfing 15m в зоне {zone_desc[:20]}"

        return {
            "symbol":    symbol,
            "direction": direction,
            "timeframe": "5m",
            "entry":     entry,
            "sl":        sl,
            "tp":        tp,
            "tp1":       tp1,
            "tp2":       tp2,
            "sl_pct":    sl_pct,
            "tp_pct":    tp_pct,
            "tp2_pct":   tp2_pct,
            "rr":        rr,
            "logic":     logic,
            "zone":      zone_desc,
            "direction_1d": direction_1d,
            "ob":        ob_4h,
            "fvg":       fvg_4h,
            "fast_score": 0,
            "scan_type": "fast",
        }

    except Exception as e:
        logging.debug(f"detect_fast_deal {symbol}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# ══  Precomputed Indicators Cache                              ══
# ═══════════════════════════════════════════════════════════════

def get_precomputed_indicators(symbol: str, timeframe: str = "4h") -> dict:
    """
    Считает ATR, ADX, EMA один раз и кеширует на 60с.
    Все стратегии используют этот кеш вместо повторных расчётов.
    """
    import time as _t
    _key = f"{symbol}:{timeframe}"
    _now = _t.time()

    with _CACHE_LOCK:
        if _key in _INDICATORS_CACHE:
            _ct, _cv = _INDICATORS_CACHE[_key]
            if _now - _ct < _INDICATORS_TTL:
                return _cv

    result = {}
    try:
        candles = get_candles(symbol, timeframe, 100)
        if not candles or len(candles) < 20:
            return result

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

        # ATR(14)
        _n14 = min(14, len(candles))
        result["atr"] = sum(highs[-i] - lows[-i] for i in range(1, _n14 + 1)) / _n14

        # ATR median(50)
        _n50 = min(50, len(candles))
        result["atr_med"] = sum(highs[-i] - lows[-i] for i in range(1, _n50 + 1)) / _n50

        # EMA20, EMA50, EMA200
        result["ema20"] = sum(closes[-min(20, len(closes)):]) / min(20, len(closes))
        result["ema50"] = sum(closes[-min(50, len(closes)):]) / min(50, len(closes))
        result["ema200"] = sum(closes[-min(200, len(closes)):]) / min(200, len(closes))

        # Volatility factor
        result["volatility_factor"] = round(
            max(0.6, min(1.8, result["atr"] / result["atr_med"] if result["atr_med"] > 0 else 1.0)), 2
        )

        # ADX(14)
        try:
            _adx_n = min(14, len(candles) - 1)
            plus_dm = minus_dm = tr_sum = 0
            for i in range(1, _adx_n + 1):
                h_diff = highs[-i] - highs[-i - 1]
                l_diff = lows[-i - 1] - lows[-i]
                plus_dm += h_diff if h_diff > l_diff and h_diff > 0 else 0
                minus_dm += l_diff if l_diff > h_diff and l_diff > 0 else 0
                tr_sum += max(
                    highs[-i] - lows[-i],
                    abs(highs[-i] - closes[-i - 1]),
                    abs(lows[-i] - closes[-i - 1])
                )
            atr14 = tr_sum / _adx_n if _adx_n > 0 else 1
            pdi = (plus_dm / _adx_n) / atr14 * 100 if atr14 > 0 else 0
            mdi = (minus_dm / _adx_n) / atr14 * 100 if atr14 > 0 else 0
            result["adx"] = round(abs(pdi - mdi) / (pdi + mdi) * 100, 1) if (pdi + mdi) > 0 else 20
        except Exception:
            result["adx"] = 20

        # Volume avg
        if len(candles) >= 21:
            result["avg_vol"] = sum(c["volume"] for c in candles[-20:-1]) / 19
        else:
            result["avg_vol"] = sum(c["volume"] for c in candles) / len(candles)

        # Price and structure
        result["price"] = closes[-1]
        result["hh_hl"] = len(closes) >= 10 and closes[-1] > closes[-5] > closes[-10]
        result["ll_lh"] = len(closes) >= 10 and closes[-1] < closes[-5] < closes[-10]

        # Trend strength
        result["trend_strength"] = (
            "strong" if result["adx"] > 30 else
            "normal" if result["adx"] > 20 else "weak"
        )
        result["adx_strong"] = result["adx"] > 30
        result["adx_weak"] = result["adx"] < 20

    except Exception as e:
        logging.warning(f"get_precomputed_indicators {symbol}: {e}")

    with _CACHE_LOCK:
        _INDICATORS_CACHE[_key] = (_now, result)

    return result


# ═══════════════════════════════════════════════════════════════
# ══  Adaptive Parameters — динамические параметры              ══
# ═══════════════════════════════════════════════════════════════

def get_adaptive_params(symbol: str, candles: list = None, timeframe: str = "4h") -> dict:
    """
    Возвращает адаптивные параметры. Использует precomputed indicators + кеш 5 мин.
    """
    import time as _time
    _cache_key = f"{symbol}:{timeframe}"
    _now = _time.time()
    if _cache_key in _adaptive_params_cache:
        _ct, _cv = _adaptive_params_cache[_cache_key]
        if _now - _ct < 300:
            return _cv

    result = {
        "volatility_factor": 1.0,
        "adx": 25.0,
        "dynamic_confluence": 30,
        "adx_strong": False,
        "adx_weak": False,
    }
    try:
        # Используем precomputed indicators вместо пересчёта
        ind = get_precomputed_indicators(symbol, timeframe)
        if ind:
            result["volatility_factor"] = ind.get("volatility_factor", 1.0)
            result["adx"] = ind.get("adx", 25.0)
            result["adx_strong"] = ind.get("adx_strong", False)
            result["adx_weak"] = ind.get("adx_weak", False)

        # ── Dynamic Confluence: повышаем после серии SL ──
        try:
            conn = sqlite3.connect("brain.db", timeout=10, check_same_thread=False)
            sl_count = conn.execute(
                "SELECT COUNT(*) FROM bot_errors WHERE error_type='SL_HIT' AND created_at > datetime('now', '-24 hours')"
            ).fetchone()[0]
            conn.close()
            if sl_count >= 5:
                result["dynamic_confluence"] = 45
            elif sl_count >= 3:
                result["dynamic_confluence"] = 38
            else:
                result["dynamic_confluence"] = 30
        except Exception:
            result["dynamic_confluence"] = 30

    except Exception as e:
        logging.debug(f"get_adaptive_params {symbol}: {e}")

    _adaptive_params_cache[_cache_key] = (_now, result)
    return result


def calc_size_multiplier(adx: float, confluence: int, recent_sl_count: int = 0) -> float:
    """
    Рассчитывает множитель размера позиции (0.5x — 1.5x).
    Высокий ADX + confluence → больше. Серия SL → меньше.
    """
    base = 1.0
    if adx > 35:
        base += 0.2
    elif adx < 15:
        base -= 0.2

    if confluence >= 50:
        base += 0.2
    elif confluence < 25:
        base -= 0.2

    if recent_sl_count >= 3:
        base -= 0.3
    elif recent_sl_count >= 2:
        base -= 0.15

    return round(max(0.5, min(base, 1.5)), 2)


def get_btc_correlation(symbol: str, btc_candles: list = None, period: int = 20) -> dict:
    """Rolling correlation с BTC. btc_candles передаётся снаружи чтобы не делать лишний API запрос."""
    import time as _t
    _now = _t.time()

    # Кеш 5 минут
    if symbol in _btc_corr_cache:
        _ct, _cv = _btc_corr_cache[symbol]
        if _now - _ct < 300:
            return _cv

    try:
        if symbol == "BTCUSDT":
            result = {"corr": 1.0, "level": "high", "btc_dir": "BULLISH", "desc": "BTC itself"}
            _btc_corr_cache[symbol] = (_now, result)
            return result

        # BTC свечи — используем переданные или берём из кеша
        if btc_candles is None:
            btc_candles = get_global_candles("BTCUSDT", "4h")
            if not btc_candles:
                btc_candles = get_candles("BTCUSDT", "4h", period + 5)

        alt_candles = get_candles(symbol, "4h", period + 5)

        if not alt_candles or not btc_candles or len(alt_candles) < period or len(btc_candles) < period:
            return {"corr": 0.7, "level": "moderate", "btc_dir": "UNKNOWN", "desc": "нет данных"}

        alt_ret = [alt_candles[-i]["close"] / alt_candles[-i - 1]["close"] - 1 for i in range(1, period + 1)]
        btc_ret = [btc_candles[-i]["close"] / btc_candles[-i - 1]["close"] - 1 for i in range(1, period + 1)]

        n = len(alt_ret)
        mean_a = sum(alt_ret) / n
        mean_b = sum(btc_ret) / n
        cov = sum((alt_ret[i] - mean_a) * (btc_ret[i] - mean_b) for i in range(n)) / n
        std_a = (sum((x - mean_a) ** 2 for x in alt_ret) / n) ** 0.5
        std_b = (sum((x - mean_b) ** 2 for x in btc_ret) / n) ** 0.5

        corr = round(cov / (std_a * std_b), 3) if std_a > 0 and std_b > 0 else 0.7

        btc_dir = "BULLISH" if btc_candles[-1]["close"] > btc_candles[-5]["close"] else "BEARISH"
        level = "high" if corr > 0.85 else "moderate" if corr > 0.3 else "low"

        result = {"corr": corr, "level": level, "btc_dir": btc_dir, "desc": f"Корр. BTC: {corr} ({level})"}
        _btc_corr_cache[symbol] = (_now, result)
        return result

    except Exception as e:
        logging.warning(f"get_btc_correlation {symbol}: {e}")
        return {"corr": 0.7, "level": "moderate", "btc_dir": "UNKNOWN", "desc": "ошибка"}


def check_session_liquidity(symbol: str, timeframe: str = "1h") -> dict:
    """
    Сравнивает текущий объём сессии с нормой за 20 свечей.
    ratio < 0.7 → skip (низкая ликвидность). Кеш 5 минут.
    """
    import time as _time
    _now = _time.time()
    _liq_key = f"{symbol}:{timeframe}"
    if _liq_key in _liquidity_cache:
        _ct, _cv = _liquidity_cache[_liq_key]
        if _now - _ct < 300:
            return _cv

    result = {"ratio": 1.0, "ok": True, "desc": ""}
    try:
        candles = get_candles(symbol, timeframe, 25)
        if not candles or len(candles) < 10:
            return result

        current_vol = candles[-1].get("volume", 0)
        avg_vol = sum(c.get("volume", 0) for c in candles[-21:-1]) / 20
        if avg_vol <= 0:
            return result

        ratio = round(current_vol / avg_vol, 2)
        ok = ratio >= 0.7
        result = {
            "ratio": ratio,
            "ok": ok,
            "desc": f"Vol ratio: {ratio:.2f}x" + ("" if ok else " (LOW)")
        }
    except Exception as e:
        logging.debug(f"check_session_liquidity {symbol}: {e}")
    _liquidity_cache[_liq_key] = (_now, result)
    return result


# ═══════════════════════════════════════════════════════════════
# ══  SMC Core Check — универсальное ядро проверки              ══
# ═══════════════════════════════════════════════════════════════

def smc_core_check(symbol: str, candles: list, direction: str, timeframe: str = "4h") -> dict | None:
    """
    Универсальное ядро SMC проверки.
    Используется всеми стратегиями.

    MUST: зона + тренд + RR
    CONFIRMATIONS: импульс + ликвидность + объём + тайминг
    """
    try:
        if not candles or len(candles) < 20:
            return None

        # ── Precomputed indicators — без повторных расчётов ──
        _ind = get_precomputed_indicators(symbol, timeframe)
        price = _ind.get("price", candles[-1]["close"])
        atr = _ind.get("atr", sum(c["high"] - c["low"] for c in candles[-14:]) / 14)
        ema20 = _ind.get("ema20", price)
        ema50 = _ind.get("ema50", price)
        hh_hl = _ind.get("hh_hl", False)
        ll_lh = _ind.get("ll_lh", False)
        _adx = _ind.get("adx", 20)
        _vf = _ind.get("volatility_factor", 1.0)

        if atr == 0:
            return None

        _ap = get_adaptive_params(symbol, candles, timeframe)

        # ── MUST 1: Зона OB/FVG ──
        ob = find_ob(candles, direction)
        fvg = find_fvg(candles, direction)
        in_ob = ob and abs(price - (ob["top"] + ob["bottom"]) / 2) <= atr * _vf
        in_fvg = fvg and abs(price - (fvg["top"] + fvg["bottom"]) / 2) <= atr * _vf
        zone = in_ob or in_fvg
        zone_desc = ""
        if in_ob and ob:
            zone_desc = f"OB {smart_price_fmt(ob['bottom'])}-{smart_price_fmt(ob['top'])}"
        elif in_fvg and fvg:
            zone_desc = f"FVG {smart_price_fmt(fvg['bottom'])}-{smart_price_fmt(fvg['top'])}"

        if not zone:
            return None

        # ── MUST 2: Тренд (EMA50 + структура HH/HL + ADX) ──
        if direction == "BULLISH":
            # Weak trend (ADX<20): разрешаем LONG даже ниже EMA50 если структура HH/HL
            if _ap["adx_weak"]:
                trend = hh_hl or (ema20 > ema50)
            else:
                trend = (price > ema50 and ema20 > ema50) or hh_hl
        else:
            if _ap["adx_weak"]:
                trend = ll_lh or (ema20 < ema50)
            else:
                trend = (price < ema50 and ema20 < ema50) or ll_lh

        if not trend:
            return None

        # ── MUST 3: RR (entry/sl/tp из структуры) ──
        if direction == "BULLISH":
            entry = smart_round(price)
            sl_candidate = ob["bottom"] * 0.998 if in_ob and ob else (fvg["bottom"] * 0.998 if in_fvg and fvg else entry - atr * _vf)
            sl = smart_round(max(sl_candidate, entry * 0.96))  # cap 4%
        else:
            entry = smart_round(price)
            sl_candidate = ob["top"] * 1.002 if in_ob and ob else (fvg["top"] * 1.002 if in_fvg and fvg else entry + atr * _vf)
            sl = smart_round(min(sl_candidate, entry * 1.04))  # cap 4%

        # TP — ближайшая ликвидность
        swing_highs, swing_lows = find_swings(candles, lookback=8)
        try:
            eqh, eql = find_equal_highs_lows(candles, lookback=30)
        except Exception:
            eqh, eql = None, None

        if direction == "BULLISH":
            tp_candidates = []
            if swing_highs:
                tp_candidates += [sh[1] for sh in swing_highs if sh[1] > entry * 1.005]
            if eqh and eqh > entry * 1.005:
                tp_candidates.append(eqh)
            tp1 = smart_round(min(tp_candidates)) if tp_candidates else smart_round(entry + atr * 3)
            tp2 = smart_round(entry + abs(tp1 - entry) * 1.5)
        else:
            tp_candidates = []
            if swing_lows:
                tp_candidates += [s[1] for s in swing_lows if s[1] < entry * 0.995]
            if eql and eql < entry * 0.995:
                tp_candidates.append(eql)
            tp1 = smart_round(max(tp_candidates)) if tp_candidates else smart_round(entry - atr * 3)
            tp2 = smart_round(entry - abs(entry - tp1) * 1.5)

        tp = tp1  # основной TP для RR
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk == 0:
            return None
        rr = round(reward / risk, 2)
        if rr < 2.0:
            return None

        # ── CONFIRMATIONS (нужно минимум 2 из 4) ──
        confirmations = 0
        confirm_details = []

        # 1. Импульс (displacement ≥0.45)
        try:
            last = candles[-1]
            _body = abs(last["close"] - last["open"])
            _range = last["high"] - last["low"]
            _disp = _body / _range >= 0.45 if _range > 0 else False
            _bull_imp = direction == "BULLISH" and last["close"] > last["open"] and _disp
            _bear_imp = direction == "BEARISH" and last["close"] < last["open"] and _disp
            if _bull_imp or _bear_imp:
                confirmations += 1
                confirm_details.append("impulse")
        except Exception:
            pass

        # 2. Ликвидность как цель
        if (direction == "BULLISH" and eqh and eqh > entry) or \
           (direction == "BEARISH" and eql and eql < entry):
            confirmations += 1
            confirm_details.append("liquidity")

        # 3. Объём выше среднего
        try:
            avg_vol = sum(c["volume"] for c in candles[-20:-1]) / 19
            if candles[-1]["volume"] > avg_vol * 1.2:
                confirmations += 1
                confirm_details.append("volume")
        except Exception:
            pass

        # 4. HTF подтверждает
        try:
            htf = smc_on_tf(symbol, "1d")
            if (direction == "BULLISH" and htf == "BULLISH") or \
               (direction == "BEARISH" and htf == "BEARISH"):
                confirmations += 1
                confirm_details.append("htf_1d")
        except Exception:
            pass

        if confirmations < 2:
            return None

        # ── BTC Correlation Filter ──
        _btc_corr = {"corr": 0.5, "level": "moderate", "btc_dir": "NEUTRAL"}
        if symbol != "BTCUSDT":
            try:
                _btc_cached = get_global_candles("BTCUSDT", "4h")
                _btc_corr = get_btc_correlation(symbol, btc_candles=_btc_cached if _btc_cached else None)
                if _btc_corr["level"] == "high":
                    # Высокая корреляция — BTC должен подтвердить
                    if (direction == "BULLISH" and _btc_corr["btc_dir"] == "BEARISH") or \
                       (direction == "BEARISH" and _btc_corr["btc_dir"] == "BULLISH"):
                        return None
                elif _btc_corr["level"] == "moderate":
                    # Умеренная — не блокируем, но понижаем score
                    if (direction == "BULLISH" and _btc_corr["btc_dir"] == "BEARISH") or \
                       (direction == "BEARISH" and _btc_corr["btc_dir"] == "BULLISH"):
                        confirmations -= 1
                        if confirmations < 2:
                            return None
            except Exception:
                pass

        # ── Size Multiplier ──
        _sl_count_24h = 0
        try:
            _sc = sqlite3.connect("brain.db", timeout=10, check_same_thread=False)
            _sl_count_24h = _sc.execute(
                "SELECT COUNT(*) FROM bot_errors WHERE error_type='SL_HIT' AND created_at > datetime('now', '-24 hours')"
            ).fetchone()[0]
            _sc.close()
        except Exception:
            pass
        _size_mult = calc_size_multiplier(_ap["adx"], confirmations * 15, _sl_count_24h)

        return {
            "symbol": symbol,
            "direction": direction,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "tp1": tp1,
            "tp2": tp2,
            "rr": rr,
            "zone": zone_desc,
            "score": confirmations,
            "confirms": confirm_details,
            "timeframe": timeframe,
            "adx": _ap["adx"],
            "volatility_factor": _vf,
            "btc_corr": _btc_corr.get("corr", 0.5),
            "size_mult": _size_mult,
        }

    except Exception as e:
        logging.warning(f"smc_core_check {symbol}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# ══  Market Regime v2 — определение режима рынка               ══
# ═══════════════════════════════════════════════════════════════

def detect_market_regime_v2(symbol: str) -> dict:
    """
    Определяет режим рынка и включает соответствующие стратегии.
    trend → MTF + FAST
    range → SWING + ZONE
    accumulation → WYCKOFF
    """
    try:
        candles = get_candles(symbol, "4h", 50)
        if not candles or len(candles) < 20:
            return {"type": "unknown", "enabled": ["MTF", "ZONE"]}

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

        # Тренд — EMA50 vs EMA20
        ema50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else closes[-1]
        ema20 = sum(closes[-20:]) / 20
        price = closes[-1]

        # Волатильность — ATR vs median ATR
        atr_now = sum(highs[-i] - lows[-i] for i in range(1, 8)) / 7
        atr_med = sum(highs[-i] - lows[-i] for i in range(1, 21)) / 20
        volatility = atr_now > atr_med * 1.2

        # Compression — сужение диапазона
        range_now = max(highs[-5:]) - min(lows[-5:])
        range_prev = max(highs[-20:-5]) - min(lows[-20:-5])
        compression = range_now < range_prev * 0.5

        # Drawdown от пика
        peak = max(highs[-40:]) if len(highs) >= 40 else max(highs)
        drawdown = (peak - price) / peak * 100

        # Логика режимов
        if compression and drawdown >= 5:
            return {
                "type": "accumulation",
                "enabled": ["WYCKOFF", "ZONE"],
            }
        elif (price > ema50 and ema20 > ema50) or (price < ema50 and ema20 < ema50):
            if volatility:
                return {
                    "type": "trend",
                    "enabled": ["MTF", "FAST", "SWING"],
                }
            else:
                return {
                    "type": "trend_slow",
                    "enabled": ["MTF", "ZONE"],
                }
        else:
            return {
                "type": "range",
                "enabled": ["SWING", "ZONE"],
            }

    except Exception:
        return {"type": "unknown", "enabled": ["MTF", "ZONE", "SWING"]}
