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
    )
    _SMC_ENGINE_OK = True
    logging.info("smc_engine.py загружен успешно")
except ImportError as e:
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
except ImportError as e:
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
except ImportError as e:
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
except ImportError as e:
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
except ImportError as e:
    _AUTOPILOT_OK = False
    logging.warning(f"apex_autopilot.py не найден: {e}")
    _autopilot_fast   = lambda: None
    _autopilot_deep   = lambda: None
    _autopilot_on_close = lambda *a, **kw: None
    _autopilot_status = lambda: "Автопилот недоступен"

TOKEN = os.environ.get("TELEGRAM_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0") or 0)
SIGNAL_CHANNEL = int(os.environ.get("SIGNAL_CHANNEL_ID", "-1003122576951"))  # TG канал сигналов
GROQ_KEY = os.environ.get("GROQ_API_KEY")
GROQ_KEYS = [
    os.environ.get("GROQ_API_KEY", ""),
    os.environ.get("GROQ_API_KEY_2", ""),
    os.environ.get("GROQ_API_KEY_3", ""),
]
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
    conn.execute("PRAGMA journal_mode=WAL")   # WAL — параллельные записи без блокировок
    conn.execute("PRAGMA synchronous=NORMAL") # быстрее без потери данных
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
        ("rule_text",       "TEXT"),
        ("source",          "TEXT"),
        ("category",        "TEXT"),
        ("rule",            "TEXT"),
        ("confirmed_by",    "INTEGER DEFAULT 0"),
        ("contradicted_by", "INTEGER DEFAULT 0"),
        ("updated_at",      "TEXT DEFAULT CURRENT_TIMESTAMP"),
        ("active",          "INTEGER DEFAULT 1"),
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
                "INSERT INTO user_memory VALUES (?,?,?,?,?,?,?,?,?,?)",
                (user_id, name, profile or "", preferences or "", coins or "", deposit or 0, 1.0, 0, now, now)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Memory error: {e}")

def save_chat_log(user_id, role, content):
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        conn.execute("INSERT INTO chat_log VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)", (user_id, role, content[:2000]))
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
        "ADAUSDT", "DOTUSDT", "POLUSDT", "LTCUSDT", "ATOMUSDT",
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
    fallback = pairs_cache if pairs_cache else FORCED
    # Гарантируем что возвращаем list, а не None
    return fallback if isinstance(fallback, list) else list(FORCED)

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

        # Стакан обязателен — без него штраф -15
        orderbook_confirmed = any("Биды" in s for s in signals)
        if not orderbook_confirmed:
            score = max(0, score - 15)

        if not signals or score < 72:  # поднят порог с 30 до 72
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

def smart_price_fmt(p) -> str:
    """Умное форматирование цены — правильное кол-во знаков для любой монеты"""
    if p is None or p == 0:
        return "нет данных"
    if p >= 10000:   return f"{p:,.2f}"
    if p >= 1000:    return f"{p:,.2f}"
    if p >= 100:     return f"{p:,.3f}"
    if p >= 10:      return f"{p:,.4f}"
    if p >= 1:       return f"{p:,.4f}"
    if p >= 0.1:     return f"{p:.5f}"
    if p >= 0.01:    return f"{p:.6f}"
    if p >= 0.001:   return f"{p:.7f}"
    if p >= 0.0001:  return f"{p:.8f}"
    return f"{p:.10f}"

def smart_round(p, direction_multiplier=1.0) -> float:
    """Умное округление — сохраняет значимые цифры"""
    if not p or p == 0:
        return p
    if p >= 10:    return round(p, 3)
    if p >= 1:     return round(p, 4)
    if p >= 0.1:   return round(p, 5)
    if p >= 0.01:  return round(p, 6)
    if p >= 0.001: return round(p, 7)
    return round(p, 10)

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

    # 5.5 KuCoin — работает с Render без блокировок
    try:
        base = symbol.replace("USDT", "")
        kc_map = {"1m":"1min","5m":"5min","15m":"15min","30m":"30min",
                  "1h":"1hour","4h":"4hour","1d":"1day","1w":"1week"}
        kc_interval = kc_map.get(interval, "1hour")
        r = requests.get(
            f"https://api.kucoin.com/api/v1/market/candles",
            params={"symbol": f"{base}-USDT", "type": kc_interval},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json().get("data", [])
            if isinstance(data, list) and len(data) > 5:
                # KuCoin: [time, open, close, high, low, volume, turnover]
                candles = [{
                    "open": float(d[1]), "high": float(d[3]),
                    "low": float(d[4]), "close": float(d[2]),
                    "volume": float(d[5])
                } for d in reversed(data[-limit:])]
                if len(candles) >= 20:
                    logging.info(f"Свечи KuCoin: {symbol} {interval} {len(candles)}шт")
                    candle_cache[cache_key] = (candles, time.time())
                    return candles
    except Exception as e:
        logging.debug(f"KuCoin {symbol} {interval}: {e}")

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

    # 6. Brain Router — синтетические свечи + workarounds из brain.db
    if _ROUTER_OK:
        try:
            rc = _brain_router.candles(symbol, interval, limit)
            if rc and len(rc) >= 10:
                logging.info(f"Свечи BrainRouter: {symbol} {interval} {len(rc)}шт")
                candle_cache[cache_key] = (rc, time.time())
                return rc
        except Exception as e:
            logging.debug(f"BrainRouter candles {symbol} {interval}: {e}")

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


# ===== COINGLASS — ЛИКВИДАЦИИ =====
_coinglass_cache = {}
_coinglass_cache_time = 0

def get_liquidations(symbol):
    """Ликвидации с CoinGlass. Много лонг-ликвидаций = рынок очищен для роста."""
    global _coinglass_cache, _coinglass_cache_time
    if time.time() - _coinglass_cache_time < 1800 and symbol in _coinglass_cache:
        return _coinglass_cache[symbol]
    try:
        if not COINGLASS_KEY:
            return None
        base = symbol.replace("USDT", "")
        r = requests.get(
            "https://open-api.coinglass.com/public/v2/liquidation_ex",
            headers={"coinglassSecret": COINGLASS_KEY},
            params={"symbol": base, "interval": "1h"},
            timeout=10
        )
        data = r.json()
        if data.get("code") != "0" or not data.get("data"):
            return None
        item = data["data"][0] if isinstance(data["data"], list) else data["data"]
        long_liq = float(item.get("longLiquidationUsd", 0))
        short_liq = float(item.get("shortLiquidationUsd", 0))
        result = {
            "long_liq_usd": long_liq,
            "short_liq_usd": short_liq,
            "total_usd": long_liq + short_liq,
            "bias": "LONGS_WIPED" if long_liq > short_liq * 1.5 else
                    "SHORTS_WIPED" if short_liq > long_liq * 1.5 else "BALANCED"
        }
        _coinglass_cache[symbol] = result
        _coinglass_cache_time = time.time()
        return result
    except Exception as e:
        logging.debug(f"CoinGlass {symbol}: {e}")
        return None


# ===== SANTIMENT — ON-CHAIN =====
_santiment_cache = {}
_santiment_cache_time = 0

def get_santiment_data(symbol):
    """On-chain sentiment с Santiment."""
    global _santiment_cache, _santiment_cache_time
    if time.time() - _santiment_cache_time < 3600 and symbol in _santiment_cache:
        return _santiment_cache[symbol]
    try:
        if not SANTIMENT_KEY:
            return None
        slug_map = {
            "BTCUSDT":"bitcoin","ETHUSDT":"ethereum","SOLUSDT":"solana",
            "BNBUSDT":"binance-coin","XRPUSDT":"ripple","ADAUSDT":"cardano",
            "AVAXUSDT":"avalanche","DOTUSDT":"polkadot","LINKUSDT":"chainlink",
            "LTCUSDT":"litecoin","ATOMUSDT":"cosmos","NEARUSDT":"near-protocol",
            "INJUSDT":"injective-protocol","SUIUSDT":"sui","ARBUSDT":"arbitrum"
        }
        slug = slug_map.get(symbol)
        if not slug:
            return None
        query = '''{ getMetric(metric: "sentiment_balance_total") {
            timeseriesData(slug: "%s", from: "utc_now-1d", to: "utc_now", interval: "1h") {
                datetime value } } }''' % slug
        r = requests.post(
            "https://api.santiment.net/graphql",
            json={"query": query},
            headers={"Authorization": f"Apikey {SANTIMENT_KEY}"},
            timeout=10
        )
        ts = r.json().get("data",{}).get("getMetric",{}).get("timeseriesData",[])
        values = [x["value"] for x in ts if x.get("value") is not None]
        if not values:
            return None
        avg = sum(values) / len(values)
        result = {
            "sentiment": round(avg, 3),
            "signal": "BULLISH" if avg > 0.1 else "BEARISH" if avg < -0.1 else "NEUTRAL"
        }
        _santiment_cache[symbol] = result
        _santiment_cache_time = time.time()
        return result
    except Exception as e:
        logging.debug(f"Santiment {symbol}: {e}")
        return None


# ===== WHALE ALERT RSS =====
_whale_cache = []
_whale_cache_time = 0

def get_whale_alerts():
    """Крупные переводы на биржи из Whale Alert RSS."""
    global _whale_cache, _whale_cache_time
    if time.time() - _whale_cache_time < 900 and _whale_cache:
        return _whale_cache
    try:
        import re as _re
        r = requests.get("https://whale-alert.io/feed",
                         headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
        items = _re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', r.text)
        alerts = [i for i in items[1:11] if any(
            w in i.lower() for w in ["bitcoin","ethereum","transfer","exchange","moved"])]
        _whale_cache = alerts[:5]
        _whale_cache_time = time.time()
        return _whale_cache
    except Exception as e:
        logging.debug(f"Whale Alert: {e}")
        return []


# ===== BTC КОРРЕЛЯЦИЯ — ФИЛЬТР =====
def get_btc_1h_change():
    """Изменение BTC за последний час."""
    try:
        candles = get_candles("BTCUSDT", "1h", 3)
        if len(candles) < 2:
            return 0.0
        return round((candles[-1]["close"] - candles[-2]["close"]) / candles[-2]["close"] * 100, 3)
    except Exception:
        return 0.0

def btc_allows_signal(direction):
    """Если BTC падает -1.5%+ за час — не даём лонги на альты."""
    btc_change = get_btc_1h_change()
    if direction == "BULLISH" and btc_change < -1.5:
        return False, f"BTC падает {btc_change:.1f}%/ч — лонги опасны"
    if direction == "BEARISH" and btc_change > 1.5:
        return False, f"BTC растёт {btc_change:.1f}%/ч — шорты опасны"
    return True, ""


# ===== СТАРШИЙ ТФ КОНТЕКСТ =====
_htf_cache = {}
_htf_cache_time = {}

def get_higher_tf_context(symbol):
    """Недельный тренд как контекст. Не входим против глобального тренда."""
    if time.time() - _htf_cache_time.get(symbol, 0) < 7200 and symbol in _htf_cache:
        return _htf_cache[symbol]
    try:
        daily = get_candles(symbol, "1d", 14)
        if len(daily) < 7:
            return {"trend": "UNKNOWN", "near_resistance": False, "note": ""}
        closes = [c["close"] for c in daily]
        price_now = closes[-1]
        weekly_change = (price_now - closes[-7]) / closes[-7] * 100
        resistance = max(c["high"] for c in daily)
        support = min(c["low"] for c in daily)
        dist_to_res = (resistance - price_now) / price_now * 100
        dist_to_sup = (price_now - support) / price_now * 100
        trend = "BULLISH" if weekly_change > 3 else "BEARISH" if weekly_change < -3 else "NEUTRAL"
        result = {
            "trend": trend,
            "weekly_change": round(weekly_change, 1),
            "dist_to_resistance": round(dist_to_res, 1),
            "dist_to_support": round(dist_to_sup, 1),
            "near_resistance": dist_to_res < 2.0,
            "near_support": dist_to_sup < 2.0,
            "note": f"Нед: {trend} ({weekly_change:+.1f}%)"
        }
        _htf_cache[symbol] = result
        _htf_cache_time[symbol] = time.time()
        return result
    except Exception as e:
        logging.debug(f"HTF context {symbol}: {e}")
        return {"trend": "UNKNOWN", "near_resistance": False, "note": ""}


# ===== FEAR & GREED ИСТОРИЯ =====
_fg_hist_cache = None
_fg_hist_time = 0

def get_fg_history():
    """F&G за 7 дней — тренд настроения."""
    global _fg_hist_cache, _fg_hist_time
    if time.time() - _fg_hist_time < 3600 and _fg_hist_cache:
        return _fg_hist_cache
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=7&format=json", timeout=8)
        data = r.json().get("data", [])
        if not data:
            return None
        values = [int(d["value"]) for d in data]
        avg7 = sum(values) / len(values)
        result = {
            "values": values, "avg7": round(avg7, 1),
            "trend": "IMPROVING" if values[0] > values[-1] else "WORSENING",
            "current": values[0]
        }
        _fg_hist_cache = result
        _fg_hist_time = time.time()
        return result
    except Exception as e:
        logging.debug(f"F&G history: {e}")
        return None


# ===== DXY SIGNAL =====

dxy_cache = {}
dxy_cache_time = 0

def get_dxy_signal():
    global dxy_cache, dxy_cache_time
    if time.time() - dxy_cache_time < 3600 and dxy_cache:
        return dxy_cache
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=5d",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json()
        # Безопасный доступ — Yahoo иногда возвращает null в result
        results = (data.get("chart") or {}).get("result") or []
        if not results:
            logging.warning("DXY: пустой ответ от Yahoo Finance")
            return None
        quote = (results[0].get("indicators") or {}).get("quote") or [{}]
        closes_raw = (quote[0] if quote else {}).get("close") or []
        closes = [c for c in closes_raw if c is not None]
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
    except Exception as e:
        logging.warning(f"DXY: {e}")
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        # Защита: если regime вернулся строкой или None — приводим к dict
        if not isinstance(regime, dict):
            regime = {"mode": str(regime) if regime else "UNKNOWN",
                      "direction": "NONE", "confidence": 0}
        if regime["mode"] == "SIDEWAYS" and regime["confidence"] > 85:
            return None

        # ── 0.5. Groq читает свои правила перед сигналом ──
        avoid_rules = get_self_rules("avoid")
        for rule_row in avoid_rules:
            try:
                rule_text = str(rule_row[0] if len(rule_row) == 1 else rule_row[1] or "")
                conf = float(rule_row[1] if len(rule_row) >= 2 else 0.5)
            except Exception:
                continue
            if symbol in rule_text and conf >= 0.75:
                logging.info(f"full_scan {symbol}: пропущен по правилу самообучения")
                return None

        # Читаем лучшие правила по этому символу для контекста Groq
        try:
            with sqlite3.connect("brain.db", timeout=30, check_same_thread=False) as _rc:
                _sym_rules = _rc.execute(
                    "SELECT rule_text, confidence FROM self_rules WHERE (rule_text LIKE ? OR category LIKE ?) AND active=1 ORDER BY confidence DESC LIMIT 5",
                    (f"%{symbol}%", f"%{symbol}%")
                ).fetchall()
                _groq_symbol_context = "; ".join([r[0][:80] for r in _sym_rules if r[0]]) if _sym_rules else ""
        except Exception:
            _groq_symbol_context = ""

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

        # ── 2.5. Новые источники данных ──
        liquidations = get_liquidations(symbol)
        santiment = get_santiment_data(symbol)
        htf = get_higher_tf_context(symbol)
        fg_hist = get_fg_history()

        # BTC корреляция — если BTC против нас, пропускаем
        btc_ok, btc_reason = btc_allows_signal(direction)
        if not btc_ok and symbol != "BTCUSDT":
            logging.info(f"[BTC Filter] {symbol} пропущен: {btc_reason}")
            return None

        # Старший ТФ — не входим в лонг у сопротивления
        if htf.get("near_resistance") and direction == "BULLISH":
            logging.info(f"[HTF Filter] {symbol} у сопротивления ({htf['dist_to_resistance']:.1f}% до него) — лонг пропущен")
            return None
        if htf.get("near_support") and direction == "BEARISH":
            logging.info(f"[HTF Filter] {symbol} у поддержки — шорт пропущен")
            return None

        # Мёртвая зона (22:00-02:00 UTC) — снижаем агрессивность
        from datetime import datetime as _dt
        _hour = _dt.utcnow().hour
        _dead_zone = (22 <= _hour or _hour <= 1)

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

        # CoinGlass ликвидации
        if liquidations:
            if direction == "BULLISH" and liquidations["bias"] == "LONGS_WIPED":
                liq_m = liquidations["long_liq_usd"] / 1_000_000
                confluence.append(f"✅ Лонги ликвидированы ${liq_m:.1f}M — рынок очищен (+10)")
                total_weight += 10
            elif direction == "BEARISH" and liquidations["bias"] == "SHORTS_WIPED":
                liq_m = liquidations["short_liq_usd"] / 1_000_000
                confluence.append(f"✅ Шорты ликвидированы ${liq_m:.1f}M — рынок очищен (+10)")
                total_weight += 10
            elif liquidations["total_usd"] > 5_000_000:
                confluence.append(f"⚠️ Высокие ликвидации ${liquidations['total_usd']/1e6:.1f}M — волатильность")

        # Santiment on-chain sentiment
        if santiment and santiment["signal"] == direction[:7]:
            confluence.append(f"✅ Santiment: {santiment['sentiment']:+.3f} — on-chain подтверждает (+8)")
            total_weight += 8
        elif santiment and santiment["signal"] not in ("NEUTRAL", direction[:7]):
            confluence.append(f"⚠️ Santiment противоречит ({santiment['signal']})")
            total_weight -= 5

        # Старший ТФ контекст
        if htf["trend"] != "UNKNOWN":
            if htf["trend"] == direction[:7] if direction != "BEARISH" else htf["trend"] == "BEARISH":
                confluence.append(f"✅ {htf['note']} — глобальный тренд совпадает (+8)")
                total_weight += 8
            elif htf["trend"] == "NEUTRAL":
                pass  # нейтрально
            else:
                confluence.append(f"⚠️ {htf['note']} — против глобального тренда (-8)")
                total_weight -= 8

        # F&G тренд
        if fg_hist:
            if fg_hist["trend"] == "IMPROVING" and direction == "BULLISH":
                confluence.append(f"✅ F&G улучшается {fg_hist['avg7']:.0f}→{fg_hist['current']} (+5)")
                total_weight += 5

        # Мёртвая зона — штраф
        if _dead_zone:
            confluence.append(f"⚠️ Мёртвая зона (UTC {_hour}:xx) — ликвидность низкая (-10)")
            total_weight -= 10

        # Supply/Demand зоны
        sd_zone = find_supply_demand(candles, direction)
        if sd_zone:
            strength_label = "сильная" if sd_zone["strength"] == "STRONG" else "умеренная"
            confluence.append(f"✅ {sd_zone['type']} зона {sd_zone['bottom']:.4f}–{sd_zone['top']:.4f} ({strength_label}) (+12)")
            total_weight += 12 if sd_zone["strength"] == "STRONG" else 7

        # Wyckoff фаза
        wyckoff = detect_wyckoff_phase(candles)
        if wyckoff["phase"] == "ACCUMULATION" and direction == "BULLISH":
            confluence.append(f"✅ Wyckoff ACCUMULATION — готовится памп (+12)")
            total_weight += 12
        elif wyckoff["phase"] == "MARKUP" and direction == "BULLISH":
            confluence.append(f"✅ Wyckoff MARKUP — тренд активен (+8)")
            total_weight += 8
        elif wyckoff["phase"] == "DISTRIBUTION" and direction == "BEARISH":
            confluence.append(f"✅ Wyckoff DISTRIBUTION — готовится дамп (+12)")
            total_weight += 12
        elif wyckoff["phase"] == "MARKDOWN" and direction == "BEARISH":
            confluence.append(f"✅ Wyckoff MARKDOWN — падение продолжается (+8)")
            total_weight += 8

        # Многомонетная корреляция
        try:
            corr = check_multi_coin_correlation(symbol, direction, get_candles) or {}
        except Exception:
            corr = {}
        if corr.get("strong", False):
            confluence.append(f"✅ {corr.get('confirmed',0)}/{corr.get('total',0)} коррелированных монет подтверждают (+{corr.get('score',0)})")
            total_weight += corr.get("score", 0)

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

        # ── Extension бустеры confluence ──
        if _EXT_OK:
            try:
                ext_bonus, ext_descs = _ext_run_boosters(candles, direction)
                if ext_bonus > 0:
                    total_weight += ext_bonus
                    confluence.extend(ext_descs)
            except Exception as _e:
                logging.debug(f"ext boosters: {_e}")

        # Минимальный порог — учитывает серию потерь
        min_weight = max(streak_threshold, 18 if mtf["match_count"] >= 3 else 22)
        if total_weight < min_weight:
            return None

        # ── Extension фильтры сигналов ──
        if _EXT_OK:
            try:
                passed, block_reason = _ext_run_filters(
                    symbol, direction, price, total_weight,
                    regime.get("mode", "") if isinstance(regime, dict) else str(regime),
                    fg
                )
                if not passed:
                    logging.info(f"full_scan {symbol}: заблокирован extension фильтром — {block_reason}")
                    return None
            except Exception as _e:
                logging.debug(f"ext filters: {_e}")

        # ── 4. Уровни входа — с проверкой цены ──
        # Если цена из свечей = 0, пробуем взять из живых цен
        if not price or price == 0:
            live = get_live_prices()
            price = live.get(symbol, {}).get("price", 0)
        if not price or price == 0:
            logging.warning(f"full_scan {symbol}: цена = 0, пропускаем")
            return None

        risk = price * 0.015
        if direction == "BULLISH":
            entry = ob["top"] if ob else price
            sl = smart_round(entry - risk)
            tp1 = smart_round(entry + risk * 2)
            tp2 = smart_round(entry + risk * 3)
            tp3 = smart_round(entry + risk * 5)
        else:
            entry = ob["bottom"] if ob else price
            sl = smart_round(entry + risk)
            tp1 = smart_round(entry - risk * 2)
            tp2 = smart_round(entry - risk * 3)
            tp3 = smart_round(entry - risk * 5)

        # Ещё раз проверяем — если entry=0, сигнал невалидный
        if not entry or entry == 0:
            logging.warning(f"full_scan {symbol}: entry = 0 после расчёта, пропускаем")
            return None

        # ── 5. Время отработки ──
        est_hours, confidence_str, win_rate = get_estimated_time(symbol, timeframe)
        time_str = f"~{est_hours}ч" if est_hours < 24 else f"~{est_hours//24}дн"
        wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"

        save_signal_db(symbol, direction, "MTF", entry, tp1, tp2, tp3, sl, timeframe, est_hours, mtf["grade"],
                       confluence=total_weight, regime=regime.get("mode","UNKNOWN") if isinstance(regime, dict) else str(regime))

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

        # ── 6.6 Brain Router контекст — OI, Funding, сессия, сезонность, накопление ──
        router_ctx = ""
        router_contradiction = ""
        router_accum = ""
        try:
            if _ROUTER_OK:
                # Полный контекст сигнала
                router_ctx = _brain_router.signal_context(
                    symbol, direction, timeframe,
                    total_weight,
                    regime.get("mode","UNKNOWN") if isinstance(regime, dict) else str(regime),
                    mtf.get("grade","?")
                )
                # Детектор противоречий
                fg_val = fg.get("value", 50) if isinstance(fg, dict) else 50
                funding_sig = _brain_router.funding(symbol).get("signal","NEUTRAL")
                contra = _brain_router.contradictions(
                    symbol, direction, fg_val, funding_sig, "NEUTRAL", mtf.get("grade","?")
                )
                if contra["conflicts"] or contra["warnings"]:
                    router_contradiction = contra["verdict"] + "\n" + "\n".join(
                        contra["conflicts"] + contra["warnings"]
                    )
                # Накопление только для топовых сигналов
                if mtf.get("grade","") in ("МЕГА ТОП", "ТОП СДЕЛКА"):
                    accum = _brain_router.accumulation(symbol)
                    if accum["score"] >= 72:
                        router_accum = f"📦 Wyckoff {accum['phase']}: {accum['score']}/100"
        except Exception as _re:
            logging.debug(f"[Router] signal context error: {_re}")

        # ── 7. Уровень инвалидации (когда сигнал отменяется) ──
        invalidation = sl  # Если цена закроется за стопом — сигнал недействителен
        inv_text = f"Сигнал отменяется если цена закроется {'ниже' if direction == 'BULLISH' else 'выше'} <code>{smart_price_fmt(invalidation)}</code>"

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
            f"💰 <b>Вход:</b> <code>{smart_price_fmt(entry)}</code>\n"
            f"🛑 <b>Стоп:</b> <code>{smart_price_fmt(sl)}</code>\n"
            f"🎯 <b>TP1:</b> <code>{smart_price_fmt(tp1)}</code> (+2R)\n"
            f"🎯 <b>TP2:</b> <code>{smart_price_fmt(tp2)}</code> (+3R)\n"
            f"🎯 <b>TP3:</b> <code>{smart_price_fmt(tp3)}</code> (+5R)\n\n"
            f"⏱ <b>Время отработки:</b> {time_str}\n"
            f"📊 <b>Точность:</b> {wr_str} | {confidence_str}\n"
            f"🧠 <b>Режим рынка:</b> {regime.get('mode','?') if isinstance(regime,dict) else regime} ({regime.get('direction','') if isinstance(regime,dict) else ''})\n"
            f"{econ_warn}"
            f"❌ <b>Инвалидация:</b> {inv_text}\n\n"
            f"📋 <b>Confluence [{total_weight}/100]:</b>\n{conf_text}\n"
            f"{hist_text}\n"
            f"💬 <b>APEX думает:</b>\n<i>{signal_comment}</i>\n"
            + (f"\n🤖 <b>Groq:</b> <i>{groq_insight}</i>\n" if groq_insight else "")
            + (f"\n🐋 <b>Киты:</b> <i>{whale_desc}</i>\n" if whale_desc else "")
            + (f"\n{router_accum}\n" if router_accum else "")
            + (f"\n📡 <b>Роутер:</b>\n<i>{router_ctx}</i>\n" if router_ctx else "")
            + (f"\n⚠️ <b>Конфликт:</b>\n<i>{router_contradiction}</i>\n" if router_contradiction else "")
            + f"{'━'*26}"
        )
    except Exception as e:
        logging.error(f"Scan error {symbol}: {e}")
        return None


def save_signal_db(symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, timeframe, est_hours, grade, confluence=0, regime="UNKNOWN"):
    """Сохраняем сигнал в обе таблицы: signals (трекинг) + signal_log (обучение)"""
    learning_id = None
    try:
        # Сохраняем в learning.py signal_log для Groq-анализа
        if _LEARNING_OK:
            learning_id = _learn_save_signal(
                symbol, direction, grade, entry, sl, tp1, tp2, tp3,
                timeframe, confluence, regime, signal_type
            )
    except Exception as e:
        logging.warning(f"save_signal learning: {e}")

    try:
        # Используем безопасное подключение с emergency patch
        from emergency_fix import safe_db_connection
        
        with safe_db_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO signals
                (symbol, direction, signal_type, entry, tp1, tp2, tp3, sl,
                 timeframe, estimated_hours, grade, result, created_at, closed_at, 
                 learning_id, confluence, regime)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 
                        CURRENT_TIMESTAMP, NULL, ?, ?, ?)
            """, (symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, 
                  timeframe, est_hours, grade, learning_id, confluence, regime))
            
            sig_id = cursor.lastrowid
            logging.info(f"Signal saved: {symbol} {direction} (ID: {sig_id})")
            return sig_id, learning_id
            
    except Exception as e:
        logging.error(f"Save signal error: {e}")
        return None, learning_id

# ===== САМООБУЧЕНИЕ =====

def get_estimated_time(symbol, timeframe):
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
                conn2 = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
                conn2.execute(
                    "UPDATE signals SET result=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                    (result, sig_id)
                )
                # Сохраняем learning_id если есть
                learning_id = conn2.execute(
                    "SELECT learning_id FROM signals WHERE id=?", (sig_id,)
                ).fetchone()
                conn2.commit()
                conn2.close()

                is_win = result in ("tp1", "tp2", "tp3")
                update_signal_learning(symbol, hours_elapsed, is_win, timeframe, result)

                # Закрываем сигнал в learning.py — это запускает Groq анализ автоматически
                if _LEARNING_OK:
                    l_id = learning_id[0] if learning_id and learning_id[0] else None
                    if l_id:
                        # close_signal теперь сам вызывает analyze_closed_trade
                        import threading
                        threading.Thread(target=_learn_close_signal, args=(l_id, result, hit_tp or 0), daemon=True).start()
                    else:
                        # Нет learning_id — анализируем напрямую через поиск по символу
                        threading.Thread(target=_learn_analyze_by_symbol, args=(symbol, direction, entry, result, hours_elapsed, timeframe), daemon=True).start()

                # Получаем confluence и regime из БД для этого сигнала
                try:
                    _row_extra = sqlite3.connect("brain.db", timeout=30, check_same_thread=False).execute(
                        "SELECT confluence, regime FROM signals WHERE id=?", (sig_id,)
                    ).fetchone()
                    _confluence_val = _row_extra[0] if _row_extra and _row_extra[0] else 0
                    _regime_val = _row_extra[1] if _row_extra and _row_extra[1] else "UNKNOWN"
                except Exception:
                    _confluence_val, _regime_val = 0, "UNKNOWN"

                # Brain Router — обучаем на результате (часы входа, урок, правило)
                if _ROUTER_OK:
                    try:
                        threading.Thread(
                            target=_brain_router.learn,
                            args=(symbol, direction, grade, timeframe, result,
                                  _confluence_val, entry, sl, tp1, _regime_val),
                            daemon=True
                        ).start()
                    except Exception as _re:
                        logging.debug(f"[Router] learn on close: {_re}")

                # Автопилот — глубокий разбор + автофикс при потере
                if _AUTOPILOT_OK:
                    l_id_for_ap = learning_id[0] if learning_id and learning_id[0] else 0
                    _autopilot_on_close(
                        signal_id=l_id_for_ap,
                        symbol=symbol,
                        direction=direction,
                        result=result,
                        hours_open=hours_elapsed,
                        confluence=_confluence_val
                    )

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

def _learn_analyze_by_symbol(symbol, direction, entry, result, hours, timeframe):
    """Fallback: сохраняем в signal_log и анализируем если нет learning_id"""
    try:
        if not _LEARNING_OK:
            return
        # Ищем последний сигнал этой монеты в signal_log без результата
        import sqlite3 as _sq
        conn = _sq.connect("brain.db")
        row = conn.execute(
            "SELECT id FROM signal_log WHERE symbol=? AND result='PENDING' ORDER BY id DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        conn.close()
        if row:
            _learn_close_signal(row[0], result)
        else:
            # Создаём запись для анализа
            l_id = _learn_save_signal(symbol, direction, "AUTO", entry, entry*0.98, entry*1.02, entry*1.03, entry*1.04, timeframe, 0, "UNKNOWN", "auto")
            if l_id:
                _learn_close_signal(l_id, result)
    except Exception as e:
        logging.warning(f"_learn_analyze_by_symbol: {e}")


def update_signal_learning(symbol, hours_to_close, is_win, timeframe, result):
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
                conn2 = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        conn.execute("INSERT INTO news_cache VALUES (NULL,?,?,CURRENT_TIMESTAMP)", (query, content[:1000]))
        conn.commit()
        conn.close()
    except:
        pass

def get_recent_news():
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        rows = conn.execute("SELECT query, content FROM news_cache ORDER BY created_at DESC LIMIT 3").fetchall()
        conn.close()
        return "\n\n".join([f"{r[0]}: {r[1]}" for r in rows])
    except:
        return ""

def save_knowledge(topic, content, source="auto"):
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        conn.execute("INSERT INTO knowledge VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)", (topic, content, source))
        conn.commit()
        conn.close()
    except:
        pass

def get_knowledge(topic):
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
                "INSERT OR IGNORE INTO self_rules (category, rule, rule_type, rule_text, confidence, source, active) VALUES (?, ?, ?, ?, ?, ?, 1)",
                (category, rule, "auto", rule, confidence, source)
            )
            log_brain_event("rule_added", f"{category}: {rule[:80]}", f"confidence={confidence}")

        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"save_self_rule: {e}")


def weaken_rule(rule_text):
    """Ослабить правило если оно дало неправильный результат"""
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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

        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)

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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
            regime_mode = regime.get("mode", str(regime)) if isinstance(regime, dict) else str(regime)
            factors.append(f"рынок в режиме {regime_mode}")

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
    - Ротация ключей для обхода лимитов
    """
    global _last_ai_call, _groq_key_index
    # Глобальный кулдаун — не спамим Groq
    elapsed = time.time() - _last_ai_call
    if elapsed < AI_COOLDOWN:
        time.sleep(AI_COOLDOWN - elapsed)
    _last_ai_call = time.time()

    # Сокращаем промпт если больше 6000 символов — экономим токены
    if len(prompt) > 6000:
        prompt = prompt[:5000] + "\n[промпт сокращён для экономии токенов]"

    models = [
        "llama-3.1-8b-instant",        # основная — 500k TPD/день
        "llama-3.1-8b-instant",        # fallback — отдельный лимит
        "llama-3.1-8b-instant",        # резерв тяжёлый
    ]

    for attempt in range(len(GROQ_KEYS) * 3):
        model = models[min(attempt % 3, len(models) - 1)]
        key_index = (attempt // 3) % len(GROQ_KEYS)
        key = GROQ_KEYS[key_index]
        if not key:
            continue
        try:
            client = Groq(api_key=key)
            r = client.chat.completions.create(
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
                logging.warning(f"Groq rate limit (ключ {key_index+1}), пробую следующий...")
                time.sleep(2)
                continue
            elif "model" in err_str and "not found" in err_str:
                logging.warning(f"Модель {model} недоступна, пробую следующую")
                continue
            else:
                logging.error(f"Groq error (ключ {key_index+1}, попытка {attempt+1}): {e}")
                if attempt < len(GROQ_KEYS) * 3 - 1:
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

# Глобальный кулдаун между вызовами Groq — защита от 429
AI_COOLDOWN = 20   # секунд между вызовами
_last_ai_call = 0

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
            # Фильтруем только dict элементы (защита от строк в ответе)
            news = [n for n in news if isinstance(n, dict)]
            relevant = [n for n in news if any(w in (n.get("title","") + n.get("body","")).lower() for w in words)]
            to_use = relevant[:3] if relevant else news[:3]
            for n in to_use:
                title = n.get("title", "")
                body  = n.get("body", "")[:400]
                src   = n.get("source_info", {}).get("name", "CryptoCompare") if isinstance(n.get("source_info"), dict) else "CryptoCompare"
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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

        conn2 = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        saved = 0

        def _to_str(v):
            """Нормализует любое значение (str/dict/list) в строку"""
            if isinstance(v, dict):
                return str(v.get("text") or v.get("rule") or v.get("description") or v.get("name") or list(v.values())[0])
            elif isinstance(v, list):
                return " ".join(str(x) for x in v)
            return str(v)

        # Сохраняем пробелы как знания
        gaps_raw = analysis.get("gaps", [])
        if not isinstance(gaps_raw, list):
            gaps_raw = [gaps_raw] if gaps_raw else []
        for gap in gaps_raw[:5]:
            try:
                save_knowledge("gap", _to_str(gap)[:200], "self-diagnosis")
            except Exception:
                pass

        # Бесплатные API которые стоит добавить — записываем в мозг
        apis_raw = analysis.get("free_apis", [])
        if not isinstance(apis_raw, list):
            apis_raw = [apis_raw] if apis_raw else []
        for api in apis_raw[:3]:
            try:
                api_str = _to_str(api)[:200]
                save_knowledge("suggested_api", api_str, "self-diagnosis")
                logging.info(f"[SelfGrow] Предложен API: {api_str[:80]}")
            except Exception:
                pass

        # Торговые правила — добавляем в self_rules
        rules_raw = analysis.get("rules", [])
        if not isinstance(rules_raw, list):
            rules_raw = [rules_raw] if rules_raw else []
        for rule_raw in rules_raw[:5]:
            try:
                rule = _to_str(rule_raw)[:200].strip()
                if not rule:
                    continue
                ex = conn2.execute("SELECT id FROM self_rules WHERE rule=?", (rule,)).fetchone()
                if not ex:
                    conn2.execute(
                        "INSERT OR IGNORE INTO self_rules (category, rule, rule_type, rule_text, confidence, source, active) VALUES (?, ?, ?, ?, ?, ?, 1)",
                        ("self_improve", rule, "auto", rule, 0.65, "self-diagnosis")
                    )
                    saved += 1
            except Exception as _re:
                logging.debug(f"self_rules insert: {_re}")

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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
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
    """Свечи с CryptoCompare — поддерживает все монеты включая SHIB/XLM/WLD/BONK"""
    try:
        base = symbol.replace("USDT", "").replace("BUSD", "")
        endpoint_map = {
            "1m": "histominute", "3m": "histominute", "5m": "histominute",
            "15m": "histominute", "30m": "histominute",
            "1h": "histohour", "2h": "histohour", "4h": "histohour",
            "1d": "histoday", "3d": "histoday", "1w": "histoday", "1M": "histoday"
        }
        endpoint = endpoint_map.get(interval, "histohour")
        aggregate_map = {
            "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
            "1h": 1, "2h": 2, "4h": 4,
            "1d": 1, "3d": 3, "1w": 7, "1M": 30
        }
        aggregate = aggregate_map.get(interval, 1)
        cc_limit = min(limit + 20, 2000)
        r = requests.get(
            f"https://min-api.cryptocompare.com/data/{endpoint}",
            params={"fsym": base, "tsym": "USD", "limit": cc_limit, "aggregate": aggregate},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=12
        )
        raw = r.json()
        data = raw.get("Data", [])
        # Новый формат API v2
        if isinstance(data, dict):
            data = data.get("Data", [])
        if not data or len(data) < 5:
            return []
        candles = [{
            "open": float(c["open"]), "high": float(c["high"]),
            "low": float(c["low"]), "close": float(c["close"]),
            "volume": float(c.get("volumeto") or c.get("volumefrom") or 0)
        } for c in data if c.get("close") and float(c.get("close", 0)) > 0]
        candles = candles[-limit:]
        if len(candles) >= 10:
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
