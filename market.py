# APEX_STRATEGY_STATS_V1
from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail
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
from core.pair_universe import (
    DEFAULT_UNIVERSE_SIZE,
    FALLBACK_COMMON_PAIRS,
    select_gate_pairs,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Внешние модули APEX ──────────────────────────────────────
import sys as _sys, os as _os_path
# Добавляем папку core/ в путь поиска модулей — файлы могут лежать там
_BASE_DIR = _os_path.path.dirname(_os_path.path.abspath(__file__))
for _p in [_os_path.path.join(_BASE_DIR, "core"), _BASE_DIR]:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

from market_structure import (
    analyze_market_structure as _analyze_market_structure,
    classify_swings as _classify_structure_swings,
    events_with_trend_fallback as _structure_events,
    find_swings as _find_structure_swings,
)

try:
    from session_clock import fast_session as _fast_session
except Exception as _session_clock_error:
    logging.error("session_clock unavailable; FAST scanner will stay disabled: %s", _session_clock_error)
    _fast_session = lambda *_args, **_kwargs: None

try:
    from signal_lifecycle import (
        ACTIVE as _LIFECYCLE_ACTIVE,
        CANCELLED as _LIFECYCLE_CANCELLED,
        WAITING_ENTRY as _LIFECYCLE_WAITING,
        activated_at_for as _lifecycle_activated_at_for,
        barrier_hits as _lifecycle_barrier_hits,
        entry_touched as _lifecycle_entry_touched,
        mark_active as _lifecycle_mark_active,
        mark_finished as _lifecycle_mark_finished,
        register_waiting as _lifecycle_register_waiting,
        state_for as _lifecycle_state_for,
        touch as _lifecycle_touch,
    )
    _SIGNAL_LIFECYCLE_OK = True
except Exception as _lifecycle_import_error:
    _SIGNAL_LIFECYCLE_OK = False
    _LIFECYCLE_ACTIVE = "active"
    _LIFECYCLE_CANCELLED = "cancelled"
    _LIFECYCLE_WAITING = "waiting_entry"
    _lifecycle_activated_at_for = lambda *_args, **_kwargs: None
    logging.error("signal_lifecycle unavailable: %s", _lifecycle_import_error)

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

# Durable market memory — facts from historical signal snapshots.
try:
    from market_memory import (
        capture_snapshot as _memory_capture_snapshot,
        close_snapshot as _memory_close_snapshot,
        record_price as _memory_record_price,
    )
    _MARKET_MEMORY_OK = True
except Exception as _memory_import_error:
    _MARKET_MEMORY_OK = False
    _memory_capture_snapshot = lambda *args, **kwargs: None
    _memory_close_snapshot = lambda *args, **kwargs: None
    _memory_record_price = lambda *args, **kwargs: None
    logging.warning("market_memory unavailable: %s", _memory_import_error)

try:
    from outcome_learning import close_learning_loop as _close_learning_loop
    _OUTCOME_LOOP_OK=True
except Exception as _outcome_loop_import_error:
    _close_learning_loop=lambda *args,**kwargs:None;_OUTCOME_LOOP_OK=False
    logging.warning("outcome_learning unavailable: %s",_outcome_loop_import_error)

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
from core.groq_models import configured_groq_models, is_model_unavailable_error
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
            model=configured_groq_models()[0],
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


def get_confirmed_candles(candles: list) -> list:
    """Return immutable, closed candles from an exchange candle response.

    Providers normally return the currently forming candle as the last row.
    Strategies may use that row's close as an indicative live price, but BOS,
    CHoCH, sweeps, displacement and important zones must be calculated from
    completed bars only.
    """
    if not candles or len(candles) < 2:
        return []
    return candles[:-1]


def ema_value(values: list, period: int) -> float | None:
    """Return a standard exponentially weighted moving average value."""
    if not values or period <= 0 or len(values) < period:
        return None
    value = sum(values[:period]) / period
    alpha = 2.0 / (period + 1.0)
    for price in values[period:]:
        value = float(price) * alpha + value * (1.0 - alpha)
    return value


def average_true_range(candles: list, period: int = 14) -> float | None:
    """Return ATR from completed candles without inventing a price level."""
    if not candles or period <= 0 or len(candles) < period + 1:
        return None
    true_ranges = []
    for index in range(1, len(candles)):
        candle = candles[index]
        previous_close = float(candles[index - 1]["close"])
        true_ranges.append(max(
            float(candle["high"]) - float(candle["low"]),
            abs(float(candle["high"]) - previous_close),
            abs(float(candle["low"]) - previous_close),
        ))
    return sum(true_ranges[-period:]) / period


def select_structural_targets(
    entry: float,
    sl: float,
    candidates: list,
    direction: str,
    min_rr: float,
    max_rr: float,
) -> tuple[float | None, float | None]:
    """Select TP1/TP2 only from supplied market-anchored levels.

    The function never creates a percentage target.  TP1 is the nearest level
    whose reward/risk is inside the strategy envelope; TP2 is the next supplied
    level beyond TP1 (if one exists).
    """
    risk = abs(float(entry) - float(sl))
    if risk <= 0:
        return None, None

    levels = []
    for value in candidates:
        try:
            level = float(value)
        except (TypeError, ValueError):
            continue
        if level <= 0:
            continue
        if direction == "BULLISH" and level <= entry:
            continue
        if direction == "BEARISH" and level >= entry:
            continue
        if not any(abs(level - existing) <= max(abs(level), 1.0) * 1e-9 for existing in levels):
            levels.append(level)

    levels.sort(key=lambda level: abs(level - entry))
    tp1_index = None
    for index, level in enumerate(levels):
        rr = abs(level - entry) / risk
        if min_rr <= rr <= max_rr:
            tp1_index = index
            break
    if tp1_index is None:
        return None, None

    tp1 = levels[tp1_index]
    tp2 = levels[tp1_index + 1] if tp1_index + 1 < len(levels) else None
    return smart_round(tp1), smart_round(tp2) if tp2 is not None else None



def get_liquidation_ratio(symbol: str) -> dict:
    """Gate liquidation dominance; no Binance market-data request."""
    try:
        from external_sources.pair_registry import get_pair
        contract = str(get_pair(symbol).get("gate_symbol") or symbol.replace("USDT", "_USDT"))
        r = requests.get(
            "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
            params={"contract": contract, "interval": "1h", "limit": 3},
            headers={"User-Agent": "APEX-SMC/1.0"},
            timeout=8
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                latest = data[-1]
                long_liq = abs(float(latest.get("long_liq_size") or 0))
                short_liq = abs(float(latest.get("short_liq_size") or 0))
                total = long_liq + short_liq
                long_pct = long_liq / total if total else 0.5
                short_pct = short_liq / total if total else 0.5
                ratio = long_liq / short_liq if short_liq > 0 else (999.0 if long_liq else 1.0)
                if ratio > 1.5:
                    signal = "BEARISH"
                    desc = f"Gate long liquidations dominate ({ratio:.2f}x)"
                elif ratio < 0.67:
                    signal = "BULLISH"
                    desc = f"Gate short liquidations dominate ({(1 / ratio) if ratio else 999:.2f}x)"
                else:
                    signal = "NEUTRAL"
                    desc = f"Gate liquidations balanced ({ratio:.2f}x)"
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

def get_top_pairs(limit=DEFAULT_UNIVERSE_SIZE):
    """Liquid Gate USD-M analysis universe, refreshed once per hour."""
    global pairs_cache, pairs_cache_time
    if time.time() - pairs_cache_time < 3600 and pairs_cache:
        return pairs_cache[:limit]
    target = max(1, min(int(limit), DEFAULT_UNIVERSE_SIZE))
    try:
        gate = requests.get(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers", timeout=7,
            headers={"User-Agent": "APEX-SMC/1.0"},
        )
        gate.raise_for_status()
        discovered = select_gate_pairs(gate.json(), limit=DEFAULT_UNIVERSE_SIZE)
        if discovered:
            pairs_cache = discovered
            logging.info("[PairUniverse] %s liquid Gate perpetuals", len(pairs_cache))
        else:
            raise RuntimeError("no eligible Gate perpetuals")
    except Exception as exc:
        pairs_cache = list(dict.fromkeys(FALLBACK_COMMON_PAIRS))[:DEFAULT_UNIVERSE_SIZE]
        logging.warning("[PairUniverse] live refresh unavailable, using %s-pair fallback: %s", len(pairs_cache), exc)
    pairs_cache_time = time.time()
    return pairs_cache[:target]


def get_live_prices():
    """Gate USD-M prices used by scanners and signal-integrity checks."""
    global price_cache, last_price_update
    if time.time() - last_price_update < 20 and price_cache:
        return price_cache
    try:
        r = requests.get(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers",
            headers={"User-Agent": "APEX-SMC/1.0"},
            timeout=10
        )
        r.raise_for_status()
        tickers = r.json()
        if isinstance(tickers, list) and tickers:
            market = {}
            for t in tickers:
                sym = str(t.get("contract", "")).replace("_", "").upper()
                if sym.endswith("USDT"):
                    try:
                        price = float(t.get("last") or t.get("mark_price") or 0)
                        change = float(t.get("change_percentage") or 0)
                        vol = float(t.get("volume_24h_quote") or t.get("volume_24h_settle") or 0)
                        if price > 0:
                            market[sym] = {"price": price, "change": round(change, 2), "volume": vol}
                    except Exception:
                        pass
            if market:
                price_cache = market
                last_price_update = time.time()
                logging.info("Цены: Gate Futures (%s пар)", len(market))
                return price_cache
    except Exception as e:
        logging.warning("Gate Futures prices unavailable: %s", e)

    logging.error("Gate Futures prices unavailable; using last cache")
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
        range_pct = (high_max - low_min) / low_min * 100 if low_min > 0 else 0

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
        avg_recent = sum(recent_vols) / len(recent_vols) if recent_vols else 0

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

        if not signals or score < 55:  # поднят порог с 30 до 72
            return None

        # ── Groq считает цель роста индивидуально ──
        pump_target = None
        pump_target_pct = None
        pump_logic = ""
        try:
            atr_1h = sum(c["high"] - c["low"] for c in candles_1h[-14:]) / 14
            recent_highs_str = ", ".join([str(round(c["high"], 6)) for c in candles_1h[-12:]])
            recent_lows_str  = ", ".join([str(round(c["low"],  6)) for c in candles_1h[-12:]])
            groq_prompt = (
                f"Ты трейдер SMC. Анализируй накопление после боковика и дай реальную цель памп минимум +10% от цены. Ответь СТРОГО JSON:\\n"
                f'{{\"target\": число_цены, \"target_pct\": процент_роста_число, \"logic\": \"причина макс 10 слов\"}}\\n\\n'
                f"Пара: {symbol}\\n"
                f"Цена сейчас: {price_now}\\n"
                f"Диапазон боковика: {round(low_min,6)} — {round(high_max,6)} ({range_pct:.1f}%)\\n"
                f"ATR: {round(atr_1h,6)}\\n"
                f"BB ширина: {bb_width:.1f}%\\n"
                f"Объём ratio: {vol_ratio:.2f}\\n"
                f"Максимумы 12ч: {recent_highs_str}\\n"
                f"Минимумы 12ч: {recent_lows_str}\\n"
                f"Признаки: {'; '.join(signals)}"
            )
            groq_resp = ask_groq(groq_prompt, max_tokens=100)
            if groq_resp and len(groq_resp) > 5:
                import json as _json, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                json_match = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if json_match:
                    clean = json_match.group()
                parsed = _json.loads(clean)
                if parsed.get("target") and float(parsed["target"]) > price_now:
                    pump_target = float(parsed["target"])
                    pump_target_pct = float(parsed.get("target_pct", round((pump_target - price_now) / price_now * 100, 1)))
                if parsed.get("logic"):
                    pump_logic = str(parsed["logic"]).strip()
        except Exception as ge:
            logging.debug(f"[AccumGroq] {symbol}: {ge}")
            # Fallback: верхняя граница боковика + ATR*2
            try:
                atr_fb = sum(c["high"] - c["low"] for c in candles_1h[-14:]) / 14
                pump_target = round(high_max + atr_fb * 2, 6)
                pump_target_pct = round((pump_target - price_now) / price_now * 100, 1)
                pump_logic = "верхняя граница + ATR×2"
            except Exception:
                pass

        return {
            "symbol": symbol,
            "score": min(score, 100),
            "price": price_now,
            "range_pct": range_pct,
            "vol_ratio": vol_ratio,
            "bb_width": bb_width,
            "signals": signals,
            "pump_target": pump_target,
            "pump_target_pct": pump_target_pct,
            "pump_logic": pump_logic,
            "high_max": high_max,
            "low_min": low_min,
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

    sep = "━" * 26
    low_fmt  = smart_price_fmt(acc.get("low_min", p))
    high_fmt = smart_price_fmt(acc.get("high_max", p))
    range_block = f"📐 Диапазон: <code>{low_fmt}</code> — <code>{high_fmt}</code>\n"

    target_block = ""
    if acc.get("pump_target"):
        pt = acc["pump_target"]
        pt_pct = acc.get("pump_target_pct", 0)
        pt_logic = acc.get("pump_logic", "")
        pt_fmt = smart_price_fmt(pt)
        target_block = (
            f"\n🎯 <b>Цель памп:</b> <code>{pt_fmt}</code> (+{pt_pct:.1f}%)\n"
            f"💡 <i>{pt_logic}</i>\n"
        )

    return (
        f"{sep}\n"
        f"{grade}\n"
        f"📦 <b>{acc['symbol']}</b> | {grade_note}\n"
        f"{sep}\n\n"
        f"💰 Цена: <code>{ps}</code>\n"
        f"{range_block}"
        f"📊 Скор накопления: <b>{score}/100</b>\n"
        f"{target_block}\n"
        f"<b>Признаки:</b>\n{signals_text}\n\n"
        f"💡 <i>Войти при пробое <code>{high_fmt}</code> с объёмом</i>\n"
        f"{sep}"
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



TIMING_EXPIRY_HOURS = {"1h": 4, "4h": 12, "1d": 48, "1w": 120}

def save_to_timing_queue(symbol, direction, timeframe, entry, sl, tp1, tp2, tp3, grade, signal_text, timing_score):
    """Сохраняет сигнал в очередь ожидания тайминга"""
    try:
        import sqlite3 as _sq3
        from datetime import timedelta
        hours = TIMING_EXPIRY_HOURS.get(timeframe, 4)
        expires = (datetime.utcnow() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        conn = _sq3.connect("brain.db", timeout=10)
        existing = conn.execute(
            "SELECT id FROM timing_queue WHERE symbol=? AND direction=? AND timeframe=? AND status='waiting'",
            (symbol, direction, timeframe)
        ).fetchone()
        if existing:
            conn.close()
            return False
        conn.execute("""INSERT INTO timing_queue
            (symbol, direction, timeframe, entry, sl, tp1, tp2, tp3, grade, signal_text, timing_score, expires_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol, direction, timeframe, entry, sl, tp1, tp2, tp3, grade, signal_text, timing_score, expires))
        conn.commit()
        conn.close()
        logging.info(f"[TimingQueue] {symbol} {direction} {timeframe} → очередь (score {timing_score}/3, истекает через {hours}ч)")
        return True
    except Exception as e:
        logging.warning(f"[TimingQueue] ОШИБКА save_to_timing_queue {symbol}: {e}")
        return False


def get_timing_queue():
    """Возвращает все активные сигналы из очереди"""
    try:
        import sqlite3 as _sq3
        conn = _sq3.connect("brain.db", timeout=10)
        rows = conn.execute("""
            SELECT id, symbol, direction, timeframe, entry, sl, tp1, tp2, tp3, grade, signal_text, timing_score, expires_at
            FROM timing_queue WHERE status='waiting' ORDER BY created_at ASC
        """).fetchall()
        conn.close()
        return rows
    except Exception as e:
        logging.debug(f"get_timing_queue: {e}")
        return []


def expire_timing_queue():
    """Помечает истёкшие сигналы как expired"""
    try:
        import sqlite3 as _sq3
        conn = _sq3.connect("brain.db", timeout=10)
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        expired = conn.execute(
            "SELECT id, symbol, timeframe FROM timing_queue WHERE status='waiting' AND expires_at < ?", (now,)
        ).fetchall()
        for row in expired:
            conn.execute("DELETE FROM timing_queue WHERE id=?", (row[0],))
            logging.debug(f"[TimingQueue] {row[1]} {row[2]} → удалён")
        conn.commit()
        conn.close()
        return len(expired)
    except Exception as e:
        logging.debug(f"expire_timing_queue: {e}")
        return 0


def remove_from_timing_queue(queue_id):
    """Помечает сигнал как отправленный"""
    try:
        import sqlite3 as _sq3
        conn = _sq3.connect("brain.db", timeout=10)
        conn.execute("UPDATE timing_queue SET status='sent' WHERE id=?", (queue_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.debug(f"remove_from_timing_queue: {e}")

def check_entry_timing(candles, direction, entry_price, timeframe="1h"):
    """
    Тайминг входа — проверяет 3 условия перед входом:
    1. Sweep ликвидности (ложный пробой уровня)
    2. Импульсная свеча (подтверждение направления)
    3. Цена не ушла далеко от зоны входа

    Возвращает dict:
      - valid: bool — можно ли входить
      - score: 0-3 — количество выполненных условий
      - reasons: список причин
      - wait: описание что ждать
    """
    if not candles or len(candles) < 5:
        return {"valid": True, "score": 0, "reasons": [], "wait": ""}

    reasons = []
    warnings = []
    score = 0

    last  = candles[-1]
    prev  = candles[-2]
    prev2 = candles[-3]

    current_price = last["close"]
    atr = sum(c["high"] - c["low"] for c in candles[-14:]) / min(14, len(candles))
    avg_vol = sum(c.get("volume", 0) for c in candles[-20:]) / 20 if len(candles) >= 20 else 1

    # ── 1. SWEEP ЛИКВИДНОСТИ ──────────────────────────────────
    # Бычий sweep: предыдущая свеча пробила лоу, текущая закрылась выше
    if direction == "BULLISH":
        # Проверяем последние 3 свечи на sweep
        swept = False
        for i in range(-3, 0):
            c = candles[i]
            c_prev = candles[i - 1]
            # Свеча пробила лоу и закрылась выше — sweep
            if c["low"] < c_prev["low"] and c["close"] > c_prev["low"]:
                wick_size = (c["close"] - c["low"]) / (c["high"] - c["low"] + 0.000001)
                if wick_size > 0.4:  # нижний хвост > 40% тела
                    swept = True
                    break
        if swept:
            score += 1
            reasons.append("✅ Sweep ликвидности — ложный пробой вниз")
        else:
            warnings.append("⚠️ Нет sweep — ждать ложного пробоя лоу")

    else:  # BEARISH
        swept = False
        for i in range(-3, 0):
            c = candles[i]
            c_prev = candles[i - 1]
            if c["high"] > c_prev["high"] and c["close"] < c_prev["high"]:
                wick_size = (c["high"] - c["close"]) / (c["high"] - c["low"] + 0.000001)
                if wick_size > 0.4:
                    swept = True
                    break
        if swept:
            score += 1
            reasons.append("✅ Sweep ликвидности — ложный пробой хая")
        else:
            warnings.append("⚠️ Нет sweep — ждать ложного пробоя хая")

    # ── 2. ИМПУЛЬСНАЯ СВЕЧА ───────────────────────────────────
    # Последняя закрытая свеча должна быть в направлении сигнала + объём выше среднего
    candle_body = abs(last["close"] - last["open"])
    candle_range = last["high"] - last["low"]
    body_ratio = candle_body / candle_range if candle_range > 0 else 0
    last_vol = last.get("volume", 0)

    if direction == "BULLISH":
        is_impulse = (last["close"] > last["open"] and  # бычья свеча
                      body_ratio > 0.5 and               # тело > 50% диапазона
                      candle_body > atr * 0.5)           # тело > половины ATR
    else:
        is_impulse = (last["close"] < last["open"] and
                      body_ratio > 0.5 and
                      candle_body > atr * 0.5)

    if is_impulse:
        score += 1
        vol_note = f" (объём x{round(last_vol/avg_vol,1)})" if avg_vol > 0 else ""
        reasons.append(f"✅ Импульсная свеча{vol_note}")
    else:
        warnings.append("⚠️ Нет импульса — ждать сильной свечи подтверждения")

    # ── 3. ЦЕНА В ЗОНЕ ВХОДА ─────────────────────────────────
    # Цена не должна уйти далеко от зоны входа (максимум 1.5 ATR)
    drift = abs(current_price - entry_price)
    max_drift = atr * 1.5

    if drift <= max_drift:
        score += 1
        drift_pct = round(drift / entry_price * 100, 2)
        reasons.append(f"✅ Цена в зоне входа (отклонение {drift_pct}%)")
    else:
        drift_pct = round(drift / entry_price * 100, 2)
        warnings.append(f"⚠️ Цена ушла от зоны на {drift_pct}% — вход поздний")

    # ── ИТОГ ─────────────────────────────────────────────────
    # Валидный вход: минимум 2 из 3 условий
    valid = score >= 2

    wait_msg = ""
    if not valid:
        if score == 0:
            wait_msg = "Ждать: sweep + импульс"
        elif "sweep" in str(warnings):
            wait_msg = "Ждать ложного пробоя уровня"
        elif "импульс" in str(warnings):
            wait_msg = "Ждать импульсной свечи подтверждения"
        else:
            wait_msg = "Ждать возврата цены в зону"

    return {
        "valid": valid,
        "score": score,
        "reasons": reasons,
        "warnings": warnings,
        "wait": wait_msg,
        "swept": swept if direction == "BULLISH" else swept,
    }

def calc_smart_levels(candles, direction, price, timeframe="1h"):
    """Build MTF levels from confirmed structure; never fabricate a level.

    Entry is the overlap between an unmitigated OB/FVG and the 0.62-0.79
    Fibonacci OTE of the latest confirmed impulse. SL is placed beyond the
    protected swing and selected zone with an ATR buffer. Targets come only
    from confirmed swings, liquidity pools, opposing zones or Fibonacci
    extensions anchored to that same impulse.
    """
    try:
        if direction not in ("BULLISH", "BEARISH"):
            raise ValueError("unknown direction")
        if not candles or len(candles) < 30 or not price or price <= 0:
            raise ValueError("insufficient confirmed candles")

        raw_highs, raw_lows = find_swings(candles, lookback=8)
        if not raw_highs or not raw_lows:
            raise ValueError("confirmed swing structure is absent")

        # Use the most recent completed impulse in the strategy direction.
        impulse_low = impulse_high = None
        if direction == "BULLISH":
            for high_index, high_price in reversed(raw_highs):
                preceding_lows = [item for item in raw_lows if item[0] < high_index]
                if preceding_lows and high_price > preceding_lows[-1][1]:
                    impulse_low = float(preceding_lows[-1][1])
                    impulse_high = float(high_price)
                    break
        else:
            for low_index, low_price in reversed(raw_lows):
                preceding_highs = [item for item in raw_highs if item[0] < low_index]
                if preceding_highs and preceding_highs[-1][1] > low_price:
                    impulse_high = float(preceding_highs[-1][1])
                    impulse_low = float(low_price)
                    break

        if impulse_low is None or impulse_high is None or impulse_high <= impulse_low:
            raise ValueError("confirmed directional impulse is absent")

        atr = average_true_range(candles, 14)
        if not atr or atr <= 0:
            raise ValueError("ATR is unavailable")

        impulse_range = impulse_high - impulse_low
        if direction == "BULLISH":
            ote_entry = impulse_high - impulse_range * 0.705
            ote_low = impulse_high - impulse_range * 0.79
            ote_high = impulse_high - impulse_range * 0.62
        else:
            ote_entry = impulse_low + impulse_range * 0.705
            ote_low = impulse_low + impulse_range * 0.62
            ote_high = impulse_low + impulse_range * 0.79

        def _zone_mitigated(zone):
            """A zone is fresh only when no later confirmed candle retested it."""
            origin = zone.get("index")
            if not isinstance(origin, int):
                return True
            bottom = float(zone["bottom"])
            top = float(zone["top"])
            # OB displacement/FVG formation uses the next candle, so begin
            # checking after it. The live candle is not in ``candles``.
            for candle in candles[origin + 2:]:
                if float(candle["low"]) <= top and float(candle["high"]) >= bottom:
                    return True
            return False

        # Entry must be both a real imbalance/order block and a Fibonacci OTE.
        zone_candidates = []
        for zone_kind, zone in (("OB", find_ob(candles, direction)),
                                ("FVG", find_fvg(candles, direction))):
            if not zone or _zone_mitigated(zone):
                continue
            overlap_low = max(float(zone["bottom"]), ote_low)
            overlap_high = min(float(zone["top"]), ote_high)
            if overlap_low > overlap_high:
                continue
            # Preserve the 0.705 anchor where possible; otherwise use the
            # nearest boundary of the genuine zone/OTE overlap.
            level = min(max(ote_entry, overlap_low), overlap_high)
            zone_candidates.append((abs(float(price) - level), zone_kind, zone, level))

        if not zone_candidates:
            raise ValueError("no fresh OB/FVG overlap with OTE")
        _, zone_kind, entry_zone, entry_raw = min(
            zone_candidates,
            key=lambda item: (item[0], 0 if item[1] == "OB" else 1),
        )
        entry = smart_round(entry_raw)

        heatmap = get_liquidity_heatmap(candles) or {}
        buffer = atr * ({"1h": 0.25, "4h": 0.30}.get(timeframe, 0.25))

        if direction == "BULLISH":
            stop_anchors = [impulse_low, float(entry_zone["bottom"])]
            # Long stops sit below sell-side liquidity, never below buy stops.
            sell_stops = heatmap.get("nearest_sell_stops")
            sell_stop_price = sell_stops.get("price") if isinstance(sell_stops, dict) else sell_stops
            if sell_stop_price and sell_stop_price < entry and abs(float(sell_stop_price) - impulse_low) <= atr:
                stop_anchors.append(float(sell_stop_price))
            sl = smart_round(min(stop_anchors) - buffer)
        else:
            stop_anchors = [impulse_high, float(entry_zone["top"])]
            # Short stops sit above buy-side liquidity, never above sell stops.
            buy_stops = heatmap.get("nearest_buy_stops")
            buy_stop_price = buy_stops.get("price") if isinstance(buy_stops, dict) else buy_stops
            if buy_stop_price and buy_stop_price > entry and abs(float(buy_stop_price) - impulse_high) <= atr:
                stop_anchors.append(float(buy_stop_price))
            sl = smart_round(max(stop_anchors) + buffer)

        risk = abs(entry - sl)
        if risk <= 0:
            raise ValueError("invalid structural stop")
        # Reject excessive or noise-tight risk. Never move a valid structural
        # invalidation inward merely to manufacture acceptable RR.
        max_stop_atr = {"1h": 3.0, "4h": 3.5}.get(timeframe, 3.0)
        if risk < atr * 0.20 or risk > atr * max_stop_atr:
            raise ValueError("structural stop is outside the risk envelope")

        target_candidates = []
        if direction == "BULLISH":
            target_candidates.extend(float(value) for _, value in raw_highs)
            target_candidates.extend((
                impulse_low + impulse_range * 1.272,
                impulse_low + impulse_range * 1.618,
            ))
            wanted_liquidity = "buy_stops"
            opposing_direction = "BEARISH"
        else:
            target_candidates.extend(float(value) for _, value in raw_lows)
            target_candidates.extend((
                impulse_high - impulse_range * 1.272,
                impulse_high - impulse_range * 1.618,
            ))
            wanted_liquidity = "sell_stops"
            opposing_direction = "BULLISH"

        for level in heatmap.get("levels", []):
            if isinstance(level, dict) and level.get("type") == wanted_liquidity:
                target_candidates.append(level.get("price"))

        for opposing_zone in (
            find_ob(candles, opposing_direction),
            find_fvg(candles, opposing_direction),
        ):
            if opposing_zone:
                target_candidates.extend((opposing_zone.get("bottom"), opposing_zone.get("top")))

        tp1, tp2 = select_structural_targets(
            entry=entry,
            sl=sl,
            candidates=target_candidates,
            direction=direction,
            min_rr=2.0,
            max_rr=4.0,
        )
        if tp1 is None or tp2 is None:
            raise ValueError("two structural targets are unavailable")
        if abs(tp2 - entry) / risk > 6.0:
            raise ValueError("second structural target is too distant")

        tp3 = tp2  # legacy DB/message field; the strategy has two real exits.
        rr = round(abs(tp1 - entry) / risk, 2)
        return {
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl_pct": round(risk / entry * 100, 2),
            "tp1_pct": round(abs(tp1 - entry) / entry * 100, 2),
            "tp2_pct": round(abs(tp2 - entry) / entry * 100, 2),
            "tp3_pct": round(abs(tp3 - entry) / entry * 100, 2),
            "rr": rr,
            "mitigated": False,
            "entry_zone": zone_kind,
            "source": f"structure+fib_ote+{zone_kind.lower()}",
        }

    except Exception as error:
        logging.debug("calc_smart_levels rejected (%s %s): %s", direction, timeframe, error)
        return None

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
    Gate USD-M candles through the configured router.

    Analysis never falls through to Binance.  Binance is reserved for the
    deterministic execution module after Groq has approved a candidate.
    """
    global candle_cache
    cache_key = f"{symbol}_{interval}"

    cache_ttl = 60 if interval in ("1m", "3m", "5m") else 180 if interval in ("15m", "30m") else 300 if interval in ("1h", "2h") else 600
    if cache_key in candle_cache:
        cached, ts = candle_cache[cache_key]
        if time.time() - ts < cache_ttl and len(cached) >= 20:
            return cached

    # Проверяем global candles storage
    _gc = get_global_candles(symbol, interval)
    if _gc and len(_gc) >= 20:
        candle_cache[cache_key] = (_gc, time.time())
        return _gc

    # 1. Brain Router — Gate USD-M in the default production policy.
    if _ROUTER_OK:
        try:
            rc = _brain_router.candles(symbol, interval, limit)
            if rc and len(rc) >= 3:
                candle_cache[cache_key] = (rc, time.time())
                update_global_candles(symbol, interval, rc)
                return rc
        except Exception as e:
            logging.debug(f"BrainRouter candles {symbol} {interval}: {e}")

    # 2. Core SMC Gate adapter — independent fallback, same venue.
    if _SMC_ENGINE_OK:
        try:
            result = get_candles_smart(symbol, interval, limit)
            candles = result.get("candles", []) if isinstance(result, dict) else []
            if candles and len(candles) >= 3:
                candle_cache[cache_key] = (candles, time.time())
                update_global_candles(symbol, interval, candles)
                return candles
        except Exception as e:
            logging.debug("SMC Gate candles %s %s: %s", symbol, interval, e)

    logging.debug("Нет Gate Futures свечей для %s %s", symbol, interval)
    return []


async def fetch_candles_batch(symbols: list, timeframe: str = "4h", limit: int = 100) -> dict:
    """
    Асинхронная загрузка свечей для списка символов одновременно.
    Возвращает {symbol: candles}
    """
    import asyncio as _asyncio

    async def _fetch_one(sym):
        try:
            loop = _asyncio.get_event_loop()
            candles = await loop.run_in_executor(None, lambda: get_candles(sym, timeframe, limit))
            return sym, candles
        except Exception:
            return sym, []

    tasks = [_fetch_one(s) for s in symbols]
    results = await _asyncio.gather(*tasks, return_exceptions=True)

    out = {}
    for r in results:
        if isinstance(r, tuple) and len(r) == 2 and r[1]:
            out[r[0]] = r[1]
    return out


def get_orderbook(symbol):
    try:
        from external_sources.pair_registry import get_pair
        pair = get_pair(symbol)
        contract = str(pair.get("gate_symbol") or symbol.replace("USDT", "_USDT"))
        multiplier = float(pair.get("gate_multiplier") or 1)
        r = requests.get(
            "https://api.gateio.ws/api/v4/futures/usdt/order_book",
            params={"contract": contract, "limit": 20}, timeout=8,
        )
        r.raise_for_status()
        d = r.json()
        bids = sum(float(row.get("p", 0)) * abs(float(row.get("s", 0))) * multiplier for row in d.get("bids", []))
        asks = sum(float(row.get("p", 0)) * abs(float(row.get("s", 0))) * multiplier for row in d.get("asks", []))
        return {"bids": bids, "asks": asks, "bias": "BUY" if bids > asks else "SELL"}
    except Exception:
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

def find_swings(candles, lookback=8):
    return _find_structure_swings(candles, lookback=lookback)

def classify_swings(highs, lows):
    return _classify_structure_swings(highs, lows)

def detect_events(candles, classified):
    """Return a real BOS/CHoCH or a symmetric HH+HL/LH+LL trend state."""
    if not classified or not candles:
        return []
    return _structure_events(candles, classified, max_break_age=1)

def find_ob(candles, direction):
    for i in range(len(candles) - 2, max(0, len(candles) - 25), -1):
        c = candles[i]
        if direction == "BULLISH" and c["close"] < c["open"]:
            return {
                "top": max(c["open"], c["close"]),
                "bottom": min(c["open"], c["close"]),
                "index": i,
            }
        if direction == "BEARISH" and c["close"] > c["open"]:
            return {
                "top": max(c["open"], c["close"]),
                "bottom": min(c["open"], c["close"]),
                "index": i,
            }
    return None

def find_fvg(candles, direction):
    for i in range(len(candles) - 3, max(1, len(candles) - 20), -1):
        if direction == "BULLISH" and candles[i+1]["low"] > candles[i-1]["high"]:
            return {
                "top": candles[i+1]["low"],
                "bottom": candles[i-1]["high"],
                "index": i,
            }
        if direction == "BEARISH" and candles[i+1]["high"] < candles[i-1]["low"]:
            return {
                "top": candles[i-1]["low"],
                "bottom": candles[i+1]["high"],
                "index": i,
            }
    return None

def check_opposing_ob(candles, direction, entry, tp):
    """Проверяет нет ли противоположного OB или FVG между entry и TP.
    Возвращает скорректированный TP или None если блокирует."""
    opposing_dir = "BEARISH" if direction == "BULLISH" else "BULLISH"
    opp_ob = find_ob(candles, opposing_dir)
    opp_fvg = find_fvg(candles, opposing_dir)

    # Собираем все блокирующие зоны
    blockers = []
    if opp_ob:
        blockers.append(("OB", opp_ob["bottom"], opp_ob["top"]))
    if opp_fvg:
        blockers.append(("FVG", opp_fvg["bottom"], opp_fvg["top"]))

    if not blockers:
        return tp

    for _btype, b_bottom, b_top in blockers:
        if direction == "BULLISH":
            if entry < b_bottom < tp:
                new_tp = smart_round(b_bottom * 0.998)
                if new_tp > entry * 1.003:
                    tp = new_tp  # сужаем TP до ближайшего блокера
                else:
                    return None
        else:
            if tp < b_top < entry:
                new_tp = smart_round(b_top * 1.002)
                if new_tp < entry * 0.997:
                    tp = new_tp
                else:
                    return None
    return tp


def detect_engulfing(candles, direction):
    """Проверяет наличие engulfing паттерна на последних 3 свечах."""
    if not candles or len(candles) < 2:
        return False
    for i in range(-1, max(-4, -len(candles)), -1):
        curr = candles[i]
        prev = candles[i - 1] if abs(i - 1) <= len(candles) else None
        if not prev:
            continue
        curr_body = abs(curr["close"] - curr["open"])
        prev_body = abs(prev["close"] - prev["open"])
        if prev_body == 0:
            continue
        if direction == "BULLISH":
            if (curr["close"] > curr["open"] and  # текущая зелёная
                prev["close"] < prev["open"] and  # предыдущая красная
                curr_body > prev_body and  # тело больше
                curr["close"] > prev["open"] and curr["open"] < prev["close"]):
                return True
        elif direction == "BEARISH":
            if (curr["close"] < curr["open"] and
                prev["close"] > prev["open"] and
                curr_body > prev_body and
                curr["close"] < prev["open"] and curr["open"] > prev["close"]):
                return True
    return False


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
    candles = get_confirmed_candles(get_candles(symbol, interval, 150))
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
        timeframes = ["15m", "1h", "4h", "1d"]
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
        from external_sources.pair_registry import get_pair
        gate_sym = str(get_pair(symbol).get("gate_symbol") or symbol.replace("USDT", "_USDT"))
        r = requests.get(
            f"https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/{gate_sym}",
            timeout=8
        )
        data = r.json()
        rate = data.get("funding_rate")
        if rate is not None:
            return float(rate) * 100
        return None
    except:
        return None

# ===== OPEN INTEREST =====

def get_open_interest(symbol):
    """Gate USD-M open-interest trend."""
    try:
        from external_sources.pair_registry import get_pair
        contract = str(get_pair(symbol).get("gate_symbol") or symbol.replace("USDT", "_USDT"))
        response = requests.get(
            "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
            params={"contract": contract, "interval": "1h", "limit": 5}, timeout=8,
        )
        response.raise_for_status()
        history = response.json()
        if not isinstance(history, list) or not history:
            return None
        current_oi = float(history[-1].get("open_interest") or 0)
        old_oi = float(history[0].get("open_interest") or 0)
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
    """Среднее изменение BTC за последние 3 свечи 1h (устойчивее к шуму)."""
    try:
        candles = get_candles("BTCUSDT", "1h", 5)
        if not candles or len(candles) < 4:
            return 0.0
        # Среднее изменение за последние 3 свечи
        changes = []
        for i in range(-3, 0):
            c0, c1 = candles[i - 1], candles[i]
            changes.append((c1["close"] - c0["close"]) / c0["close"] * 100)
        return round(sum(changes) / len(changes), 3)
    except Exception:
        return 0.0

def get_btc_4h_change():
    """Среднее изменение BTC за последние 3 свечи 4h (для WYCKOFF/долгих стратегий)."""
    try:
        candles = get_candles("BTCUSDT", "4h", 5)
        if not candles or len(candles) < 4:
            return 0.0
        changes = []
        for i in range(-3, 0):
            c0, c1 = candles[i - 1], candles[i]
            changes.append((c1["close"] - c0["close"]) / c0["close"] * 100)
        return round(sum(changes) / len(changes), 3)
    except Exception:
        return 0.0

def btc_allows_signal(direction, use_4h=False):
    """Если BTC падает -0.8%+ (среднее за 3 свечи) — не даём лонги на альты."""
    btc_change = get_btc_4h_change() if use_4h else get_btc_1h_change()
    tf_label = "4h" if use_4h else "1h"
    if direction == "BULLISH" and btc_change < -0.8:
        return False, f"BTC падает {btc_change:.1f}%/{tf_label} — лонги опасны"
    if direction == "BEARISH" and btc_change > 0.8:
        return False, f"BTC растёт {btc_change:.1f}%/{tf_label} — шорты опасны"
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
            logging.debug("DXY: пустой ответ от Yahoo Finance")
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
        logging.debug(f"DXY: {e}")
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
        items = []
        try:
            r = requests.get(
                "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10
            )
            if r.status_code == 200:
                items = r.json()
        except Exception as e:
            logging.debug(f"get_upcoming_events fetch: {e}")

        for item in items:
            title = item.get("title", "")
            if any(kw.lower() in title.lower() for kw in high_impact):
                warnings.append(f"{item.get('date','')}: {title[:60]}")

        econ_cache = " | ".join(warnings[:2]) if warnings else ""
        econ_cache_time = time.time()
        return econ_cache
    except Exception as e:
        logging.debug(f"get_upcoming_events: {e}")
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
        prices = await asyncio.to_thread(get_live_prices)
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
                    pump_alerted.add(symbol)
                    # Internal detector only: no Telegram pump/dump alert.
                    asyncio.get_running_loop().call_later(1800, lambda s=symbol: pump_alerted.discard(s))
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
    # Legacy formatter used percentage-based stops/targets.  It is retained
    # only as a compatibility entry point and must delegate to the canonical
    # MTF candidate builder, which owns all structural level calculations.
    _handler = globals().get("_RAW_SCAN_HANDLER")
    if callable(_handler):
        _tf_by_type = {"scalp": "15m", "swing": "1h", "long": "4h"}
        return _handler(symbol, _tf_by_type.get(trade_type, "1h"), False)
    logging.warning("analyze_trade_type disabled until canonical scan handler is registered")
    return None

    # Unreachable legacy implementation remains below for source compatibility
    # with old deployments; the guard above prevents its fabricated levels.
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
            entry = ob["top"] if ob else price
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
            f"🎯 <b>TP:</b>  <code>{fmt(tp1)}</code>\n\n"
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


_RAW_SCAN_HANDLER = None


def register_raw_scan_handler(handler):
    """Register the canonical MTF candidate builder owned by bot.py.

    This keeps conversational/manual analysis on exactly the same strategy
    implementation as the automatic scanner without introducing a circular
    import between the two legacy modules.
    """
    global _RAW_SCAN_HANDLER
    _RAW_SCAN_HANDLER = handler


def full_scan(symbol, timeframe="1h"):
    """Полный SMC анализ с мультитаймфреймом + все новые фильтры"""
    # All callers use the canonical implementation registered by bot.py.
    # The former body contains historical fallback levels and is never allowed
    # to run when the canonical strategy is unavailable.
    if callable(_RAW_SCAN_HANDLER):
        return _RAW_SCAN_HANDLER(symbol, timeframe, False)
    logging.warning("full_scan disabled until canonical scan handler is registered")
    return None

    # Unreachable legacy implementation retained for old source deployments.
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

        # ── HTF фильтр: 1w → penalty, 1d → подтверждение ──
        htf_1w = smc_on_tf(symbol, "1w")
        htf_1d = smc_on_tf(symbol, "1d")
        # 1w конфликт — не блокируем, а штрафуем и передаём Groq
        _1w_conflict = False
        if htf_1w:
            if direction == "BULLISH" and "BEARISH" in str(htf_1w).upper():
                _1w_conflict = True
                logging.info(f"[1w Filter] {symbol} LONG против 1w BEARISH — penalty -10")
            if direction == "BEARISH" and "BULLISH" in str(htf_1w).upper():
                _1w_conflict = True
                logging.info(f"[1w Filter] {symbol} SHORT против 1w BULLISH — penalty -10")
        # 1d подтверждение
        if htf_1d:
            if direction == "BULLISH" and "BEARISH" in str(htf_1d).upper():
                logging.info(f"[HTF Filter] {symbol} LONG заблокирован — 1d BEARISH")
                return None
            if direction == "BEARISH" and "BULLISH" in str(htf_1d).upper():
                logging.info(f"[HTF Filter] {symbol} SHORT заблокирован — 1d BULLISH")
                return None
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

        # Мёртвая зона (22:00-07:00 UTC) — снижаем агрессивность
        from datetime import datetime as _dt
        _hour = _dt.utcnow().hour
        _dead_zone = (22 <= _hour or _hour <= 6)

        # ── 3. Взвешенный confluence ──
        weights = get_confluence_weights(symbol)
        confluence = []
        total_weight = 0

        # 1w penalty если против тренда
        if _1w_conflict:
            total_weight -= 10
            confluence.append("⚠️ Против 1w тренда (-10)")

        # MTF — базовый вес
        mtf_w = weights.get("mtf", 30)
        confluence.append(f"✅ {mtf['match_count']}/{mtf['total']} ТФ совпали (вес {mtf_w})")
        total_weight += mtf_w

        # ── CHoCH/MSS подтверждение на 15m ──
        try:
            _candles_15m = get_confirmed_candles(get_candles(symbol, "15m", 30))
            if _candles_15m and len(_candles_15m) >= 10:
                _sw_15m, _sl_15m = find_swings(_candles_15m, lookback=3)
                _cl_15m = classify_swings(_sw_15m, _sl_15m)
                _ev_15m = detect_events(_candles_15m, _cl_15m)
                _has_choch_15m = any(
                    e.get("direction") == direction and e.get("type") in ("CHoCH", "BOS")
                    for e in _ev_15m
                )
                if not _has_choch_15m:
                    logging.info(f"full_scan {symbol}: нет CHoCH/BOS на 15m — hard block")
                    return None
        except Exception:
            pass

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
            # Hard block при экстремальном FR (>±0.2%)
            if abs(funding) > 0.2:
                if (direction == "BULLISH" and funding > 0.2) or (direction == "BEARISH" and funding < -0.2):
                    logging.info(f"[FR Hard Block] {symbol} {direction}: FR {funding:+.4f}% экстремальный")
                    return None
            if direction == "BULLISH" and funding < 0.05:
                confluence.append(f"✅ Funding: {funding:+.4f}% — нейтральный")
                total_weight += 8
            elif direction == "BEARISH" and funding > -0.05:
                confluence.append(f"✅ Funding: {funding:+.4f}% — нейтральный")
                total_weight += 8
            elif abs(funding) > 0.15:
                confluence.append(f"⚠️ Funding: {funding:+.4f}% — перегрев, риск ликвидаций")
        else:
            # FR недоступен — penalty
            confluence.append(f"⚠️ Funding Rate недоступен (-5)")
            total_weight -= 5

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

        # Мёртвая зона — штраф + hard block при низком confluence
        if _dead_zone:
            confluence.append(f"⚠️ Мёртвая зона (UTC {_hour}:xx) — ликвидность низкая (-15)")
            total_weight -= 15
            if total_weight < 70:
                logging.info(f"full_scan {symbol}: dead zone + confluence {total_weight} < 70 — hard block")
                return None

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

                # Fibonacci — золотая зона 0.618-0.786
                fib = get_fibonacci_levels(candles, direction)
                if fib.get("in_golden_zone"):
                    confluence.append(f"✅ Fibonacci золотая зона {fib['nearest_ratio']:.3f} — точный вход (+15)")
                    total_weight += 15
                elif fib.get("nearest_ratio") in (0.382, 0.5):
                    confluence.append(f"📍 Fibonacci {fib['nearest_ratio']:.3f} уровень — зона интереса (+7)")
                    total_weight += 7

                # Market Maker Accumulation Detector
                mm_acc = detect_mm_accumulation(candles)
                mm_sig = mm_acc.get("signal", "NEUTRAL")
                mm_score = mm_acc.get("score", 0)
                if mm_sig == "STRONG_ACCUMULATION":
                    confluence.append(f"✅ MM Накопление СИЛЬНОЕ (score {mm_score}/4) — фондовый паттерн (+15)")
                    total_weight += 15
                    for s in mm_acc.get("signals", []):
                        confluence.append(f"  {s}")
                elif mm_sig == "ACCUMULATION":
                    confluence.append(f"✅ MM Накопление (score {mm_score}/4) — вероятен выход (+10)")
                    total_weight += 10
                    for s in mm_acc.get("signals", []):
                        confluence.append(f"  {s}")
                elif mm_sig == "WEAK_ACCUMULATION":
                    confluence.append(f"📦 MM Слабое накопление (score {mm_score}/4) (+5)")
                    total_weight += 5
                if mm_acc.get("pre_pump"):
                    confluence.append(f"🚀 PRE-PUMP паттерн подтверждён — объём↑ диапазон↓ лои↑ (+10)")
                    total_weight += 10

                # Smart Money Divergence
                smd = detect_smart_money_divergence(candles, ob, fvg, direction)
                if smd["score"] != 0:
                    total_weight += smd["score"]
                    for sig_text in smd["signals"]:
                        confluence.append(sig_text)

                # Inducement — ложный пробой
                ind = detect_inducement(candles, direction)
                if ind:
                    w = ind["weight"]
                    confluence.append(f"✅ Inducement {ind['type']} (wick {ind['wick_size']:.0%}, объём x{ind['vol_spike']:.1f}) (+{w})")
                    total_weight += w

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

                # RSI/MACD Divergence
                rmd = detect_rsi_macd_divergence(candles, direction)
                if rmd["found"]:
                    total_weight += rmd["weight"]
                    for s in rmd["signals"]:
                        confluence.append(s)

                # VWAP — средневзвешенная цена по объёму
                vwap_data = calculate_vwap(candles)
                if vwap_data["vwap"] > 0:
                    dev = vwap_data["deviation_pct"]
                    if vwap_data["near_vwap"]:
                        confluence.append(f"📍 Цена у VWAP {vwap_data['vwap']:.4f} — зона интереса")
                    elif vwap_data["signal"] == direction:
                        confluence.append(f"✅ VWAP: {vwap_data['desc']} (+5)")
                        total_weight += 5
                    else:
                        confluence.append(f"⚠️ VWAP: {vwap_data['desc']} — против направления (-3)")
                        total_weight -= 3

                # Heatmap ликвидности — ближайшие стопы
                heatmap = get_liquidity_heatmap(candles)
                if direction == "BULLISH" and heatmap["nearest_buy_stops"]:
                    lvl = heatmap["nearest_buy_stops"]
                    if lvl["strength"] == "HIGH":
                        confluence.append(f"🎯 Buy Stops выше на {lvl['dist_pct']:.1f}% ({lvl['touches']} касаний) — цель для пампа (+5)")
                        total_weight += 5
                elif direction == "BEARISH" and heatmap["nearest_sell_stops"]:
                    lvl = heatmap["nearest_sell_stops"]
                    if lvl["strength"] == "HIGH":
                        confluence.append(f"🎯 Sell Stops ниже на {lvl['dist_pct']:.1f}% ({lvl['touches']} касаний) — цель для дампа (+5)")
                        total_weight += 5

                # Breaker Block — пробитый OB как новый уровень
                breaker = detect_breaker_block(candles, direction)
                if breaker:
                    w = breaker["weight"]
                    confluence.append(f"✅ {breaker['desc']} (+{w})")
                    total_weight += w

            except Exception as _e:
                logging.debug(f"new_smc_confluence {symbol}: {_e}")

        # ── OI + Funding Rate + Liquidation ────────────────────
        try:
            oi_data = get_open_interest(symbol)
            fr_data = get_funding_rate(symbol)
            liq_data = get_liquidation_ratio(symbol)

            if fr_data["ok"] and fr_data["signal"] != "NEUTRAL":
                if fr_data["signal"] == direction[:4] or fr_data["signal"] == direction:
                    pass  # совпадает — нейтрально
                else:
                    confluence.append(f"⚠️ {fr_data['desc']} — противоречие направлению")
                    total_weight -= 5

            if liq_data["ok"] and liq_data["signal"] != "NEUTRAL":
                if liq_data["signal"] == direction:
                    confluence.append(f"✅ {liq_data['desc']} (+8)")
                    total_weight += 8
                else:
                    confluence.append(f"⚠️ {liq_data['desc']}")
                    total_weight -= 3
        except Exception as _oi_e:
            logging.debug(f"OI/FR/Liq {symbol}: {_oi_e}")

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

                # BTC корреляция — предупреждение если альт идёт против BTC
                btc_corr = _learn_btc_corr(symbol)
                if btc_corr.get("samples", 0) >= 5:
                    beta = btc_corr.get("beta", 1.0)
                    if beta > 1.3:
                        confluence.append(f"⚠️ Высокая BTC корреляция (β={beta:.1f}) — риск выше")
                        total_weight -= 3
                    elif beta < 0.5:
                        confluence.append(f"✅ Низкая BTC корреляция (β={beta:.1f}) — независимое движение")
                        total_weight += 3

                # День недели — лучшее/худшее время
                best_hours = _learn_best_hours()
                current_hour = __import__('datetime').datetime.utcnow().hour
                if best_hours and current_hour in best_hours[:3]:
                    confluence.append(f"✅ Лучший час для входа ({current_hour}:00 UTC) (+3)")
                    total_weight += 3

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

        # ── MTF Combo-Score: минимум 3 из 7 подтверждений ──
        _combo_hits = 0
        _combo_total = 7
        # 1) OB найден
        if ob:
            _combo_hits += 1
        # 2) FVG найден
        if fvg:
            _combo_hits += 1
        # 3) CVD совпадает с направлением
        try:
            _combo_cvd = calculate_cvd(candles)
            if _combo_cvd["signal"] == direction[:4] or _combo_cvd["signal"] == direction:
                _combo_hits += 1
        except Exception:
            pass
        # 4) RSI дивергенция (уже посчитана в confluence)
        try:
            _combo_div = detect_rsi_macd_divergence(candles, direction) if _SMC_ENGINE_OK else {"found": False}
            if _combo_div.get("found"):
                _combo_hits += 1
        except Exception:
            pass
        # 5) Объём последних 3 свечей выше среднего
        try:
            _vol_avg20 = sum(c.get("volume", 0) for c in candles[-20:]) / 20 if len(candles) >= 20 else 0
            _vol_last3 = sum(c.get("volume", 0) for c in candles[-3:]) / 3 if len(candles) >= 3 else 0
            if _vol_avg20 > 0 and _vol_last3 > _vol_avg20:
                _combo_hits += 1
                confluence.append(f"✅ Объём 3 свечи выше avg ({_vol_last3/_vol_avg20:.1f}x) (+4)")
                total_weight += 4
        except Exception:
            pass
        # 6) BTC идёт в том же направлении (бонус, не просто "не против")
        try:
            _btc_chg = get_btc_1h_change()
            if (direction == "BULLISH" and _btc_chg > 0.3) or (direction == "BEARISH" and _btc_chg < -0.3):
                _combo_hits += 1
                confluence.append(f"✅ BTC совпадает ({_btc_chg:+.1f}%) (+5)")
                total_weight += 5
        except Exception:
            pass
        # 7) Цена в OTE зоне (Fib 0.62-0.79)
        try:
            _combo_fib = get_fibonacci_levels(candles, direction) if _SMC_ENGINE_OK else {}
            _nr = _combo_fib.get("nearest_ratio", 0)
            if 0.62 <= _nr <= 0.79:
                _combo_hits += 1
                confluence.append(f"✅ OTE зона (Fib {_nr:.3f}) (+6)")
                total_weight += 6
        except Exception:
            pass

        if _combo_hits < 2:
            logging.info(f"full_scan {symbol}: combo-score {_combo_hits}/{_combo_total} < 2 — пропускаем")
            return None
        confluence.append(f"🎯 Combo-score: {_combo_hits}/{_combo_total}")

        # Минимальный порог — учитывает серию потерь
        min_weight = max(streak_threshold, 18 if mtf["match_count"] >= 3 else 22)
        # Ограничиваем скор максимум 100
        total_weight = min(total_weight, 100)
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

        # ── Engulfing pattern как дополнительное подтверждение ──
        if detect_engulfing(candles, direction):
            confluence.append(f"✅ Engulfing паттерн подтверждает {direction} (+5)")
            total_weight += 5

        risk = price * 0.015
        if direction == "BULLISH":
            entry = ob["top"] if ob else price
            sl = smart_round(entry - risk)
            tp1 = smart_round(entry + risk * 2)
            tp2 = smart_round(entry + risk * 3)
            tp3 = smart_round(entry + risk * 5)
        else:
            entry = ob["top"] if ob else price
            sl = smart_round(entry + risk)
            tp1 = smart_round(entry - risk * 2)
            tp2 = smart_round(entry - risk * 3)
            tp3 = smart_round(entry - risk * 5)

        # Ещё раз проверяем — если entry=0, сигнал невалидный
        if not entry or entry == 0:
            logging.warning(f"full_scan {symbol}: entry = 0 после расчёта, пропускаем")
            return None

        # ── 4.1 Минимальный RR фильтр ──
        _mtf_risk = abs(entry - sl)
        _mtf_reward = abs(tp1 - entry)
        if _mtf_risk > 0 and _mtf_reward / _mtf_risk < 1.5:
            logging.info(f"full_scan {symbol}: RR {_mtf_reward/_mtf_risk:.2f} < 1.5 — пропускаем")
            return None

        # ── 4.2 Проверка противоположного OB между entry и TP1 ──
        _adj_tp1 = check_opposing_ob(candles, direction, entry, tp1)
        if _adj_tp1 is None:
            logging.info(f"full_scan {symbol}: противоположный OB блокирует TP1 — пропускаем")
            return None
        tp1 = _adj_tp1

        # ── 4.5 Тайминг входа — sweep + импульс + зона ──
        timing = check_entry_timing(candles, direction, entry, timeframe)
        timing_score = timing.get("score", 0)
        timing_reasons = timing.get("reasons", [])
        timing_warnings = timing.get("warnings", [])

        # Добавляем тайминг в confluence
        for r in timing_reasons:
            confluence.append(r)
            total_weight += 8  # каждое подтверждение тайминга добавляет вес

        # Если тайминг плохой — добавляем предупреждение но не блокируем
        if not timing["valid"]:
            for w in timing_warnings:
                confluence.append(w)
            # Понижаем вес если нет ни одного подтверждения тайминга
            if timing_score == 0:
                total_weight -= 10

        # ── 5. Время отработки ──
        est_hours, confidence_str, win_rate = get_estimated_time(symbol, timeframe)
        time_str = f"~{est_hours}ч" if est_hours < 24 else f"~{est_hours//24}дн"
        wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"

        # ── 6. AI комментарий к сигналу — с учётом правил самообучения ──
        brain_ctx = get_brain_context(symbol, direction)
        signal_comment = generate_signal_comment(
            symbol, direction, mtf, total_weight, regime, fg, funding, ob, fvg, brain_ctx,
            entry=entry, sl=sl, tp1=tp1, timeframe=timeframe
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
            f"🎯 <b>TP:</b>  <code>{smart_price_fmt(tp1)}</code>\n\n"
            f"⏱ <b>Время отработки:</b> {time_str}\n"
            f"📊 <b>Точность:</b> {wr_str} | {confidence_str}\n"
            f"⏰ <b>Тайминг входа:</b> {'✅ Готов к входу' if timing['valid'] else '⏳ ' + timing.get('wait','Ждать подтверждения')} ({timing_score}/3)\n"
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


def _capture_market_memory_snapshot(sig_id, symbol, signal_type, direction, timeframe,
                                    entry, sl, tp1, tp2, tp3, confluence, regime):
    """Background persistence only; it never blocks signal delivery."""
    if not _MARKET_MEMORY_OK:
        return
    try:
        timeframes = [timeframe]
        for higher_tf in ("4h", "1d"):
            if higher_tf not in timeframes:
                timeframes.append(higher_tf)
        candles_by_timeframe = {}
        for tf in timeframes:
            candles = get_candles(symbol, tf, 120 if tf == timeframe else 80)
            if candles:
                candles_by_timeframe[tf] = candles
        _memory_capture_snapshot(
            sig_id, symbol, signal_type, direction, timeframe, entry, sl,
            tp1, tp2, tp3, confluence or 0, regime or "UNKNOWN",
            candles_by_timeframe,
        )
    except Exception as exc:
        logging.debug("[MarketMemory] background snapshot %s: %s", symbol, exc)


def save_signal_db(symbol, direction, signal_type, entry, tp1, tp2, tp3, sl, timeframe, est_hours, grade, confluence=0, regime="UNKNOWN"):
    """Persist a delivered signal; learning starts only after entry activation."""
    learning_id = None
    if not _SIGNAL_LIFECYCLE_OK:
        logging.error("save_signal_db: lifecycle module unavailable — persistence blocked")
        return None, None
    try:
        _pre = sqlite3.connect("brain.db", timeout=10, check_same_thread=False)
        existing = _pre.execute(
            "SELECT id FROM signals WHERE symbol=? AND timeframe=? AND direction=? AND result='pending' LIMIT 1",
            (symbol, timeframe, direction)
        ).fetchone()
        _pre.close()
        if existing:
            logging.info(f"save_signal_db: дубль {symbol} {timeframe} {direction} — пропущено")
            return None, None
    except Exception as _pre_error:
        logging.warning(f"save_signal_db precheck: {_pre_error}")

    for _attempt in range(5):
        try:
            conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=15000")
            # ── Защита от дублей: не сохраняем если уже есть pending сигнал по этой паре+ТФ+направление ──
            existing = conn.execute(
                "SELECT id FROM signals WHERE symbol=? AND timeframe=? AND direction=? AND result='pending' LIMIT 1",
                (symbol, timeframe, direction)
            ).fetchone()
            if existing:
                conn.close()
                logging.info(f"save_signal_db: дубль {symbol} {timeframe} {direction} — пропущено (ID существует: {existing[0]})")
                return None, learning_id
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
            _lifecycle_register_waiting(conn, sig_id)
            conn.commit()
            conn.close()
            if _MARKET_MEMORY_OK:
                threading.Thread(
                    target=_capture_market_memory_snapshot,
                    args=(sig_id, symbol, signal_type, direction, timeframe, entry, sl,
                          tp1, tp2, tp3, confluence, regime),
                    daemon=True,
                ).start()
            logging.info(f"Signal saved awaiting entry: {symbol} {direction} (ID: {sig_id})")
            return sig_id, learning_id
        except Exception as e:
            if "locked" in str(e).lower() and _attempt < 4:
                logging.warning(f"save_signal locked, retry {_attempt+1}...")
                time.sleep(1 + _attempt)
                continue
            logging.error(f"save_signal: {e}")
            break
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


def get_similar_patterns(symbol, direction, timeframe, regime="UNKNOWN", confluence=0):
    """Публичная обёртка pattern history для bot.py.

    Приватные имена не импортируются через ``from market import *``, из-за
    чего MTF раньше всегда молча терял исторический контекст.
    """
    try:
        return _learn_patterns(symbol, direction, timeframe, regime, confluence)
    except Exception:
        return {"found": False, "samples": 0}


def should_skip_symbol(symbol, direction):
    """Публичный доступ к межстратегийной защите для bot.py."""
    try:
        return _learn_should_skip(symbol, direction)
    except Exception:
        return False, ""

def check_pending_signals():
    """Проверяем открытые сигналы — сработал ли TP/SL"""
    try:
        conn = get_db_conn()
        pending = conn.execute(
            """SELECT id, symbol, direction, entry, tp1, tp2, tp3, sl,
                      timeframe, grade, created_at, signal_type, estimated_hours,
                      tp1_hit, trailing_sl, best_price, confluence, regime, learning_id
               FROM signals WHERE result='pending'"""
        ).fetchall()
        conn.close()

        # Expiry по стратегии (часы)
        _STRATEGY_EXPIRY = {"FAST": 4, "MTF": 72, "SWING": 96, "WYCKOFF": 504}
        # Trailing коэффициенты: после TP1 переносим SL на entry + X% от (tp1-entry)
        _TRAIL_COEFF = {"FAST": 0.3, "MTF": 0.4, "SWING": 0.5, "WYCKOFF": 0.5, "ZONE": 0.4}

        closed = []
        prices = get_live_prices()
        for row in pending:
            (sig_id, symbol, direction, entry, tp1, tp2, tp3, sl, timeframe,
             grade, created_at, signal_type, estimated_hours, tp1_hit_flag,
             trailing_sl, best_price, confluence, regime, learning_id_value) = row
            tp1_hit_flag = tp1_hit_flag or 0
            current = None
            if symbol in prices:
                current = prices[symbol]["price"]
            # Fallback на brain_router если live_prices недоступен
            if current is None and _ROUTER_OK:
                try:
                    rc = _brain_router.candles(symbol, "1h", 3)
                    if rc and len(rc) >= 1:
                        current = rc[-1]["close"]
                        logging.debug(f"check_pending_signals: {symbol} цена через router {current}")
                except Exception as _re:
                    logging.debug(f"check_pending_signals router fallback {symbol}: {_re}")
            if current is None:
                continue
            created = datetime.fromisoformat(created_at)
            hours_elapsed = (datetime.now() - created).total_seconds() / 3600

            lifecycle_state = _LIFECYCLE_ACTIVE
            lifecycle_activated_at = None
            if _SIGNAL_LIFECYCLE_OK:
                try:
                    _lc = get_db_conn(timeout=10)
                    lifecycle_state = _lifecycle_state_for(_lc, sig_id)
                    lifecycle_activated_at = _lifecycle_activated_at_for(_lc, sig_id)
                    _lifecycle_touch(_lc, sig_id)
                    _lc.commit()
                    _lc.close()
                except Exception as _lc_error:
                    logging.warning("[SignalLifecycle] state read %s: %s", sig_id, _lc_error)

            # Price-path learning begins only after the advertised entry was activated.
            if _MARKET_MEMORY_OK and lifecycle_state == _LIFECYCLE_ACTIVE:
                _memory_record_price(sig_id, current)

            # The current 5m candle can include price action from before the
            # signal was delivered or activated.  Candle high/low becomes safe
            # only after one full five-minute boundary since activation.
            interval_low = interval_high = current
            use_interval = lifecycle_state == _LIFECYCLE_ACTIVE
            if lifecycle_activated_at:
                try:
                    activated = datetime.fromisoformat(lifecycle_activated_at)
                    use_interval = (datetime.now() - activated).total_seconds() >= 300
                except (TypeError, ValueError):
                    use_interval = False
            if use_interval:
                try:
                    _obs = get_candles(symbol, "5m", 3)
                    if _obs:
                        interval_low = min(float(_obs[-1]["low"]), current)
                        interval_high = max(float(_obs[-1]["high"]), current)
                except Exception:
                    pass

            _sig_type_check = (signal_type or "").upper()
            _expiry_h = _STRATEGY_EXPIRY.get(_sig_type_check, estimated_hours or 72)

            if lifecycle_state == _LIFECYCLE_WAITING:
                invalidated = (
                    direction == "BULLISH" and current <= sl
                ) or (
                    direction == "BEARISH" and current >= sl
                )
                target_passed = (
                    direction == "BULLISH" and current >= tp1
                ) or (
                    direction == "BEARISH" and current <= tp1
                )
                if invalidated or target_passed or hours_elapsed > _expiry_h:
                    reason = (
                        "stop_reached_before_confirmed_entry" if invalidated else
                        "target_reached_without_entry" if target_passed else
                        "entry_not_filled_before_expiry"
                    )
                    _cc = get_db_conn(timeout=10)
                    _cc.execute(
                        "UPDATE signals SET result='cancelled', closed_at=CURRENT_TIMESTAMP WHERE id=?",
                        (sig_id,),
                    )
                    if _SIGNAL_LIFECYCLE_OK:
                        _lifecycle_mark_finished(_cc, sig_id, _LIFECYCLE_CANCELLED, reason)
                    _cc.commit(); _cc.close()
                    logging.info("[SignalLifecycle] %s cancelled unfilled: %s", symbol, reason)
                    closed.append({
                        "signal_id": sig_id, "symbol": symbol, "result": "cancelled",
                        "hours": round(hours_elapsed, 1), "grade": grade,
                        "is_win": False, "reason": reason,
                    })
                    continue

                if not _lifecycle_entry_touched(
                    direction, entry, current=current
                ):
                    continue

                # Start the learning record only after the advertised entry is
                # observed.  This removes rejected/unselected/unfilled rows
                # from Groq's ground truth.
                new_learning_id = None
                if _LEARNING_OK:
                    try:
                        new_learning_id = _learn_save_signal(
                            symbol, direction, grade, entry, sl, tp1, tp2, tp3,
                            timeframe, confluence or 0, regime or "UNKNOWN", signal_type or "",
                        )
                    except Exception as _learning_start_error:
                        logging.warning("[SignalLifecycle] learning activation %s: %s", symbol, _learning_start_error)
                _ac = get_db_conn(timeout=10)
                _ac.execute(
                    "UPDATE signals SET learning_id=? WHERE id=?",
                    (new_learning_id, sig_id),
                )
                if _SIGNAL_LIFECYCLE_OK:
                    _lifecycle_mark_active(_ac, sig_id)
                _ac.commit(); _ac.close()
                logging.info("[SignalLifecycle] %s entry activated at %s", symbol, entry)
                # Never infer entry→TP/SL ordering from the activation bar.
                continue

            result = None
            hit_tp = None
            _active_sl = trailing_sl if trailing_sl else sl

            # ── Trailing Stop Logic ──
            if tp1_hit_flag:
                # TP1 уже достигнут — отслеживаем best_price и trailing SL → TP2.
                # Используем high/low интерва, а не только текущий snapshot.
                _bp = best_price or entry
                if direction == "BULLISH":
                    _bp = max(_bp, interval_high)
                else:
                    _bp = min(_bp, interval_low)

                _hits = _lifecycle_barrier_hits(
                    direction, _active_sl, tp1, tp2, interval_low, interval_high
                )
                if _hits["sl"] and _hits["tp2"]:
                    # Порядок внутри 5m свечи неизвестен: не завышаем WR.
                    result, hit_tp = "tp1", 1
                elif _hits["tp2"]:
                    result, hit_tp = "tp2", 2
                elif _hits["sl"]:
                    # TP1 уже зафиксирован, trailing-выход не является SL.
                    result, hit_tp = "tp1", 1

                # Обновляем best_price и trailing_sl
                _trail_c = _TRAIL_COEFF.get(_sig_type_check, 0.4)
                if direction == "BULLISH":
                    _new_trail = _bp - abs(tp1 - entry) * _trail_c
                    if not trailing_sl or _new_trail > trailing_sl:
                        trailing_sl = round(_new_trail, 8)
                else:
                    _new_trail = _bp + abs(entry - tp1) * _trail_c
                    if not trailing_sl or _new_trail < trailing_sl:
                        trailing_sl = round(_new_trail, 8)

                # Сохраняем trailing state
                try:
                    _tc = get_db_conn(timeout=10)
                    _tc.execute("UPDATE signals SET best_price=?, trailing_sl=? WHERE id=?", (_bp, trailing_sl, sig_id))
                    _tc.commit()
                    _tc.close()
                except Exception:
                    pass
            else:
                # TP1 ещё не достигнут. При одновременном касании SL и TP
                # внутри одной 5m свечи засчитываем SL: иначе WR будет завышен.
                _hits = _lifecycle_barrier_hits(
                    direction, sl, tp1, tp2, interval_low, interval_high
                )
                if _hits["sl"]:
                    result = "sl"
                elif _sig_type_check == "FAST":
                    if _hits["tp2"]:
                        result, hit_tp = "tp2", 2
                    elif _hits["tp1"]:
                        result, hit_tp = "tp1", 1
                elif _hits["tp2"]:
                    # Сигнал был активен до этого интерва, поэтому TP2
                    # невозможен без предварительного прохода TP1.
                    result, hit_tp = "tp2", 2
                elif _hits["tp1"]:
                    # TP1 hit — НЕ закрываем, включаем trailing.
                    hit_tp = 1
                    _trail_c = _TRAIL_COEFF.get(_sig_type_check, 0.4)
                    if direction == "BULLISH":
                        _new_trail_sl = round(entry + abs(tp1 - entry) * _trail_c, 8)
                        _new_best = interval_high
                    else:
                        _new_trail_sl = round(entry - abs(entry - tp1) * _trail_c, 8)
                        _new_best = interval_low
                    try:
                        _tc = get_db_conn(timeout=10)
                        _tc.execute(
                            "UPDATE signals SET tp1_hit=1, trailing_sl=?, best_price=? WHERE id=?",
                            (_new_trail_sl, _new_best, sig_id),
                        )
                        _tc.commit()
                        _tc.close()
                        logging.info(f"[Trailing] {symbol} TP1 hit! Trail SL → {_new_trail_sl}")
                    except Exception:
                        pass
                    closed.append({
                        "signal_id": sig_id, "symbol": symbol,
                        "result": "tp1_hit", "hours": round(hours_elapsed, 1),
                        "grade": grade, "is_win": False,
                        "trailing_sl": _new_trail_sl, "tp2": tp2,
                        "entry": entry, "direction": direction,
                    })
                    # Не даём expiry в том же цикле отменить trailing.
                    continue

            # Expiry: используем estimated_hours или стратегию, fallback 72ч
            _sig_type = (signal_type or "").upper()
            _expiry_h = _STRATEGY_EXPIRY.get(_sig_type, estimated_hours or 72)
            if not result and hours_elapsed > _expiry_h:
                result = "expired"

            if result:
                if _MARKET_MEMORY_OK:
                    _memory_close_snapshot(sig_id, result, current)
                if _OUTCOME_LOOP_OK:
                    try:_close_learning_loop(sig_id,result,DB_PATH)
                    except Exception as exc:logging.debug("[ClosedLoop] close %s: %s",sig_id,exc)
                conn2 = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
                conn2.execute(
                    "UPDATE signals SET result=?, closed_at=CURRENT_TIMESTAMP WHERE id=?",
                    (result, sig_id)
                )
                if _SIGNAL_LIFECYCLE_OK:
                    _lifecycle_mark_finished(conn2, sig_id, "closed", result)
                # Сохраняем learning_id если есть
                learning_id = conn2.execute(
                    "SELECT learning_id FROM signals WHERE id=?", (sig_id,)
                ).fetchone()
                conn2.commit()
                conn2.close()

                is_win = result in ("tp1", "tp2", "tp3")
                update_signal_learning(symbol, hours_elapsed, is_win, timeframe, result)
                if result != "expired":
                    try:
                        _learn_streak(result)
                        _closed_rr = abs(tp1 - entry) / max(abs(entry - sl), 0.0001)
                        _learn_grade_acc(grade or signal_type or "UNKNOWN", result, _closed_rr if is_win else -1.0)
                    except Exception:
                        pass

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

                # Expired — нейтральный исход: он не должен создавать
                # анти-правило и ухудшать статистику как настоящий SL.
                if result != "expired":
                    asyncio.create_task(signal_reflection(
                        symbol, direction, entry, sl, tp1, result, hours_elapsed, timeframe
                    ))
                    candles = get_candles(symbol, timeframe, 50)
                    asyncio.create_task(self_learn_from_signal(
                        symbol, direction, entry, result, hours_elapsed, timeframe, candles, _sig_type_check
                    ))

                # Глубокий анализ ошибок только для проигрышей
                if not is_win and result != "expired":
                    asyncio.create_task(deep_error_analysis(
                        sig_id, symbol, direction, entry, sl, result, hours_elapsed, timeframe
                    ))

                # FIX 9: Smart feedback loop при SL
                if result == "sl":
                    try:
                        _err_conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
                        _sl_pct = round(abs(entry - sl) / entry * 100, 2) if entry else 0

                        # Собираем полный контекст
                        _sl_ap = get_adaptive_params(symbol)
                        _sl_regime = "UNKNOWN"
                        try:
                            _sl_reg = detect_market_regime_v2(symbol)
                            _sl_regime = _sl_reg.get("regime", "UNKNOWN")
                        except Exception:
                            pass
                        _sl_hour = datetime.now().hour

                        _sl_details = (
                            f"SL hit after {round(hours_elapsed,1)}h | SL%={_sl_pct}% | TF={timeframe} | "
                            f"Type={_sig_type_check} | ADX={_sl_ap['adx']} | VF={_sl_ap['volatility_factor']} | "
                            f"Regime={_sl_regime} | Hour={_sl_hour}"
                        )
                        _err_conn.execute(
                            """INSERT INTO bot_errors
                               (signal_id, symbol, direction, entry, sl, result, error_type,
                                error_description, market_context, created_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                            (sig_id, symbol, direction, entry, sl, result, "SL_HIT",
                             _sl_details, _sl_details)
                        )
                        _err_conn.commit()
                        _err_conn.close()

                        # Groq анализ паттерна SL — создаёт self_rule
                        try:
                            _sl_prompt = (
                                f"Сигнал {symbol} {direction} {_sig_type_check} закрылся по SL через {round(hours_elapsed,1)}ч.\n"
                                f"Контекст: ADX={_sl_ap['adx']}, VF={_sl_ap['volatility_factor']}, Regime={_sl_regime}, Hour={_sl_hour}\n"
                                f"SL%={_sl_pct}%, TF={timeframe}\n"
                                'Ответь JSON: {"rule": "правило в 1 предложении как избежать подобного SL", "confidence": 0.5-1.0}'
                            )
                            _sl_resp = ask_groq(_sl_prompt, max_tokens=80)
                            if _sl_resp:
                                import json as _j6, re as _r6
                                _m6 = _r6.search(r'\{[^}]+\}', _sl_resp, _r6.DOTALL)
                                if _m6:
                                    _p6 = _j6.loads(_m6.group())
                                    if _p6.get("rule"):
                                        _rc = sqlite3.connect("brain.db", timeout=10, check_same_thread=False)
                                        _rc.execute(
                                            """INSERT INTO self_rules
                                               (category, rule, rule_type, rule_text, confidence, source,
                                                symbol, direction, strategy, active, created_at, updated_at)
                                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
                                            ("avoid", _p6["rule"], "avoid", _p6["rule"],
                                             float(_p6.get("confidence", 0.6)),
                                             f"SL_feedback_{_sig_type_check}", symbol, direction,
                                             _sig_type_check)
                                        )
                                        _rc.commit()
                                        _rc.close()
                                        logging.info(f"[Feedback] New rule from SL: {_p6['rule'][:60]}")
                        except Exception:
                            pass

                    except Exception as _err_e:
                        logging.debug(f"[FIX9] bot_errors write: {_err_e}")

                # Сохраняем паттерн для обучения (pattern_history)
                try:
                    _learn_save_pattern(
                        symbol, direction, timeframe,
                        _regime_val, _confluence_val,
                        result, rr=round(abs(tp1 - entry) / max(abs(entry - sl), 0.0001), 2)
                    )
                except Exception as _sp_e:
                    logging.debug(f"save_pattern: {_sp_e}")

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
    """Close a matching legacy learning row without fabricating trade levels."""
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
            logging.warning(
                "[Learning] no activated record for %s; result %s is not synthesized",
                symbol, result,
            )
    except Exception as e:
        logging.warning(f"_learn_analyze_by_symbol: {e}")


def update_signal_learning(symbol, hours_to_close, is_win, timeframe, result):
    try:
        if result in ("expired", "cancelled", "ai_rejected", "ai_wait"):
            return
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
                """INSERT INTO signal_learning
                (symbol, total, wins, losses, avg_hours_to_tp, best_timeframe, worst_timeframe, win_rate, last_analysis)
                VALUES (?,1,?,?,?,?,?,?,?)""",
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
        conn.execute("INSERT INTO knowledge (topic, content, source, created_at) VALUES (?,?,?,CURRENT_TIMESTAMP)", (topic, content, source))
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


def save_self_rule(category, rule, confidence=0.5, source="auto", symbol="", direction="", strategy=""):
    """Сохранить новое правило или обновить существующее"""
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        existing = conn.execute(
            """SELECT id, confidence, confirmed_by FROM self_rules
               WHERE rule LIKE ? AND category=? AND COALESCE(symbol, '')=?
                 AND COALESCE(direction, '')=? AND COALESCE(strategy, '')=?""",
            (f"%{rule[:50]}%", category, symbol, direction, strategy)
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
                """INSERT OR IGNORE INTO self_rules
                   (category, rule, rule_type, rule_text, confidence, source, active, symbol, direction, strategy)
                   VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
                (category, rule, category.lower(), rule, confidence, source, symbol, direction, strategy)
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


async def self_learn_from_signal(symbol, direction, entry, result, hours, timeframe, candles, strategy=""):
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
                try:
                    confidence = float(str(data.get("confidence", 0.5)).split()[0])
                except (ValueError, TypeError):
                    confidence = 0.5

                if rule and len(rule) > 10:
                    if is_win:
                        save_self_rule("prefer", rule, confidence, f"win_{symbol}", symbol, direction, strategy)
                    else:
                        # При проигрыше сохраняем как анти-паттерн
                        save_self_rule("avoid", f"ИЗБЕГАТЬ: {rule}", confidence * 0.8, f"loss_{symbol}", symbol, direction, strategy)
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
        candles = get_confirmed_candles(get_candles(symbol, timeframe, 201))
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
        conn.execute("""INSERT INTO bot_errors
            (signal_id, symbol, direction, entry, sl, result,
             error_type, error_description, ai_analysis, ai_lesson, ai_next_time,
             fixed, fix_description, hours_in_trade, market_context, created_at, fixed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,0,NULL,?,?,CURRENT_TIMESTAMP,NULL)""",
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
                """INSERT INTO error_patterns
                   (pattern, symbol, timeframe, conditions, sl_count, last_seen, active, error_type, count, rule_added)
                   VALUES (?,?,?,?,?,CURRENT_TIMESTAMP,1,?,1,NULL)""",
                (error_type, "", "", "{}", 1, error_type)
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


def get_recent_errors(symbol=None, limit=10):
    """Возвращает последние ошибки из bot_errors для контекста Groq промптов"""
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        if symbol:
            rows = conn.execute(
                """SELECT error_type, symbol, direction,
                           COALESCE(NULLIF(error_description, ''), NULLIF(ai_lesson, ''),
                                    NULLIF(ai_analysis, ''), NULLIF(market_context, ''), '') AS details,
                           created_at
                    FROM bot_errors WHERE symbol=? ORDER BY rowid DESC LIMIT ?""",
                (symbol, limit,)
            ).fetchall()
            if not rows:
                rows = conn.execute(
                    """SELECT error_type, symbol, direction,
                               COALESCE(NULLIF(error_description, ''), NULLIF(ai_lesson, ''),
                                        NULLIF(ai_analysis, ''), NULLIF(market_context, ''), '') AS details,
                               created_at
                        FROM bot_errors ORDER BY rowid DESC LIMIT ?""",
                    (limit,)
                ).fetchall()
        else:
            rows = conn.execute(
                """SELECT error_type, symbol, direction,
                           COALESCE(NULLIF(error_description, ''), NULLIF(ai_lesson, ''),
                                    NULLIF(ai_analysis, ''), NULLIF(market_context, ''), '') AS details,
                           created_at
                    FROM bot_errors ORDER BY rowid DESC LIMIT ?""",
                (limit,)
            ).fetchall()
        conn.close()
        if not rows:
            return ""
        lines = []
        for r in rows:
            lines.append(f"{r[0]}: {r[1]} {r[2]} — {r[3]}")
        return "\nПоследние ошибки бота:\n" + "\n".join(lines)
    except Exception:
        return ""


def get_relevant_rules(symbol: str, direction: str, strategy: str = "", limit: int = 5) -> str:
    """Получить релевантные правила самообучения для Groq промптов"""
    try:
        import sqlite3 as _sq
        _c = _sq.connect("brain.db", timeout=5)
        rows = _c.execute("""
            SELECT rule_text FROM self_rules
            WHERE active=1
            AND rule_type IN ('avoid', 'prefer', 'timing', 'risk', 'auto')
            AND (rule_text LIKE ? OR rule_text LIKE ? OR rule_text LIKE '%' || ? || '%'
                 OR COALESCE(symbol, '')=? OR COALESCE(direction, '')=?)
            AND (COALESCE(strategy, '')='' OR COALESCE(strategy, '')=?)
            ORDER BY confidence DESC LIMIT ?
        """, (f"%{symbol}%", f"%{direction}%", direction, symbol, direction, strategy, limit)).fetchall()
        if not rows:
            rows = _c.execute("""
                SELECT rule_text FROM self_rules
                WHERE active=1 AND rule_type IN ('avoid', 'prefer', 'timing', 'risk', 'auto')
                  AND confidence >= 0.8
                  AND (COALESCE(strategy, '')='' OR COALESCE(strategy, '')=?)
                ORDER BY confidence DESC LIMIT ?
            """, (strategy, limit)).fetchall()
        _c.close()
        if not rows:
            return ""
        rules_text = "\nПРАВИЛА САМООБУЧЕНИЯ:\n"
        for r in rows:
            if r[0]:
                rules_text += f"- {r[0][:100]}\n"
        return rules_text
    except Exception:
        return ""


async def auto_add_rule(error_type, count):
    """Когда ошибка повторяется 3+ раз — AI ищет паттерн и формулирует правило"""
    try:
        conn = sqlite3.connect("brain.db", timeout=30, check_same_thread=False)
        
        # Достаём последние 5 анализов этого типа
        rows = conn.execute(
            "SELECT symbol, direction, ai_analysis, ai_lesson, market_context FROM bot_errors WHERE error_type=? ORDER BY id DESC LIMIT 5",
            (error_type,)
        ).fetchall()
        
        # Достаём ВСЕ типы ошибок для поиска паттернов
        all_errors = conn.execute(
            "SELECT error_type, count FROM error_patterns ORDER BY count DESC LIMIT 10"
        ).fetchall()
        
        # Проверяем паттерн direction — все ошибки в одну сторону?
        directions = [r[1] for r in rows if r[1]]
        direction_pattern = ""
        if directions and len(set(directions)) == 1:
            direction_pattern = f"Все {count} ошибок в направлении {directions[0]}. "
        
        # Проверяем паттерн символов
        symbols = [r[0] for r in rows]
        symbol_pattern = f"Частые символы: {', '.join(set(symbols))}. " if symbols else ""
        
        # Контекст рынка
        contexts = [r[4] for r in rows if r[4]]
        market_pattern = contexts[0][:100] if contexts else ""
        
        all_errors_text = ", ".join([f"{ERROR_TYPES.get(e[0], e[0])}: {e[1]}x" for e in all_errors])
        examples = "\n".join([f"- {r[0]} {r[1]}: {r[2][:80]}" for r in rows])

        prompt = f"""Ты анализируешь паттерны ошибок торгового бота.

ГЛАВНАЯ ОШИБКА: "{ERROR_TYPES.get(error_type, error_type)}" повторилась {count} раз.
ВСЕ ОШИБКИ БОТА: {all_errors_text}

ПАТТЕРН: {direction_pattern}{symbol_pattern}
КОНТЕКСТ РЫНКА: {market_pattern}

ПРИМЕРЫ ОШИБОК:
{examples}

Найди КОРНЕВУЮ ПРИЧИНУ паттерна ошибок (не симптом).
Сформулируй ОДНО конкретное правило которое исправит корень проблемы.
Начни с "Не входить если..." или "Фильтровать..." или "Проверять..."
Максимум 1 предложение."""

        rule = ask_groq(prompt, max_tokens=120)
        conn.close()
        return rule.strip() if rule else None
    except:
        return None


def generate_signal_comment(symbol, direction, mtf, confluence_score, regime, fg, funding, ob, fvg, brain_ctx="", entry=None, sl=None, tp1=None, timeframe=None):
    """Короткий AI-комментарий к сигналу — с учётом накопленного опыта"""
    try:
        # Определяем таймфрейм из mtf dict или параметра
        tf_label = timeframe or ""
        if not tf_label and isinstance(mtf, dict):
            tf_label = mtf.get("timeframe", mtf.get("tf", ""))
        tf_text = f" | ТФ: {tf_label}" if tf_label else ""

        # Конкретный паттерн входа (не общие фразы)
        pattern_parts = []
        if ob:
            ob_dir = "медвежий" if direction == "BEARISH" else "бычий"
            pattern_parts.append(f"{ob_dir} OB {ob['bottom']:.4f}–{ob['top']:.4f}")
        if fvg:
            pattern_parts.append(f"FVG {fvg['bottom']:.4f}–{fvg['top']:.4f}")
        if fg:
            pattern_parts.append(f"F&G={fg['value']} ({fg['label']})")
        if funding is not None:
            pattern_parts.append(f"FR {funding:+.4f}%")
        if regime:
            regime_mode = regime.get("mode", str(regime)) if isinstance(regime, dict) else str(regime)
            pattern_parts.append(f"режим {regime_mode}")

        pattern_text = ", ".join(pattern_parts) if pattern_parts else "нет доп. факторов"
        past_errors = get_knowledge(f"error_{symbol}")

        brain_section = f"\nМОЙ НАКОПЛЕННЫЙ ОПЫТ:\n{brain_ctx[:400]}" if brain_ctx else ""
        errors_section = f"\nПРОШЛЫЕ ОШИБКИ ПО {symbol}: {past_errors[:200]}" if past_errors else ""

        # Уровни входа — обязательно конкретные цены
        levels_section = ""
        if entry and sl and tp1:
            _rr = abs(tp1 - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
            _sl_pct = abs(entry - sl) / entry * 100 if entry > 0 else 0
            _tp_pct = abs(tp1 - entry) / entry * 100 if entry > 0 else 0
            levels_section = (
                f"\nВход: {entry} | SL: {sl} (-{_sl_pct:.2f}%) | TP1: {tp1} (+{_tp_pct:.2f}%) | RR: {_rr:.1f}"
            )

        # OB/FVG уровни отдельно для анализа
        zones_section = ""
        if ob:
            zones_section += f"\nOB зона: {ob['bottom']:.6f} – {ob['top']:.6f}"
        if fvg:
            zones_section += f"\nFVG зона: {fvg['bottom']:.6f} – {fvg['top']:.6f}"

        prompt = f"""Отвечай ТОЛЬКО на русском языке, без иероглифов и символов других языков.

Ты APEX — торговый бот. Анализируй КОНКРЕТНЫЙ паттерн входа, не общие фразы.

Сигнал: {symbol} {direction}{tf_text} | Скор: {confluence_score}/100
Паттерн: {pattern_text}{levels_section}{zones_section}{brain_section}{errors_section}

Напиши 2-3 предложения на русском:
1. Какой конкретный паттерн (OB, FVG, sweep, CHoCH) и на каком уровне цены
2. Что знаешь об этой монете из опыта (если есть)
3. Ключевой риск этой конкретной сделки

Только русский язык. Конкретные цены и уровни. Без воды и общих фраз."""

        comment = ask_groq(prompt, max_tokens=200)
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

# Словарь блокировок ключей: {key_index: timestamp когда получил rate limit}
_key_rate_limited: dict = {}


def legacy_strategy_groq_enabled() -> bool:
    """Keep only the central post-strategy quality gate enabled by default.

    Older deployments can temporarily restore per-strategy AI calls with
    LEGACY_STRATEGY_GROQ=true.  Deterministic strategy calculations and
    mandatory filters are never conditional on this flag.
    """
    return os.environ.get("LEGACY_STRATEGY_GROQ", "false").strip().lower() in {
        "1", "true", "yes", "on",
    }

def ask_groq(prompt, max_tokens=800):
    """
    Умный запрос к Groq с быстрой ротацией ключей:
    - При rate limit сразу помечает ключ на 60с и берёт следующий
    - Не ждёт — мгновенно переключается
    - Использует все доступные ключи
    """
    global _last_ai_call, _groq_key_index

    # Сокращаем промпт если больше 6000 символов
    if len(prompt) > 6000:
        prompt = prompt[:5000] + "\n[промпт сокращён для экономии токенов]"

    models = configured_groq_models()

    active_keys = [k for k in GROQ_KEYS if k]
    if not active_keys:
        logging.error("Groq: нет активных ключей")
        return None

    rate_limited = 0
    tried_request = False
    for model in models:
        # A 404 is tied to the model, not to an individual API key.  Trying it
        # with every key only creates misleading "all keys exhausted" logs.
        model_unavailable = False
        for offset in range(len(active_keys)):
            key_index = (_groq_key_index + offset) % len(active_keys)
            if time.time() - _key_rate_limited.get(key_index, 0) < 60:
                rate_limited += 1
                continue
            tried_request = True
            try:
                client = Groq(api_key=active_keys[key_index])
                r = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    timeout=30,
                )
                _track_tokens(len(prompt) // 4 + max_tokens)
                _groq_key_index = (key_index + 1) % len(active_keys)
                return r.choices[0].message.content
            except Exception as e:
                err_str = str(e).lower()
                if is_model_unavailable_error(e):
                    logging.warning("Groq model %s недоступна; пробую fallback", model)
                    model_unavailable = True
                    break
                if "rate_limit" in err_str or "429" in err_str or "rate limit" in err_str:
                    _key_rate_limited[key_index] = time.time()
                    rate_limited += 1
                    logging.warning("Groq rate limit ключ %s — блокирую на 60с", key_index + 1)
                    continue
                if "401" in err_str or "403" in err_str or "invalid api key" in err_str:
                    logging.error("Groq ключ %s отклонён или не имеет доступа", key_index + 1)
                    continue
                logging.error("Groq error (ключ %s, модель %s): %s", key_index + 1, model, e)
        if model_unavailable:
            continue

    if rate_limited and rate_limited >= len(active_keys) and tried_request:
        logging.error("Groq: все доступные ключи получили rate limit; повтор через 60с")
    else:
        logging.error("Groq недоступен: проверьте GROQ_MODEL/доступ ключа; ключи не помечены исчерпанными")
    return None

# ===== APEX BRAIN v2 — АВТОНОМНОЕ САМООБУЧЕНИЕ =====
# Бот постоянно растёт: читает рынок, запоминает паттерны, строит модель мира

# Кэш для тяжёлых вызовов — не грузим Groq каждый раз
_groq_cache = {}
_groq_cache_time = {}
GROQ_CACHE_TTL = 300  # 5 минут

# Кулдаун убран — ротация ключей справляется без глобального ожидания
AI_COOLDOWN = 0
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

        total_chars = sum(len(str(r)) for r in web_results)
        facts_text = "\n\n".join(str(r) if not isinstance(r, str) else r for r in web_results)
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
                    try:
                        confidence = float(str(data.get("confidence", 0.5)).split()[0])
                    except (ValueError, TypeError):
                        confidence = 0.5
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

        # Уведомление отключено — только лог
        if new_insights:
            logging.info(f"[Обучение] {len(new_insights)} новых знаний")

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
        rules_before = len(await asyncio.to_thread(get_self_rules) or [])
        # self_research_loop is legacy async code whose body is synchronous.
        # Run it on a worker loop so SQLite work cannot stall APScheduler.
        await asyncio.to_thread(lambda: asyncio.run(self_research_loop()))
        rules_after_rows = await asyncio.to_thread(get_self_rules) or []
        rules_after = len(rules_after_rows)
        new_rules = rules_after - rules_before

        # 2. Исследование темы
        old_knowledge, new_analysis = await asyncio.gather(
            asyncio.to_thread(get_knowledge, topic),
            asyncio.to_thread(deep_research, topic),
        )

        comparison = ""
        if new_analysis and old_knowledge:
            comparison_prompt = f"""Сравни старый и новый анализ по теме: {topic}

СТАРЫЙ: {old_knowledge[:400]}
НОВЫЙ: {new_analysis[:400]}

Что изменилось? Прошлый прогноз сбылся? Какое правило нужно добавить в стратегию?
Ответ: 2-3 предложения + одно конкретное правило."""

            comparison = await asyncio.to_thread(ask_groq, comparison_prompt, max_tokens=200)
            if comparison:
                await asyncio.to_thread(save_knowledge, f"comparison_{topic}", comparison, "self-compare")
                # Если нашли правило — сохраняем
                if "правило" in comparison.lower() or "избегать" in comparison.lower():
                    await asyncio.to_thread(
                        save_self_rule, "market", comparison[:150], 0.6, "night_comparison",
                    )

        # 3. Обновляем модели топ-монет
        top_coins = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
        for sym in top_coins:
            try:
                candles = await asyncio.to_thread(get_candles, sym, "4h", 50)
                if candles:
                    await asyncio.to_thread(update_market_model, sym, candles, "NEUTRAL")
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

            # Уведомление отключено — только лог
            logging.info(f"[NightBrain] {msg[:100]}")

        logging.info(f"Ночная задача выполнена. Новых правил: {new_rules}, всего: {rules_after}")

        # brain.db backup is owned by bot.py's single bounded scheduler. Keeping
        # one writer prevents duplicate commits and main-branch pollution.

        # Очистка старых timing_queue записей (старше 24 часов)
        try:
            _cleanup_conn = sqlite3.connect("brain.db", timeout=10, check_same_thread=False)
            _tq_deleted = _cleanup_conn.execute(
                "DELETE FROM timing_queue WHERE status='waiting' AND created_at < datetime('now', '-24 hours')"
            ).rowcount
            # Очистка barrier_log — оставляем только последние 1000 записей
            _cleanup_conn.execute(
                "DELETE FROM barrier_log WHERE id NOT IN (SELECT id FROM barrier_log ORDER BY id DESC LIMIT 1000)"
            )
            _cleanup_conn.commit()
            _cleanup_conn.close()
            if _tq_deleted > 0:
                logging.info(f"[Cleanup] timing_queue: удалено {_tq_deleted} старых записей")
            logging.info("[Cleanup] barrier_log очищен до 1000 записей")
        except Exception as _cleanup_e:
            logging.warning(f"[Cleanup] {_cleanup_e}")

    except Exception as e:
        logging.error(f"Night brain error: {e}")


# ===== СИСТЕМА 4: УМНЫЙ ASK_AI С АВТО-РЕСЁРЧЕМ =====

def get_price_realtime(symbol="BTCUSDT"):
    """Get the current Gate USD-M price with a non-exchange fallback."""
    try:
        from external_sources.pair_registry import get_pair
        contract = str(get_pair(symbol).get("gate_symbol") or symbol.replace("USDT", "_USDT"))
        r = requests.get(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers",
            params={"contract": contract},
            headers={"User-Agent": "APEX-SMC/1.0"},
            timeout=8
        )
        r.raise_for_status()
        data = r.json()
        row = data[0] if isinstance(data, list) and data else {}
        price = float(row.get("last") or row.get("mark_price") or 0)
        if price > 0:
            return {
                "price": price,
                "change": round(float(row.get("change_percentage") or 0), 2),
                "source": "Gate.io Futures",
            }
    except Exception:
        pass

    cg_id = COINGECKO_IDS.get(symbol)
    try:
        if not cg_id:
            raise ValueError(f"No CoinGecko mapping for {symbol}")
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd", "include_24hr_change": "true"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=8
        )
        r.raise_for_status()
        data = r.json()
        if cg_id in data:
            return {
                "price": data[cg_id]["usd"],
                "change": round(data[cg_id].get("usd_24h_change", 0), 2),
                "source": "CoinGecko",
            }
    except Exception:
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


def get_all_market_pairs():
    """
    Return the same Gate analysis universe used by scheduled scanners.
    """
    return get_top_pairs(DEFAULT_UNIVERSE_SIZE)



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

    # Живые цены берём с того же Gate USD-M venue, что и стратегии.
    live_prices_text = ""
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
                scan_result = (
                    _RAW_SCAN_HANDLER(found_symbol, "1h", False)
                    if _RAW_SCAN_HANDLER else None
                )
                if scan_result:
                    return scan_result.get("text") if isinstance(scan_result, dict) else scan_result
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

# ===== SWING SCANNER — торговля от экстремумов =====


def find_equal_highs_lows(candles, lookback=20, tolerance=0.002):
    """
    Находит Equal Highs (EQH) и Equal Lows (EQL) — уровни ликвидности.
    EQH = два одинаковых хая (±0.2%) = скопление стопов покупателей
    EQL = два одинаковых лоя (±0.2%) = скопление стопов продавцов
    """
    recent = candles[-lookback:]
    highs = [c["high"] for c in recent]
    lows  = [c["low"]  for c in recent]

    eqh_level = None
    eql_level = None

    # Ищем EQH — два хая в пределах tolerance
    for i in range(len(highs)-1):
        for j in range(i+1, len(highs)):
            if abs(highs[i] - highs[j]) / highs[i] <= tolerance:
                eqh_level = (highs[i] + highs[j]) / 2
                break
        if eqh_level:
            break

    # Ищем EQL — два лоя в пределах tolerance
    for i in range(len(lows)-1):
        for j in range(i+1, len(lows)):
            if abs(lows[i] - lows[j]) / lows[i] <= tolerance:
                eql_level = (lows[i] + lows[j]) / 2
                break
        if eql_level:
            break

    return eqh_level, eql_level

def get_bos_choch_event(
    candles: list,
    direction: str,
    lookback: int = 15,
    max_break_age: int = 1,
) -> dict | None:
    """Return a close-confirmed structural event matching ``direction``.

    ``lookback`` keeps its legacy meaning as the minimum amount of history,
    while centred pivots use a small, symmetric radius appropriate for the
    supplied LTF sample.  A BOS continues an established HH+HL/LH+LL trend;
    a CHoCH breaks against it.  Mixed structure returns ``None``.
    """
    try:
        if direction not in ("BULLISH", "BEARISH"):
            return None
        if not candles or len(candles) < max(lookback + 3, 12):
            return None

        swing_lookback = 3 if len(candles) >= 50 else 2
        structure = _analyze_market_structure(
            candles,
            swing_lookback=swing_lookback,
            max_break_age=max_break_age,
        )
        event = structure.get("event")
        if not event or event.get("direction") != direction:
            return None
        if event.get("type") not in ("BOS", "CHoCH") or not event.get("closed"):
            return None
        return event
    except Exception:
        return None


def detect_bos_choch(candles: list, direction: str, lookback: int = 15) -> bool:
    """Compatibility boolean for callers that do not need event metadata."""
    return get_bos_choch_event(candles, direction, lookback=lookback) is not None


@_audit_strategy("SWING")
def detect_swing_setup(symbol: str, timeframe: str = "4h") -> dict | None:
    """
    Ловит swing сетапы: sweep экстремума → CHoCH → вход.
    Логика: лоу пробит и закрылся выше (бычий sweep) → лонг
            хай пробит и закрылся ниже (медвежий sweep) → шорт
    """
    try:
        raw_candles = get_candles(symbol, timeframe, 101)
        candles = get_confirmed_candles(raw_candles)
        if _audit_test('SWING_DETECT_SWING_SETUP_G7123', (not candles or len(candles) < 20), 'not candles or len(candles) < 20', 'not candles or len(candles) < 20', 7123):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7124', 'not candles or len(candles) < 20', locals(), 'not candles or len(candles) < 20', 7124)

        live_price = raw_candles[-1]["close"]

        closes  = [c["close"] for c in candles]
        highs   = [c["high"]  for c in candles]
        lows    = [c["low"]   for c in candles]
        price   = closes[-1]
        last    = candles[-1]
        prev    = candles[-2]

        # ATR для фильтра и стопа
        atr = sum(highs[i] - lows[i] for i in range(-14, 0)) / 14
        _ap_sw = get_adaptive_params(symbol, candles)
        _vf_sw = _ap_sw["volatility_factor"]

        # ── Swing highs/lows (lookback=12) ──
        swing_highs, swing_lows = find_swings(candles, lookback=12)
        if _audit_test('SWING_DETECT_SWING_SETUP_G7142', (len(swing_highs) < 2 or len(swing_lows) < 2), 'Swing highs/lows (lookback=12)', 'len(swing_highs) < 2 or len(swing_lows) < 2', 7142):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7143', 'Swing highs/lows (lookback=12)', locals(), 'len(swing_highs) < 2 or len(swing_lows) < 2', 7143)

        # Берём последние 3 свинга
        recent_highs = sorted(swing_highs[-3:], key=lambda x: x[0])
        recent_lows  = sorted(swing_lows[-3:],  key=lambda x: x[0])

        last_swing_high = recent_highs[-1][1]
        last_swing_low  = recent_lows[-1][1]

        # Предыдущий свинг для цели
        prev_swing_high = recent_highs[-2][1]
        prev_swing_low  = recent_lows[-2][1]

        direction = None
        entry = sl = tp = None
        logic = ""
        trigger_candle = None
        trigger_lookback = 1

        # ── Проверяем последние 6 свечей на свежий sweep ──
        for lookback_i in range(1, 7):
            check      = candles[-lookback_i]
            check_prev = candles[-lookback_i - 1]

            # Свинги без последних свечей чтобы не учитывать текущее движение
            base_candles = candles[:-lookback_i] if lookback_i > 0 else candles
            if len(base_candles) < 20:
                continue
            sh, sl_list = find_swings(base_candles, lookback=5)
            if len(sh) < 2 or len(sl_list) < 2:
                continue

            rec_h    = sorted(sh[-3:],      key=lambda x: x[0])
            rec_l    = sorted(sl_list[-3:], key=lambda x: x[0])
            chk_high = rec_h[-1][1]
            chk_low  = rec_l[-1][1]
            prv_high = rec_h[-2][1]
            prv_low  = rec_l[-2][1]

            # Bullish sweep
            if (check["low"] < chk_low and
                    check["close"] > chk_low and
                    (chk_low - check["low"]) > atr * 0.1 and
                    check_prev["close"] < check_prev["open"]):
                direction = "BULLISH"
                entry = smart_round(check["close"])
                sl    = smart_round(check["low"] - atr * _vf_sw)
                tp    = smart_round(prv_high)
                logic = "свип лоу ↓ + возврат в диапазон + импульс вверх"
                trigger_candle = check
                trigger_lookback = lookback_i
                break

            # Bearish sweep
            if (check["high"] > chk_high and
                    check["close"] < chk_high and
                    (check["high"] - chk_high) > atr * 0.1 and
                    check_prev["close"] > check_prev["open"]):
                direction = "BEARISH"
                entry = smart_round(check["close"])
                sl    = smart_round(check["high"] + atr * _vf_sw)
                tp    = smart_round(prv_low)
                logic = "свип хая ↑ + отклонение + импульс вниз"
                trigger_candle = check
                trigger_lookback = lookback_i
                break

        # ── EQH/EQL как дополнительный триггер ──
        # Если обычный sweep не найден — проверяем есть ли sweep EQH/EQL
        if not direction:
            try:
                eqh_level, eql_level = find_equal_highs_lows(candles, lookback=30)
                last_c = candles[-1]
                prev_c = candles[-2]

                # Bullish: sweep EQL (выбитие двойного лоя с возвратом)
                if eql_level and last_c["low"] < eql_level and last_c["close"] > eql_level:
                    wick = (last_c["close"] - last_c["low"]) / (last_c["high"] - last_c["low"] + 0.000001)
                    if wick > 0.4:
                        direction = "BULLISH"
                        entry = smart_round(last_c["close"])
                        sl    = smart_round(last_c["low"] - atr * _vf_sw)
                        # TP = предыдущий хай свинга
                        tp    = smart_round(last_swing_high)
                        logic = f"EQL sweep — двойной лоу ${eql_level:.4f} выбит → разворот"
                        trigger_candle = last_c
                        trigger_lookback = 1

                # Bearish: sweep EQH (выбитие двойного хая с возвратом)
                elif eqh_level and last_c["high"] > eqh_level and last_c["close"] < eqh_level:
                    wick = (last_c["high"] - last_c["close"]) / (last_c["high"] - last_c["low"] + 0.000001)
                    if wick > 0.4:
                        direction = "BEARISH"
                        entry = smart_round(last_c["close"])
                        sl    = smart_round(last_c["high"] + atr * _vf_sw)
                        tp    = smart_round(last_swing_low)
                        logic = f"EQH sweep — двойной хай ${eqh_level:.4f} выбит → разворот"
                        trigger_candle = last_c
                        trigger_lookback = 1
            except Exception:
                pass

        # ── ВАРИАНТ 2: Реакция от OB/FVG без sweep ──
        if _audit_test('SWING_DETECT_SWING_SETUP_G7246', (not direction), 'ВАРИАНТ 2: Реакция от OB/FVG без sweep', 'not direction', 7246):
            try:
                _ob_sw = find_ob(candles, "BULLISH")
                _fvg_sw = find_fvg(candles, "BULLISH")
                _atr_sw = atr
                _price = candles[-1]["close"]

                # Bullish: цена у OB/FVG + бычья свеча отбоя
                if _ob_sw:
                    _in_ob = _ob_sw["bottom"] - _atr_sw*0.3 <= _price <= _ob_sw["top"] + _atr_sw*0.3
                    _bull_candle = candles[-1]["close"] > candles[-1]["open"]
                    _bull_body = abs(candles[-1]["close"]-candles[-1]["open"])
                    _bull_range = candles[-1]["high"]-candles[-1]["low"]
                    _bull_disp = _bull_body/_bull_range > 0.5 if _bull_range > 0 else False
                    if _in_ob and _bull_candle and _bull_disp:
                        direction = "BULLISH"
                        entry = smart_round(_price)
                        sl = smart_round(_ob_sw["bottom"] - _atr_sw * 0.5)
                        tp = smart_round(last_swing_high)
                        logic = f"Реакция от OB {smart_price_fmt(_ob_sw['bottom'])}–{smart_price_fmt(_ob_sw['top'])}"
                        trigger_candle = candles[-1]
                        trigger_lookback = 1

                # Bearish: цена у OB/FVG + медвежья свеча
                _ob_bear = find_ob(candles, "BEARISH")
                if not direction and _ob_bear:
                    _in_ob_b = _ob_bear["bottom"] - _atr_sw*0.3 <= _price <= _ob_bear["top"] + _atr_sw*0.3
                    _bear_candle = candles[-1]["close"] < candles[-1]["open"]
                    _bear_body = abs(candles[-1]["close"]-candles[-1]["open"])
                    _bear_range = candles[-1]["high"]-candles[-1]["low"]
                    _bear_disp = _bear_body/_bear_range > 0.5 if _bear_range > 0 else False
                    if _in_ob_b and _bear_candle and _bear_disp:
                        direction = "BEARISH"
                        entry = smart_round(_price)
                        sl = smart_round(_ob_bear["top"] + _atr_sw * 0.5)
                        tp = smart_round(last_swing_low)
                        logic = f"Реакция от OB {smart_price_fmt(_ob_bear['bottom'])}–{smart_price_fmt(_ob_bear['top'])}"
                        trigger_candle = candles[-1]
                        trigger_lookback = 1

                # Variant 2: минимум RR 2.0
                if _audit_test('SWING_DETECT_SWING_SETUP_G7287', (direction and entry and sl and tp), 'Variant 2: минимум RR 2.0', 'direction and entry and sl and tp', 7287):
                    _v2_risk = abs(entry - sl)
                    _v2_reward = abs(tp - entry)
                    if _audit_test('SWING_DETECT_SWING_SETUP_G7290', (_v2_risk > 0 and _v2_reward / _v2_risk < 2.0), 'Variant 2: минимум RR 2.0', '_v2_risk > 0 and _v2_reward / _v2_risk < 2.0', 7290):
                        logging.info(f"[SWING V2] {symbol}: RR {_v2_reward/_v2_risk:.2f} < 2.0 — пропуск")
                        return _audit_fail('SWING_DETECT_SWING_SETUP_R7292', 'Variant 2: минимум RR 2.0', locals(), '_v2_risk > 0 and _v2_reward / _v2_risk < 2.0', 7292)
            except Exception:
                pass

        # ── Reaction speed — цена должна быстро реагировать от зоны ──
        if direction:
            try:
                _reaction_candles = 0
                _react_ob = None
                if direction == "BULLISH":
                    _react_ob = find_ob(candles, "BULLISH")
                else:
                    _react_ob = find_ob(candles, "BEARISH")
                if _react_ob:
                    for _rc in candles[-5:]:
                        if direction == "BULLISH":
                            _in_zone_rc = _react_ob["bottom"] <= _rc["low"] <= _react_ob["top"] * 1.01
                        else:
                            _in_zone_rc = _react_ob["bottom"] * 0.99 <= _rc["high"] <= _react_ob["top"]
                        if _in_zone_rc:
                            _reaction_candles += 1

                    if _reaction_candles >= 4:
                        logging.debug(f"[SWING] {symbol}: цена тупит у зоны {_reaction_candles} свечей — слабый сетап")
                        direction = None
                        entry = None
            except Exception:
                pass

        # ── Liquidity pool рядом — понимаем куда пойдут стопы ��─
        if direction:
            try:
                eqh_level, eql_level = find_equal_highs_lows(candles, lookback=30)
                if direction == "BULLISH" and eqh_level and eqh_level > entry:
                    if eqh_level < tp:
                        tp = smart_round(eqh_level)
                    logic = logic + f" → ликвидность EQH {smart_price_fmt(eqh_level)}"
                elif direction == "BEARISH" and eql_level and eql_level < entry:
                    if eql_level > tp:
                        tp = smart_round(eql_level)
                    logic = logic + f" ��� ликвидность EQL {smart_price_fmt(eql_level)}"
            except Exception:
                pass

        if _audit_test('SWING_DETECT_SWING_SETUP_G7336', (not direction), 'not direction', 'not direction', 7336):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7337', 'not direction', locals(), 'not direction', 7337)

        try:
            _skip_symbol, _skip_reason = _learn_should_skip(symbol, direction)
            if _audit_test('SWING_DETECT_SWING_SETUP_G7341', (_skip_symbol), '_skip_symbol', '_skip_symbol', 7341):
                logging.info(f"[SWING] {symbol}: {_skip_reason}")
                return _audit_fail('SWING_DETECT_SWING_SETUP_R7343', '_skip_symbol', locals(), '_skip_symbol', 7343)
        except Exception:
            pass

        # ── BTC фильтр для SWING ──
        if _audit_test('SWING_DETECT_SWING_SETUP_G7348', (symbol != "BTCUSDT"), 'BTC фильтр для SWING', 'symbol != "BTCUSDT"', 7348):
            btc_ok, btc_reason = btc_allows_signal(direction)
            if _audit_test('SWING_DETECT_SWING_SETUP_G7350', (not btc_ok), 'BTC фильтр для SWING', 'not btc_ok', 7350):
                logging.info(f"[SWING BTC Filter] {symbol} {direction} пропущен: {btc_reason}")
                return _audit_fail('SWING_DETECT_SWING_SETUP_R7352', 'BTC фильтр для SWING', locals(), 'not btc_ok', 7352)

        # ── Фильтр объёма на sweep свече (адаптивный: 1.5x active / 1.2x off-hours) ──
        try:
            from datetime import datetime as _dt_vol
            _vol_hour = _dt_vol.utcnow().hour
            _vol_mult = 1.3 if 8 <= _vol_hour <= 21 else 1.2
            sweep_candle = trigger_candle or candles[-1]
            avg_vol = sum(c["volume"] for c in candles[-20:-1]) / 19 if len(candles) >= 20 else 0
            sweep_vol = sweep_candle.get("volume", 0)
            if _audit_test('SWING_DETECT_SWING_SETUP_G7362', (avg_vol > 0 and sweep_vol < avg_vol * _vol_mult), 'avg_vol > 0 and sweep_vol < avg_vol * _vol_mult', 'avg_vol > 0 and sweep_vol < avg_vol * _vol_mult', 7362):
                return _audit_fail('SWING_DETECT_SWING_SETUP_R7363', 'avg_vol > 0 and sweep_vol < avg_vol * _vol_mult', locals(), 'avg_vol > 0 and sweep_vol < avg_vol * _vol_mult', 7363)
        except Exception as _swing_volume_error:
            logging.debug("[SWING] %s: volume validation failed: %s", symbol, _swing_volume_error)
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7366', 'detector returned None', locals(), '', 7366)

        # ── Displacement candle — свеча после sweep должна быть импульсной ──
        # Адаптивный порог: если ATR < median → 50%, иначе 60%
        try:
            if _audit_test('SWING_DETECT_SWING_SETUP_G7371', (trigger_lookback >= 2), 'Адаптивный порог: если ATR < median → 50%, иначе 60%', 'trigger_lookback >= 2', 7371):
                _disp_candle = candles[-trigger_lookback + 1]
                _disp_body = abs(_disp_candle["close"] - _disp_candle["open"])
                _disp_range = _disp_candle["high"] - _disp_candle["low"]
                if _audit_test('SWING_DETECT_SWING_SETUP_G7375', (_disp_range > 0), 'Адаптивный порог: если ATR < median → 50%, иначе 60%', '_disp_range > 0', 7375):
                    _disp_ratio = _disp_body / _disp_range
                    if _audit_test('SWING_DETECT_SWING_SETUP_G7377', (_disp_ratio < 0.50), '_disp_ratio < 0.50', '_disp_ratio < 0.50', 7377):
                        return _audit_fail('SWING_DETECT_SWING_SETUP_R7378', '_disp_ratio < 0.50', locals(), '_disp_ratio < 0.50', 7378)
                    # Проверяем направление displacement
                    if _audit_test('SWING_DETECT_SWING_SETUP_G7380', (direction == "BULLISH" and _disp_candle["close"] < _disp_candle["open"]), 'Проверяем направление displacement', 'direction == "BULLISH" and _disp_candle["close"] < _disp_candle["open"]', 7380):
                        return _audit_fail('SWING_DETECT_SWING_SETUP_R7381', 'Проверяем направление displacement', locals(), 'direction == "BULLISH" and _disp_candle["close"] < _disp_candle["open"]', 7381)
                    if _audit_test('SWING_DETECT_SWING_SETUP_G7382', (direction == "BEARISH" and _disp_candle["close"] > _disp_candle["open"]), 'Проверяем направление displacement', 'direction == "BEARISH" and _disp_candle["close"] > _disp_candle["open"]', 7382):
                        return _audit_fail('SWING_DETECT_SWING_SETUP_R7383', 'Проверяем направление displacement', locals(), 'direction == "BEARISH" and _disp_candle["close"] > _disp_candle["open"]', 7383)
            else:
                _disp_candle = trigger_candle or candles[-1]
                _disp_range = _disp_candle["high"] - _disp_candle["low"]
                _disp_body = abs(_disp_candle["close"] - _disp_candle["open"])
                if _audit_test('SWING_DETECT_SWING_SETUP_G7388', (_disp_range <= 0 or _disp_body / _disp_range < 0.50), '_disp_range <= 0 or _disp_body / _disp_range < 0.50', '_disp_range <= 0 or _disp_body / _disp_range < 0.50', 7388):
                    return _audit_fail('SWING_DETECT_SWING_SETUP_R7389', '_disp_range <= 0 or _disp_body / _disp_range < 0.50', locals(), '_disp_range <= 0 or _disp_body / _disp_range < 0.50', 7389)
        except Exception as _swing_displacement_error:
            logging.debug("[SWING] %s: displacement validation failed: %s", symbol, _swing_displacement_error)
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7392', 'detector returned None', locals(), '', 7392)

        # ── BOS/CHoCH after the sweep, confirmed by candle close ──
        # The canonical engine distinguishes continuation (BOS) from a real
        # change of character (CHoCH) using the prior paired swing structure.
        _swing_structure_event = get_bos_choch_event(
            candles,
            direction,
            lookback=30,
            max_break_age=max(1, trigger_lookback),
        )
        if _audit_test('SWING_DETECT_SWING_SETUP_G7403', (not _swing_structure_event), 'not _swing_structure_event', 'not _swing_structure_event', 7403):
            logging.info(f"[SWING] {symbol}: нет подтверждённого BOS/CHoCH после триггера")
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7405', 'not _swing_structure_event', locals(), 'not _swing_structure_event', 7405)

        # ── Reaction speed: sweep recovery within 1-2 candles ──
        try:
            if trigger_lookback <= 2:
                pass  # Быстрая реакция — ОК
            elif _audit_test('SWING_DETECT_SWING_SETUP_G7411', (trigger_lookback > 6), 'Reaction speed: sweep recovery within 1-2 candles', 'trigger_lookback > 6', 7411):
                return _audit_fail('SWING_DETECT_SWING_SETUP_R7412', 'Reaction speed: sweep recovery within 1-2 candles', locals(), 'trigger_lookback > 6', 7412)  # Слишком долгое восстановление после sweep
        except Exception:
            pass

        # ── RSI дивергенция для SWING ──
        _swing_rsi_bonus = False
        try:
            if _SMC_ENGINE_OK:
                _sw_rsi_div = detect_rsi_macd_divergence(candles, direction)
                if _sw_rsi_div.get("found"):
                    _swing_rsi_bonus = True
        except Exception:
            pass

        # ── CVD подтверждение для SWING ──
        _swing_cvd_ok = False
        try:
            _sw_cvd = calculate_cvd(candles)
            if _sw_cvd["signal"] == direction[:4] or _sw_cvd["signal"] == direction:
                _swing_cvd_ok = True
        except Exception:
            pass

        # ── FVG в направлении сигнала между entry и TP ──
        _swing_fvg_ok = False
        _sw_dir_fvg = None
        try:
            _sw_dir_fvg = find_fvg(candles, direction)
            if _sw_dir_fvg:
                if direction == "BULLISH" and entry <= _sw_dir_fvg["bottom"] <= tp:
                    _swing_fvg_ok = True
                elif direction == "BEARISH" and tp <= _sw_dir_fvg["top"] <= entry:
                    _swing_fvg_ok = True
        except Exception:
            pass

        # ── 1h CHoCH/BOS после 4h sweep ──
        _swing_1h_choch = False
        _swing_1h_structure_event = None
        try:
            _c1h_sw = get_confirmed_candles(get_candles(symbol, "1h", 31))
            _swing_1h_structure_event = (
                get_bos_choch_event(_c1h_sw, direction, lookback=8, max_break_age=2)
                if _c1h_sw else None
            )
            _swing_1h_choch = bool(_swing_1h_structure_event)
        except Exception:
            pass

        # ── Premium/Discount зона для SWING ──
        _swing_pd_ok = False
        try:
            if _SMC_ENGINE_OK:
                _sw_pd = get_premium_discount(candles)
                # BULLISH должен быть в DISCOUNT, BEARISH в PREMIUM
                if (direction == "BULLISH" and _sw_pd.get("zone") == "DISCOUNT") or \
                   (direction == "BEARISH" and _sw_pd.get("zone") == "PREMIUM"):
                    _swing_pd_ok = True
        except Exception:
            pass

        # ── Проверка ретеста OB в зоне CHoCH ──
        _swing_ob = None
        try:
            _swing_ob = find_ob(candles, direction)
            if _swing_ob:
                # Проверяем что текущая цена вернулась к OB зоне (ретест)
                _in_ob_zone = (_swing_ob["bottom"] <= candles[-1]["close"] <= _swing_ob["top"] or
                               abs(candles[-1]["close"] - _swing_ob["top"]) < atr * 0.5 or
                               abs(candles[-1]["close"] - _swing_ob["bottom"]) < atr * 0.5)
                if _in_ob_zone:
                    # Цена в зоне OB — подтверждение ретеста
                    entry = smart_round(candles[-1]["close"])  # уточняем entry
        except Exception:
            pass

        # Если sweep был давно — цена могла уйти далеко от входа
        current_price = live_price
        if _audit_test('SWING_DETECT_SWING_SETUP_G7490', (abs(current_price - entry) > atr * 4), 'Если sweep был давно — цена могла уйти далеко от входа', 'abs(current_price - entry) > atr * 4', 7490):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7491', 'Если sweep был давно — цена могла уйти далеко от входа', locals(), 'abs(current_price - entry) > atr * 4', 7491)

        # ── Проверка противоположного OB между entry и TP ──
        _adj_tp = check_opposing_ob(candles, direction, entry, tp)
        if _audit_test('SWING_DETECT_SWING_SETUP_G7495', (_adj_tp is None), 'Проверка противоположного OB между entry и TP', '_adj_tp is None', 7495):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7496', 'Проверка противоположного OB между entry и TP', locals(), '_adj_tp is None', 7496)
        tp = _adj_tp

        # A structural stop is immutable.  If it is too wide, reject the
        # candidate instead of pulling SL inside market noise.
        _sl_max_pct = 0.04
        if _audit_test('SWING_DETECT_SWING_SETUP_G7502', (abs(entry - sl) / max(abs(entry), 1e-12) > _sl_max_pct), 'candidate instead of pulling SL inside market noise.', 'abs(entry - sl) / max(abs(entry), 1e-12) > _sl_max_pct', 7502):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7503', 'candidate instead of pulling SL inside market noise.', locals(), 'abs(entry - sl) / max(abs(entry), 1e-12) > _sl_max_pct', 7503)

        # ── Фильтр RR ──
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if _audit_test('SWING_DETECT_SWING_SETUP_G7508', (risk == 0), 'Фильтр RR', 'risk == 0', 7508):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7509', 'Фильтр RR', locals(), 'risk == 0', 7509)
        rr_check = reward / risk
        if _audit_test('SWING_DETECT_SWING_SETUP_G7511', (rr_check < 2.0 or rr_check > 4.0), 'rr_check < 2.0 or rr_check > 4.0', 'rr_check < 2.0 or rr_check > 4.0', 7511):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7512', 'rr_check < 2.0 or rr_check > 4.0', locals(), 'rr_check < 2.0 or rr_check > 4.0', 7512)

        # ── Фильтр — цель должна быть реальной ──
        if _audit_test('SWING_DETECT_SWING_SETUP_G7515', (direction == "BULLISH" and tp <= entry), 'Фильтр — цель должна быть реальной', 'direction == "BULLISH" and tp <= entry', 7515):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7516', 'Фильтр — цель должна быть реальной', locals(), 'direction == "BULLISH" and tp <= entry', 7516)
        if _audit_test('SWING_DETECT_SWING_SETUP_G7517', (direction == "BEARISH" and tp >= entry), 'Фильтр — цель должна быть реальной', 'direction == "BEARISH" and tp >= entry', 7517):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7518', 'Фильтр — цель должна быть реальной', locals(), 'direction == "BEARISH" and tp >= entry', 7518)

        # ── HTF: блок только если ОБА (4h И 1d) против ──
        htf_4h_sw = smc_on_tf(symbol, "4h")
        htf_1d_sw = smc_on_tf(symbol, "1d")
        htf_dir = htf_1d_sw  # для совместимости ниже

        if _audit_test('SWING_DETECT_SWING_SETUP_G7525', (direction == "BULLISH"), 'HTF: блок только если ОБА (4h И 1d) против', 'direction == "BULLISH"', 7525):
            _4h_against = htf_4h_sw and "BEARISH" in str(htf_4h_sw).upper()
            _1d_against = htf_1d_sw and "BEARISH" in str(htf_1d_sw).upper()
            if _audit_test('SWING_DETECT_SWING_SETUP_G7528', (_4h_against and _1d_against), '_4h_against and _1d_against', '_4h_against and _1d_against', 7528):
                return _audit_fail('SWING_DETECT_SWING_SETUP_R7529', '_4h_against and _1d_against', locals(), '_4h_against and _1d_against', 7529)  # оба HTF против — блок
        elif _audit_test('SWING_DETECT_SWING_SETUP_G7530', (direction == "BEARISH"), 'direction == "BEARISH"', 'direction == "BEARISH"', 7530):
            _4h_against = htf_4h_sw and "BULLISH" in str(htf_4h_sw).upper()
            _1d_against = htf_1d_sw and "BULLISH" in str(htf_1d_sw).upper()
            if _audit_test('SWING_DETECT_SWING_SETUP_G7533', (_4h_against and _1d_against), '_4h_against and _1d_against', '_4h_against and _1d_against', 7533):
                return _audit_fail('SWING_DETECT_SWING_SETUP_R7534', '_4h_against and _1d_against', locals(), '_4h_against and _1d_against', 7534)  # оба HTF против — блок

        # 1w — дополнительное подтверждение (бонус/штраф, НЕ hard block)
        htf_1w_swing = smc_on_tf(symbol, "1w")
        weekly_warning = ""
        if htf_1w_swing:
            _1w_str = str(htf_1w_swing).upper()
            if direction == "BULLISH" and "BEARISH" in _1w_str:
                weekly_warning = "⚠️ 1w BEARISH — осторожно с лонгом"
            elif direction == "BEARISH" and "BULLISH" in _1w_str:
                weekly_warning = "⚠️ 1w BULLISH — осторожно с шортом"

        # ── Дополнительно: 15m подтверждение (бонус, не блок) ──
        _swing_15m_confirms = False
        try:
            candles_15m = get_confirmed_candles(get_candles(symbol, "15m", 21))
            if candles_15m and len(candles_15m) >= 5:
                last_15m = candles_15m[-1]
                body_15m = abs(last_15m["close"] - last_15m["open"])
                range_15m = last_15m["high"] - last_15m["low"] if last_15m["high"] != last_15m["low"] else 0.001
                is_impulse_15m = body_15m / range_15m > 0.6

                if direction == "BULLISH" and last_15m["close"] > last_15m["open"] and is_impulse_15m:
                    _swing_15m_confirms = True
                elif direction == "BEARISH" and last_15m["close"] < last_15m["open"] and is_impulse_15m:
                    _swing_15m_confirms = True
        except Exception:
            pass

        rr = round(reward / risk, 2)
        sl_pct = round(abs(entry - sl) / entry * 100, 2)
        tp_pct = round(abs(tp - entry) / entry * 100, 2)

        # ── Funding is risk context, never an automatic rejection ──
        _swing_funding_warning = ""
        try:
            _sw_funding = get_funding_rate(symbol)
            if _sw_funding is not None and abs(_sw_funding) > 0.2:
                if (direction == "BULLISH" and _sw_funding > 0.2) or (direction == "BEARISH" and _sw_funding < -0.2):
                    _swing_funding_warning = f"extreme crowded funding {_sw_funding:+.4f}%"
                    logging.info("[SWING Funding Warning] %s: %s", symbol, _swing_funding_warning)
        except Exception:
            pass

        # ── Dead hours penalty для SWING (22:00-06:00 UTC) ──
        from datetime import datetime as _dt_sw
        _sw_hour = _dt_sw.utcnow().hour
        _is_dead_hours = 22 <= _sw_hour or _sw_hour <= 5
        _rr_min = 1.8 if _is_dead_hours else 1.5
        if _audit_test('SWING_DETECT_SWING_SETUP_G7583', (rr_check < _rr_min), 'Dead hours penalty для SWING (22:00-06:00 UTC)', 'rr_check < _rr_min', 7583):
            logging.info(f"[SWING] {symbol}: RR {rr_check} < {_rr_min} {'(dead hours)' if _is_dead_hours else ''} — пропускаем")
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7585', 'rr_check < _rr_min', locals(), 'rr_check < _rr_min', 7585)

        # ── Groq анализирует реальную картину сетапа (бонус, не блокирует) ──
        _swing_groq_ok = False
        try:
            last_candles_summary = []
            for c in candles[-5:]:
                body = abs(c["close"] - c["open"])
                wick_up = c["high"] - max(c["open"], c["close"])
                wick_dn = min(c["open"], c["close"]) - c["low"]
                color = "🟢" if c["close"] > c["open"] else "🔴"
                last_candles_summary.append(
                    f"{color} O={smart_round(c['open'])} H={smart_round(c['high'])} "
                    f"L={smart_round(c['low'])} C={smart_round(c['close'])}"
                )

            # Расчёт времени до TP через ATR
            tf_hours = {"1h": 1, "4h": 4, "1d": 24, "1w": 168}
            candle_hours = tf_hours.get(timeframe, 4)
            distance_to_tp = abs(tp - entry)
            est_candles = round(distance_to_tp / atr, 1) if atr > 0 else 3
            est_hours = int(round(est_candles * candle_hours, 0))
            # Минимум 12ч для swing (структурные сделки), максимум 96ч
            est_hours = max(12, min(est_hours, 96))

            candles_str = " | ".join(last_candles_summary)
            # OB/FVG зоны для промпта
            _sw_ob = find_ob(candles, direction)
            _sw_fvg = find_fvg(candles, direction)
            _ob_desc = f"OB: {_sw_ob['bottom']:.6f}–{_sw_ob['top']:.6f}" if _sw_ob else "OB: нет"
            _fvg_desc = f"FVG: {_sw_fvg['bottom']:.6f}–{_sw_fvg['top']:.6f}" if _sw_fvg else "FVG: нет"
            # Volume profile последних 10 свечей
            _vol_10 = [c.get("volume", 0) for c in candles[-10:]]
            _avg_vol_10 = sum(_vol_10) / len(_vol_10) if _vol_10 else 0
            _vol_desc = f"Vol avg10: {_avg_vol_10:.0f}, last: {_vol_10[-1]:.0f}" if _vol_10 else ""

            # Funding, Fear&Greed, Market Regime
            _sw_funding = get_funding_rate(symbol)
            _sw_fg = get_fear_greed()
            _sw_regime = get_market_regime(symbol)
            _sw_fund_str = f"{_sw_funding:+.4f}%" if _sw_funding is not None else "N/A"
            _sw_fg_str = f"{_sw_fg['value']} ({_sw_fg['label']})" if _sw_fg else "N/A"
            _sw_regime_str = _sw_regime.get("mode", "?") if isinstance(_sw_regime, dict) else str(_sw_regime)

            # Pattern history для Groq
            _sw_pat_str = ""
            try:
                _sw_pat = _learn_patterns(symbol, direction, timeframe, _sw_regime_str, 0)
                if _sw_pat.get("found") and _sw_pat.get("samples", 0) >= 3:
                    _sw_pat_str = (f"\nИстория похожих: {_sw_pat['samples']} сделок, "
                                   f"WR: {_sw_pat['win_rate']:.0f}%, avg RR: {_sw_pat['avg_rr']:.1f}, "
                                   f"вердикт: {_sw_pat.get('verdict', '?')}")
            except Exception:
                pass

            _sw_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry > 0 else 0
            _self_rules = get_relevant_rules(symbol, direction, "SWING")
            _recent_errors = get_recent_errors(symbol)
            groq_prompt = (
                "Ты опытный SMC трейдер специализирующийся на swing торговле. "
                "Оцени качество sweep сетапа. "
                f"Ответь СТРОГО JSON: {{\"logic\": \"макс 15 слов\", \"hours\": число, \"valid\": true/false}}\n\n"
                "БЛОКИРУЙ (valid: false) если:\n"
                f"- RR < 2.5 или стоп > 4% (RR={rr}, стоп={_sw_sl_pct}%)\n"
                "- Sweep был слабым (нет объёма, нет импульсной свечи обратно)\n"
                "- Нет CHoCH после sweep — структура не сменилась\n"
                "- 1d тренд против направления сигнала\n"
                "- Цена уже далеко от зоны sweep (> 2% от входа)\n"
                "- Между входом и TP есть сильный OB или FVG против направления\n"
                "- SL выставлен математически (entry ± X%), а не за структуру\n\n"
                "ПОДТВЕРЖДАЙ (valid: true) если:\n"
                "- Sweep чёткий — пробой swing low/high с быстрым возвратом за 1-2 свечи\n"
                "- Объём на sweep выше среднего\n"
                "- CHoCH или BOS подтверждает разворот\n"
                "- TP на реальном структурном уровне\n"
                "- RR ≥ 2.5 — это swing, нужен запас\n\n"
                "ПРАВИЛА ВЫСТАВЛЕНИЯ УРОВНЕЙ:\n"
                "- SL ТОЛЬКО за структурный уровень (swing low/high, OB edge, FVG edge)\n"
                "- ЗАПРЕЩЕНО: SL = entry ± X% (математические стопы не работают)\n"
                "- TP ТОЛЬКО на структурный уровень (EQH/EQL, OB, FVG, swing point)\n"
                "- Если нет структуры для SL — НЕ ВХОДИТЬ\n\n"
                f"Данные: Пара: {symbol} ТФ: {timeframe} Направление: {direction}\n"
                f"Вход: {entry} SL: {sl} TP: {tp} HTF: {htf_dir} 1w: {htf_1w_swing}\n"
                f"RR: {rr} | Стоп: {_sw_sl_pct}% | ATR: {smart_round(atr)} | До TP: {smart_round(distance_to_tp)}ч\n"
                f"Funding: {_sw_fund_str} | Fear&Greed: {_sw_fg_str} | Режим: {_sw_regime_str}\n"
                f"{_ob_desc} | {_fvg_desc} | {_vol_desc}\n"
                f"Свечи: {candles_str}"
                f"{_sw_pat_str}"
                f"{_self_rules}"
                f"{_recent_errors}"
            )

            groq_response = ask_groq(groq_prompt, max_tokens=100) if legacy_strategy_groq_enabled() else None
            if _audit_test('SWING_DETECT_SWING_SETUP_G7678', (groq_response and len(groq_response) > 5), 'groq_response and len(groq_response) > 5', 'groq_response and len(groq_response) > 5', 7678):
                try:
                    import json as _json, re as _re
                    clean = groq_response.strip().replace("```json", "").replace("```", "").strip()
                    json_match = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                    if json_match:
                        clean = json_match.group()
                    parsed = _json.loads(clean)
                    # Это финальное подтверждение структуры. Явный отказ Groq
                    # не должен проходить через один случайный Q-бонус.
                    if parsed.get("valid", True):
                        _swing_groq_ok = True
                    else:
                        logging.info(f"[SWING Groq] {symbol}: Groq отклонил сигнал")
                        return _audit_fail('SWING_DETECT_SWING_SETUP_R7692', 'не должен проходить через один случайный Q-бонус.', locals(), 'parsed.get("valid", True)', 7692)
                    if parsed.get("logic") and len(str(parsed["logic"])) > 5:
                        logic = str(parsed["logic"]).strip()
                    if parsed.get("hours") and str(parsed["hours"]).isdigit():
                        est_hours = max(12, min(int(parsed["hours"]), 96))
                except Exception:
                    # JSON не распарсился — fallback, не блокируем
                    clean_text = groq_response.strip().replace("\n", " ")
                    if len(clean_text) > 10:
                        logic = clean_text[:80]
            else:
                logging.debug(f"[SWING Groq] {symbol}: Groq не ответил — fallback")
        except Exception as ge:
            logging.debug(f"[SwingGroq] {symbol}: {ge}")
            tf_hours = {"1h": 1, "4h": 4, "1d": 24}
            est_hours = int(round((abs(tp - entry) / atr) * tf_hours.get(timeframe, 4), 0)) if atr > 0 else 12
            est_hours = max(12, min(est_hours, 96))

        # ── SWING Quality Score: six independent factual confirmations ──
        # Sweep volume ≥1.2x
        _sw_vol_ok = False
        try:
            _sw_avg_vol = sum(c["volume"] for c in candles[-20:-1]) / 19
            _sw_vol_ok = (trigger_candle or candles[-1])["volume"] >= _sw_avg_vol * 1.2
        except Exception:
            pass
        # Displacement ≥0.45
        _sw_disp_ok = False
        try:
            _sw_last = _disp_candle if '_disp_candle' in locals() else (trigger_candle or candles[-1])
            _sw_body = abs(_sw_last["close"] - _sw_last["open"])
            _sw_range = _sw_last["high"] - _sw_last["low"]
            _sw_disp_ok = _sw_body / _sw_range >= 0.45 if _sw_range > 0 else False
        except Exception:
            pass

        # Fresh 1h structure is part of the SWING thesis, not a bonus vote.
        if _audit_test('SWING_DETECT_SWING_SETUP_G7729', (not _swing_1h_choch), 'Fresh 1h structure is part of the SWING thesis, not a bonus vote.', 'not _swing_1h_choch', 7729):
            logging.info(f"[SWING Structure] {symbol}: no fresh 1h BOS/CHoCH after sweep — пропуск")
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7731', 'Fresh 1h structure is part of the SWING thesis, not a bonus vote.', locals(), 'not _swing_1h_choch', 7731)

        _sw_confirms = sum([
            _sw_vol_ok,           # Volume ≥1.2x
            _sw_disp_ok,          # Displacement ≥0.45
            _swing_pd_ok,         # Premium/Discount context
            _swing_fvg_ok,        # FVG between entry and target
            _swing_15m_confirms,  # 15m impulse in trade direction
        ])
        _sw_quality = f" [Q:{_sw_confirms}/5]"
        if _audit_test('SWING_DETECT_SWING_SETUP_G7741', (_sw_confirms < 2), '_sw_confirms < 2', '_sw_confirms < 2', 7741):
            logging.info(f"[SWING Quality] {symbol}: confirms={_sw_confirms}/5 < 2 — пропуск")
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7743', '_sw_confirms < 2', locals(), '_sw_confirms < 2', 7743)

        # TP2 is optional and must be another real structural swing.  No
        # synthetic distance multiplier is used when the market has no target.
        if direction == "BULLISH":
            _sw_tp2_candidates = [level for _, level in swing_highs if level > tp * 1.005]
            _sw_tp2 = smart_round(min(_sw_tp2_candidates)) if _sw_tp2_candidates else None
        else:
            _sw_tp2_candidates = [level for _, level in swing_lows if level < tp * 0.995]
            _sw_tp2 = smart_round(max(_sw_tp2_candidates)) if _sw_tp2_candidates else None

        return {
            "symbol":    symbol,
            "direction": direction,
            "timeframe": timeframe,
            "entry":     entry,
            "sl":        sl,
            "tp":        tp,
            "tp2":       _sw_tp2,
            "sl_pct":    sl_pct,
            "tp_pct":    tp_pct,
            "rr":        rr,
            "logic":     logic + _sw_quality,
            "htf_dir":   htf_dir,
            "htf_1w":    htf_1w_swing,
            "weekly_warning": weekly_warning,
            "est_hours": est_hours,
            "ob":        _swing_ob,
            "fvg":       _sw_dir_fvg if _swing_fvg_ok else None,
            "confirms":  _sw_confirms,
            "funding_warning": _swing_funding_warning,
            "structure_event": _swing_structure_event,
            "structure_event_1h": _swing_1h_structure_event,
            "scan_type": "swing",
        }

    except Exception as e:
        logging.debug(f"detect_swing_setup {symbol}: {e}")
        return _audit_fail('SWING_DETECT_SWING_SETUP_R7781', 'detector returned None', locals(), '', 7781)




# ===== СТРАТЕГИЯ 5: ZONE — вход из Discount/Premium зоны =====

@_audit_strategy("ZONE")
def detect_zone_setup(symbol: str, timeframe: str = "4h", passive_watch: bool = False) -> dict | None:
    """
    ZONE стратегия: вход из Discount/Premium зоны с OB/FVG подтверждением.
    Не требует sweep — опирается на зону интереса и отбой от неё.
    """
    try:
        raw_candles = get_candles(symbol, timeframe, 101)
        candles = get_confirmed_candles(raw_candles)
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7796', (not candles or len(candles) < 50), 'not candles or len(candles) < 50', 'not candles or len(candles) < 50', 7796):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7797', 'not candles or len(candles) < 50', locals(), 'not candles or len(candles) < 50', 7797)

        price = raw_candles[-1]["close"]
        atr = sum(c["high"] - c["low"] for c in candles[-14:]) / 14
        _ap_zone = get_adaptive_params(symbol, candles)
        _vf_zone = _ap_zone["volatility_factor"]

        # ── 1. Диапазон и зоны ──
        range_candles = candles[-50:]
        range_high = max(c["high"] for c in range_candles)
        range_low  = min(c["low"]  for c in range_candles)
        range_mid  = (range_high + range_low) / 2
        range_size = range_high - range_low

        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7811', (range_size < atr * 2), 'range_size < atr * 2', 'range_size < atr * 2', 7811):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7812', 'range_size < atr * 2', locals(), 'range_size < atr * 2', 7812)  # Диапазон слишком мал

        # Require a real range extreme and leave the middle 40% neutral.
        in_discount = price <= range_low + range_size * 0.30
        in_premium = price >= range_high - range_size * 0.30

        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7818', (not in_discount and not in_premium), 'Require a real range extreme and leave the middle 40% neutral.', 'not in_discount and not in_premium', 7818):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7819', 'Require a real range extreme and leave the middle 40% neutral.', locals(), 'not in_discount and not in_premium', 7819)

        # Определяем направление
        direction = "BULLISH" if in_discount else "BEARISH"

        # ── 2. Находим OB и FVG в зоне ──
        ob  = find_ob(candles, direction)
        fvg = find_fvg(candles, direction)

        zone_level = None
        zone_type  = None

        if ob:
            # OB должен быть в нужной зоне
            if direction == "BULLISH" and ob["top"] <= range_mid:
                if ob["bottom"] <= price <= ob["top"] + atr * 0.5:
                    zone_level = ob["bottom"]
                    zone_type  = "OB"
            elif direction == "BEARISH" and ob["bottom"] >= range_mid:
                if ob["bottom"] - atr * 0.5 <= price <= ob["top"]:
                    zone_level = ob["top"]
                    zone_type  = "OB"

        if not zone_level and fvg:
            if direction == "BULLISH" and fvg["top"] <= range_mid:
                if fvg["bottom"] <= price <= fvg["top"] + atr * 0.5:
                    zone_level = fvg["bottom"]
                    zone_type  = "FVG"
            elif direction == "BEARISH" and fvg["bottom"] >= range_mid:
                if fvg["bottom"] - atr * 0.5 <= price <= fvg["top"]:
                    zone_level = fvg["top"]
                    zone_type  = "FVG"

        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7852', (not zone_level), 'not zone_level', 'not zone_level', 7852):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7853', 'not zone_level', locals(), 'not zone_level', 7853)  # Нет зоны интереса рядом с ценой

        # ── 2.5. Проверка свежести зоны (unmitigated + strong move away) ──
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7856', (zone_level and zone_type), '2.5. Проверка свежести зоны (unmitigated + strong move away)', 'zone_level and zone_type', 7856):
            try:
                _test_count = 0
                _zone_top = ob["top"] if zone_type == "OB" and ob else (fvg["top"] if fvg else zone_level * 1.01)
                _zone_bot = ob["bottom"] if zone_type == "OB" and ob else (fvg["bottom"] if fvg else zone_level * 0.99)

                for c in candles[-40:-3]:
                    if _zone_bot <= c["low"] <= _zone_top or _zone_bot <= c["high"] <= _zone_top:
                        _test_count += 1

                if _audit_test('ZONE_DETECT_ZONE_SETUP_G7866', (_test_count > 2), '_test_count > 2', '_test_count > 2', 7866):
                    logging.debug(f"[ZONE] {symbol}: зона протестирована {_test_count} раз — mitigated")
                    return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7868', '_test_count > 2', locals(), '_test_count > 2', 7868)

                # Strong move away: displacement ≥0.5 + body > ATR×1.0
                _strong_move = False
                for i in range(max(-len(candles), -35), -3):
                    c = candles[i]
                    c_body = abs(c["close"] - c["open"])
                    c_range = c["high"] - c["low"]
                    if c_range > 0 and c_body / c_range >= 0.5 and c_body > atr * _vf_zone:
                        if direction == "BULLISH" and c["close"] > c["open"]:
                            _strong_move = True
                            break
                        elif direction == "BEARISH" and c["close"] < c["open"]:
                            _strong_move = True
                            break

                if _audit_test('ZONE_DETECT_ZONE_SETUP_G7884', (not _strong_move), 'not _strong_move', 'not _strong_move', 7884):
                    logging.debug(f"[ZONE] {symbol}: нет сильного импульса (displacement < 0.5)")
                    return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7886', 'not _strong_move', locals(), 'not _strong_move', 7886)

            except Exception as _zone_freshness_error:
                logging.debug("[ZONE] %s: zone freshness unavailable: %s", symbol, _zone_freshness_error)
                return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7890', 'zone_level and zone_type', locals(), 'zone_level and zone_type', 7890)

        # ── 3. Подтверждение отбоя — хотя бы 1 свеча в направлении ──
        last = candles[-1]
        rebound_bull = (last["close"] > last["open"] and
                        last["low"] <= zone_level + atr * 0.3)
        rebound_bear = (last["close"] < last["open"] and
                        last["high"] >= zone_level - atr * 0.3)

        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7899', (direction == "BULLISH" and not rebound_bull), 'direction == "BULLISH" and not rebound_bull', 'direction == "BULLISH" and not rebound_bull', 7899):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7900', 'direction == "BULLISH" and not rebound_bull', locals(), 'direction == "BULLISH" and not rebound_bull', 7900)
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7901', (direction == "BEARISH" and not rebound_bear), 'direction == "BEARISH" and not rebound_bear', 'direction == "BEARISH" and not rebound_bear', 7901):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7902', 'direction == "BEARISH" and not rebound_bear', locals(), 'direction == "BEARISH" and not rebound_bear', 7902)

        # ── 4. HTF фильтры ──
        htf_1d = smc_on_tf(symbol, "1d")
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7906', (htf_1d), '4. HTF фильтры', 'htf_1d', 7906):
            if _audit_test('ZONE_DETECT_ZONE_SETUP_G7907', (direction == "BULLISH" and "BEARISH" in str(htf_1d).upper()), '4. HTF фильтры', 'direction == "BULLISH" and "BEARISH" in str(htf_1d).upper()', 7907):
                return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7908', '4. HTF фильтры', locals(), 'direction == "BULLISH" and "BEARISH" in str(htf_1d).upper()', 7908)
            if _audit_test('ZONE_DETECT_ZONE_SETUP_G7909', (direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()), '4. HTF фильтры', 'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()', 7909):
                return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7910', 'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()', locals(), 'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()', 7910)

        # ── 5. BTC фильтр ──
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7913', (symbol != "BTCUSDT"), '5. BTC фильтр', 'symbol != "BTCUSDT"', 7913):
            btc_ok, btc_reason = btc_allows_signal(direction)
            if _audit_test('ZONE_DETECT_ZONE_SETUP_G7915', (not btc_ok), '5. BTC фильтр', 'not btc_ok', 7915):
                return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7916', '5. BTC фильтр', locals(), 'not btc_ok', 7916)

        # ── 6. Funding is risk context, not an automatic direction block ──
        try:
            fr = get_funding_rate(symbol)
            if fr is not None and abs(fr) > 0.2:
                logging.info("[ZONE] %s: extreme funding %.4f%% — risk warning", symbol, fr)
        except Exception:
            pass

        # ── 7. Quality score (минимум 3 из 8) ──
        q_score = 0

        # Q0: Wick rejection — тень > тела
        try:
            if direction == "BULLISH":
                _wick_z = candles[-1]["close"] - candles[-1]["low"]
                _body_z = abs(candles[-1]["close"] - candles[-1]["open"])
                if _wick_z > _body_z:
                    q_score += 1
            else:
                _wick_z = candles[-1]["high"] - candles[-1]["close"]
                _body_z = abs(candles[-1]["close"] - candles[-1]["open"])
                if _wick_z > _body_z:
                    q_score += 1
        except Exception:
            pass

        # Q1: CHoCH/BOS на 1h (реальная проверка структуры)
        _zone_ltf_structure = False
        _zone_structure_event = None
        try:
            _c1h_zone = get_confirmed_candles(get_candles(symbol, "1h", 31))
            _zone_structure_event = (
                get_bos_choch_event(_c1h_zone, direction, lookback=8, max_break_age=1)
                if _c1h_zone else None
            )
            if _zone_structure_event:
                _zone_ltf_structure = True
        except Exception:
            pass
        if passive_watch and not _zone_ltf_structure:
            return {
                "_pending_ltf": True,
                "symbol": symbol,
                "strategy": "ZONE",
                "direction": direction,
                "required_timeframe": "1h",
                "reason": "зона подтверждена; ожидается свежий 1h BOS/CHoCH",
            }
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7966', (not _zone_ltf_structure), 'not _zone_ltf_structure', 'not _zone_ltf_structure', 7966):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7967', 'not _zone_ltf_structure', locals(), 'not _zone_ltf_structure', 7967)

        # Volume is scored once below on the actual rejection candle.

        # Q3: RSI не перекуплен (30-70)
        try:
            rmd = detect_rsi_macd_divergence(candles, direction)
            rsi_val = rmd.get("rsi") if rmd else None
            if rsi_val is not None and 30 <= rsi_val <= 70:
                q_score += 1
        except Exception:
            pass

        # Q4: FVG между entry и TP в направлении
        try:
            if fvg and direction == "BULLISH" and fvg["bottom"] > price:
                q_score += 1
            elif fvg and direction == "BEARISH" and fvg["top"] < price:
                q_score += 1
        except Exception:
            pass

        # Q5: BTC на 4h в том же направлении
        try:
            btc_4h = smc_on_tf("BTCUSDT", "4h")
            if btc_4h and direction in str(btc_4h).upper():
                q_score += 1
        except Exception:
            pass

        # Q6: Funding rate нейтральный или против толпы. Extreme funding is
        # risk context for Groq, not a deterministic veto.
        _zone_funding_warning = ""
        try:
            fr = get_funding_rate(symbol)
            if fr is not None:
                if direction == "BULLISH" and fr < 0:
                    q_score += 1  # Шорты накопились — хорошо для LONG
                elif direction == "BEARISH" and fr > 0:
                    q_score += 1  # Лонги накопились — хорошо для SHORT
                elif abs(fr) < 0.05:
                    q_score += 1  # Нейтральный
                elif (direction == "BULLISH" and fr > 0.2) or (direction == "BEARISH" and fr < -0.2):
                    _zone_funding_warning = f"extreme crowded funding {fr:+.4f}%"
        except Exception:
            pass

        # Q7: Свеча с объёмом на отбое > 1.3x avg
        try:
            avg_vol = sum(c["volume"] for c in candles[-20:-1]) / 19
            if last["volume"] > avg_vol * 1.3:
                q_score += 1
        except Exception:
            pass

        # ── 7.5. Imbalance (FVG внутри движения) — зона сильнее если есть дисбаланс ──
        try:
            _imbalance_found = False
            for i in range(1, min(10, len(candles)-1)):
                _c1 = candles[-i-1]
                _c2 = candles[-i]
                _c3 = candles[-i+1] if i > 1 else candles[-1]

                if direction == "BULLISH":
                    if _c3["low"] > _c1["high"]:
                        _imbalance_found = True
                        break
                else:
                    if _c3["high"] < _c1["low"]:
                        _imbalance_found = True
                        break

            if not _imbalance_found:
                q_score = max(0, q_score - 1)
                logging.debug(f"[ZONE] {symbol}: нет FVG дисбаланса — q_score снижен до {q_score}")
        except Exception:
            pass

        # Structure is already mandatory above. Optional confirmations are
        # wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.
        _zone_ap = get_adaptive_params(symbol, candles)
        _zone_vf = _zone_ap.get("volatility_factor", 1.0) if _zone_ap else 1.0
        _q_min = 3
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G8050', (q_score < _q_min), 'wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.', 'q_score < _q_min', 8050):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R8051', 'wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.', locals(), 'q_score < _q_min', 8051)  # Недостаточно независимых подтверждений

        # ── 8. Расчёт entry / SL / TP ──
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G8054', (direction == "BULLISH"), '8. Расчёт entry / SL / TP', 'direction == "BULLISH"', 8054):
            entry = smart_round(price)
            sl    = smart_round(zone_level - atr * 0.5)
            # TP = ближайший swing high
            swing_highs, _ = find_swings(candles, lookback=5)
            tp_candidates = [sh[1] for sh in swing_highs if sh[1] > entry * 1.005]
            if _audit_test('ZONE_DETECT_ZONE_SETUP_G8060', (not tp_candidates), 'TP = ближайший swing high', 'not tp_candidates', 8060):
                return _audit_fail('ZONE_DETECT_ZONE_SETUP_R8061', 'TP = ближайший swing high', locals(), 'not tp_candidates', 8061)
            tp = smart_round(min(tp_candidates))
        else:
            entry = smart_round(price)
            sl    = smart_round(zone_level + atr * 0.5)
            _, swing_lows = find_swings(candles, lookback=5)
            tp_candidates = [sw[1] for sw in swing_lows if sw[1] < entry * 0.995]
            if _audit_test('ZONE_DETECT_ZONE_SETUP_G8068', (not tp_candidates), 'not tp_candidates', 'not tp_candidates', 8068):
                return _audit_fail('ZONE_DETECT_ZONE_SETUP_R8069', 'not tp_candidates', locals(), 'not tp_candidates', 8069)
            tp = smart_round(max(tp_candidates))

        # ── 9. RR фильтр ──
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G8075', (risk == 0), '9. RR фильтр', 'risk == 0', 8075):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R8076', '9. RR фильтр', locals(), 'risk == 0', 8076)
        rr = round(reward / risk, 2)
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G8078', (rr < 2.0), 'rr < 2.0', 'rr < 2.0', 8078):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R8079', 'rr < 2.0', locals(), 'rr < 2.0', 8079)

        # ── 10. Groq анализ ──
        logic = f"Вход из {'Discount' if direction == 'BULLISH' else 'Premium'} зоны ({zone_type})"
        try:
            _zone_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry > 0 else 0
            _self_rules = get_relevant_rules(symbol, direction, "ZONE")
            _recent_errors = get_recent_errors(symbol)
            _zone_prompt = (
                "Ты SMC трейдер специализирующийся на зонах интереса. "
                "Оцени вход из Discount/Premium зоны. "
                f'Ответь СТРОГО JSON: {{\"logic\": \"макс 15 слов\", \"valid\": true/false}}\n\n'
                "БЛОКИРУЙ (valid: false) если:\n"
                f"- RR < 2.0 или стоп > 3% (RR={rr}, стоп={_zone_sl_pct}%)\n"
                "- Цена не чётко в OB или FVG зоне\n"
                "- Нет отбоя от зоны (нет бычьей/медвежьей свечи от уровня)\n"
                "- 1d тренд против направления\n"
                "- OB/FVG уже был протестирован несколько раз (mitigated)\n"
                "- Нет FVG между входом и TP для притяжения цены\n"
                "- SL выставлен математически (entry ± X%), а не за структуру\n\n"
                "ПОДТВЕРЖДАЙ (valid: true) если:\n"
                "- Цена чётко внутри OB или касается FVG\n"
                "- Зона нетронутая (первый или второй тест)\n"
                "- Есть хотя бы одна свеча отбоя от зоны\n"
                "- 1d и 4h тренд совпадают с направлением\n"
                "- RR ≥ 2.0, TP на реальном swing уровне\n\n"
                "ПРАВИЛА ВЫСТАВЛЕНИЯ УРОВНЕЙ:\n"
                "- SL ТОЛЬКО за структурный уровень (OB edge, FVG edge, swing low/high)\n"
                "- ЗАПРЕЩЕНО: SL = entry ± X% (математические стопы не работают)\n"
                "- TP ТОЛЬКО на структурный уровень (EQH/EQL, OB, FVG, swing point)\n"
                "- Если нет структуры для SL — НЕ ВХОДИТЬ\n\n"
                f"Данные: Пара: {symbol} ТФ: {timeframe} Направление: {direction}\n"
                f"Зона: {'Discount' if direction == 'BULLISH' else 'Premium'} | Тип: {zone_type}\n"
                f"Диапазон: {smart_price_fmt(range_low)}–{smart_price_fmt(range_high)} | Mid: {smart_price_fmt(range_mid)}\n"
                f"Цена: {smart_price_fmt(price)} | OB: {smart_price_fmt(ob['bottom']) + '–' + smart_price_fmt(ob['top']) if ob else 'нет'}\n"
                f"FVG: {smart_price_fmt(fvg['bottom']) + '–' + smart_price_fmt(fvg['top']) if fvg else 'нет'}\n"
                f"1d тренд: {htf_1d} | Quality score: {q_score}/6\n"
                f"Entry: {smart_price_fmt(entry)} SL: {smart_price_fmt(sl)} TP: {smart_price_fmt(tp)} RR: {rr} Стоп: {_zone_sl_pct}%"
                f"{_self_rules}"
                f"{_recent_errors}"
            )
            _zone_resp = ask_groq(_zone_prompt, max_tokens=80) if legacy_strategy_groq_enabled() else None
            if _audit_test('ZONE_DETECT_ZONE_SETUP_G8121', (_zone_resp), '_zone_resp', '_zone_resp', 8121):
                import json as _j, re as _re
                _clean = _re.sub(r'```json|```', '', _zone_resp).strip()
                _m = _re.search(r'\{[^}]+\}', _clean, _re.DOTALL)
                if _audit_test('ZONE_DETECT_ZONE_SETUP_G8125', (_m), '_m', '_m', 8125):
                    _parsed = _j.loads(_m.group())
                    if _audit_test('ZONE_DETECT_ZONE_SETUP_G8127', (not _parsed.get("valid", True)), 'not _parsed.get("valid", True)', 'not _parsed.get("valid", True)', 8127):
                        return _audit_fail('ZONE_DETECT_ZONE_SETUP_R8128', 'not _parsed.get("valid", True)', locals(), 'not _parsed.get("valid", True)', 8128)
                    if _parsed.get("logic"):
                        logic = str(_parsed["logic"]).strip()
        except Exception:
            pass

        # TP2 is optional and only exists when a second structural swing does.
        try:
            _z_sh, _z_sl = find_swings(candles, lookback=12)
            if direction == "BULLISH":
                _tp2_cands = [s[1] for s in _z_sh if s[1] > tp * 1.005]
                _z_tp2 = smart_round(min(_tp2_cands)) if _tp2_cands else None
            else:
                _tp2_cands = [s[1] for s in _z_sl if s[1] < tp * 0.995]
                _z_tp2 = smart_round(max(_tp2_cands)) if _tp2_cands else None
        except Exception:
            _z_tp2 = None

        _zone_est_hours = max(12, min(96, int(round(abs(tp - entry) / max(atr, 1e-12) * 4))))

        return {
            "symbol":    symbol,
            "direction": direction,
            "entry":     entry,
            "sl":        sl,
            "tp":        tp,
            "tp2":       _z_tp2,
            "rr":        rr,
            "zone_type": zone_type,
            "zone":      "Discount" if direction == "BULLISH" else "Premium",
            "q_score":   q_score,
            "htf_dir":   htf_1d,
            "funding_warning": _zone_funding_warning,
            "logic":     logic,
            "est_hours": _zone_est_hours,
            "structure_event": _zone_structure_event,
        }

    except Exception as e:
        logging.warning(f"detect_zone_setup {symbol}: {e}")
        return _audit_fail('ZONE_DETECT_ZONE_SETUP_R8168', 'detector returned None', locals(), '', 8168)


# ===== СТРАТЕГИЯ 3: WYCKOFF ACCUMULATION + DISTRIBUTION =====

def _find_wyckoff_phases_accumulation(candles_1d, candles_4h):
    """
    Определяет фазы Wyckoff Accumulation:
    PS  — Preliminary Support (первая поддержка на падении)
    SC  — Selling Climax (паническая свеча с огромным объёмом = дно)
    AR  — Automatic Rally (отскок от SC — кит выкупает)
    ST  — Secondary Test (тест лоу SC с меньшим объёмом)
    Spring — ложный пробой ниже ST/SC с возвратом
    SOS — Sign of Strength (пробой AR с объёмом = подтверждение)
    """
    if len(candles_1d) < 40:
        return {}

    phases = {}
    vols = [c["volume"] for c in candles_1d]
    avg_vol = sum(vols) / len(vols) if vols else 1

    # SC — Selling Climax: самая большая медвежья свеча с объёмом x3+
    sc_idx = None
    sc_vol_max = 0
    for i in range(10, len(candles_1d) - 5):
        c = candles_1d[i]
        body = c["open"] - c["close"]  # медвежья = open > close
        if body > 0 and c["volume"] > avg_vol * 2.5:
            if c["volume"] > sc_vol_max:
                sc_vol_max = c["volume"]
                sc_idx = i

    if sc_idx is None:
        return {}

    sc_candle = candles_1d[sc_idx]
    phases["SC"] = {"idx": sc_idx, "price": sc_candle["low"], "vol": sc_vol_max}

    # AR — Automatic Rally: первый сильный рост после SC
    ar_idx = None
    ar_high = 0
    for i in range(sc_idx + 1, min(sc_idx + 15, len(candles_1d))):
        c = candles_1d[i]
        if c["close"] > c["open"] and c["high"] > ar_high:
            ar_high = c["high"]
            ar_idx = i

    if ar_idx is None:
        return {}

    phases["AR"] = {"idx": ar_idx, "price": ar_high}

    # ST — Secondary Test: тест лоу SC с меньшим объёмом
    st_idx = None
    for i in range(ar_idx + 1, min(ar_idx + 20, len(candles_1d))):
        c = candles_1d[i]
        near_sc_low = abs(c["low"] - sc_candle["low"]) / sc_candle["low"] < 0.05
        lower_vol = c["volume"] < sc_vol_max * 0.6
        if near_sc_low and lower_vol:
            st_idx = i
            break

    if st_idx:
        phases["ST"] = {"idx": st_idx, "price": candles_1d[st_idx]["low"]}

    # Spring — ложный пробой ниже ST/SC на 4h
    spring_level = phases.get("ST", phases["SC"])["price"]
    spring_found = False
    spring_price = None

    for c in candles_4h[-30:]:
        if c["low"] < spring_level and c["close"] > spring_level:
            wick = (c["close"] - c["low"]) / (c["high"] - c["low"] + 0.000001)
            if wick > 0.4:
                spring_found = True
                spring_price = c["low"]
                phases["Spring"] = {"found": True, "price": spring_price}
                break

    if not spring_found:
        phases["Spring"] = {"found": False}

    # SOS — Sign of Strength: пробой AR уровня с объёмом
    sos_found = False
    for c in candles_4h[-10:]:
        if c["close"] > ar_high:
            vol_4h_avg = sum(x["volume"] for x in candles_4h[-20:-10]) / 10 if len(candles_4h) >= 20 else 1
            if c["volume"] > vol_4h_avg * 1.5:
                sos_found = True
                phases["SOS"] = {"found": True, "price": c["close"]}
                break

    if not sos_found:
        phases["SOS"] = {"found": False}

    return phases


def _find_wyckoff_phases_distribution(candles_1d, candles_4h):
    """
    Определяет фазы Wyckoff Distribution (для шортов):
    PSY — Preliminary Supply (первое сопротивление на росте)
    BC  — Buying Climax (эйфорийная свеча с объёмом = вершина)
    AR  — Automatic Reaction (откат от BC)
    ST  — Secondary Test (тест хая BC с меньшим объёмом)
    UTAD — UpThrust After Distribution (ложный пробой вверх = финальная ловушка)
    LPSY — Last Point of Supply (последний отскок перед падением)
    """
    if len(candles_1d) < 40:
        return {}

    phases = {}
    vols = [c["volume"] for c in candles_1d]
    avg_vol = sum(vols) / len(vols) if vols else 1

    # BC — Buying Climax: самая большая бычья свеча с объёмом x3+
    bc_idx = None
    bc_vol_max = 0
    for i in range(10, len(candles_1d) - 5):
        c = candles_1d[i]
        body = c["close"] - c["open"]  # бычья = close > open
        if body > 0 and c["volume"] > avg_vol * 2.5:
            if c["volume"] > bc_vol_max:
                bc_vol_max = c["volume"]
                bc_idx = i

    if bc_idx is None:
        return {}

    bc_candle = candles_1d[bc_idx]
    phases["BC"] = {"idx": bc_idx, "price": bc_candle["high"], "vol": bc_vol_max}

    # AR — Automatic Reaction: первый сильный откат после BC
    ar_idx = None
    ar_low = float('inf')
    for i in range(bc_idx + 1, min(bc_idx + 15, len(candles_1d))):
        c = candles_1d[i]
        if c["close"] < c["open"] and c["low"] < ar_low:
            ar_low = c["low"]
            ar_idx = i

    if ar_idx is None:
        return {}

    phases["AR"] = {"idx": ar_idx, "price": ar_low}

    # ST — Secondary Test: тест хая BC с меньшим объёмом
    st_idx = None
    for i in range(ar_idx + 1, min(ar_idx + 20, len(candles_1d))):
        c = candles_1d[i]
        near_bc_high = abs(c["high"] - bc_candle["high"]) / bc_candle["high"] < 0.05
        lower_vol = c["volume"] < bc_vol_max * 0.6
        if near_bc_high and lower_vol:
            st_idx = i
            break

    if st_idx:
        phases["ST"] = {"idx": st_idx, "price": candles_1d[st_idx]["high"]}

    # UTAD — ложный пробой выше BC/ST на 4h (ловушка для покупателей)
    utad_level = phases.get("ST", phases["BC"])["price"]
    utad_found = False

    for c in candles_4h[-30:]:
        if c["high"] > utad_level and c["close"] < utad_level:
            wick = (c["high"] - c["close"]) / (c["high"] - c["low"] + 0.000001)
            if wick > 0.4:
                utad_found = True
                phases["UTAD"] = {"found": True, "price": c["high"]}
                break

    if not utad_found:
        phases["UTAD"] = {"found": False}

    # SOW — Sign of Weakness: пробой AR уровня вниз с объёмом
    sow_found = False
    for c in candles_4h[-10:]:
        if c["close"] < ar_low:
            vol_4h_avg = sum(x["volume"] for x in candles_4h[-20:-10]) / 10 if len(candles_4h) >= 20 else 1
            if c["volume"] > vol_4h_avg * 1.5:
                sow_found = True
                phases["SOW"] = {"found": True, "price": c["close"]}
                break

    if not sow_found:
        phases["SOW"] = {"found": False}

    return phases


@_audit_strategy("WYCKOFF", subtype='SPRING')
def detect_wyckoff_spring(symbol: str) -> dict | None:
    """
    Wyckoff Accumulation Spring — LONG сигнал.
    Полный анализ фаз: SC → AR → ST → Spring → SOS
    Редкий сигнал +30-200%
    """
    try:
        _skip_symbol, _skip_reason = _learn_should_skip(symbol, "BULLISH")
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8367', (_skip_symbol), '_skip_symbol', '_skip_symbol', 8367):
            logging.info(f"[WYCKOFF] {symbol}: {_skip_reason}")
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8369', '_skip_symbol', locals(), '_skip_symbol', 8369)
        raw_candles_1d = get_candles(symbol, "1d", 61)
        raw_candles_4h = get_candles(symbol, "4h", 121)
        candles_1d = get_confirmed_candles(raw_candles_1d)
        candles_4h = get_confirmed_candles(raw_candles_4h)

        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8375', (not candles_1d or len(candles_1d) < 40), 'not candles_1d or len(candles_1d) < 40', 'not candles_1d or len(candles_1d) < 40', 8375):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8376', 'not candles_1d or len(candles_1d) < 40', locals(), 'not candles_1d or len(candles_1d) < 40', 8376)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8377', (not candles_4h or len(candles_4h) < 40), 'not candles_4h or len(candles_4h) < 40', 'not candles_4h or len(candles_4h) < 40', 8377):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8378', 'not candles_4h or len(candles_4h) < 40', locals(), 'not candles_4h or len(candles_4h) < 40', 8378)

        price_now = raw_candles_1d[-1]["close"]
        score = 0
        signals = []

        # ── BTC фильтр для WYCKOFF (4h) ──
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8385', (symbol != "BTCUSDT"), 'BTC фильтр для WYCKOFF (4h)', 'symbol != "BTCUSDT"', 8385):
            btc_ok, btc_reason = btc_allows_signal("BULLISH", use_4h=True)
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8387', (not btc_ok), 'BTC фильтр для WYCKOFF (4h)', 'not btc_ok', 8387):
                logging.info(f"[WYCKOFF BTC Filter] {symbol} LONG пропущен: {btc_reason}")
                return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8389', 'BTC фильтр для WYCKOFF (4h)', locals(), 'not btc_ok', 8389)

        # ── 1. ДАУНТРЕНД 30+ дней ──
        price_peak = max(c["high"] for c in candles_1d[-50:-15])
        drawdown_pct = (price_peak - price_now) / price_peak * 100 if price_peak > 0 else 0

        # Для BTC порог снижен до 12% (BTC редко падает на 20%)
        _wyckoff_min_drawdown = 7 if symbol == "BTCUSDT" else 12
        if drawdown_pct >= 35:
            score += 30
            signals.append(f"✅ Глубокий даунтренд -{drawdown_pct:.0f}% от пика")
        elif drawdown_pct >= _wyckoff_min_drawdown:
            score += 15
            signals.append(f"⚡️ Коррекция -{drawdown_pct:.0f}% от пика")
        else:
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8404', 'drawdown_pct >= _wyckoff_min_drawdown', locals(), 'drawdown_pct >= _wyckoff_min_drawdown', 8404)

        # ── 2. БОКОВИК У ОСНОВАНИЯ (последние 30 дней) ──
        accumulation_candles = candles_1d[-30:]
        acc_high = max(c["high"] for c in accumulation_candles)
        acc_low  = min(c["low"]  for c in accumulation_candles)
        acc_range_pct = (acc_high - acc_low) / acc_low * 100 if acc_low > 0 else 0

        if acc_range_pct < 15:
            score += 20
            signals.append(f"✅ Боковик {acc_range_pct:.1f}% за 20 дней")
        elif acc_range_pct < 25:
            score += 10
            signals.append(f"⚡️ Диапазон {acc_range_pct:.1f}% за 20 дней")
        else:
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8419', 'acc_range_pct < 25', locals(), 'acc_range_pct < 25', 8419)

        # ── 3. ФАЗЫ WYCKOFF ──
        phases = _find_wyckoff_phases_accumulation(candles_1d, candles_4h)

        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8424', (not phases), '3. ФАЗЫ WYCKOFF', 'not phases', 8424):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8425', '3. ФАЗЫ WYCKOFF', locals(), 'not phases', 8425)

        # SC найден
        if "SC" in phases:
            score += 15
            signals.append(f"✅ SC (Selling Climax) — паника с объёмом x{phases['SC']['vol']/sum(c['volume'] for c in candles_1d)/len(candles_1d):.1f}")

        # AR найден
        if "AR" in phases:
            score += 10
            signals.append(f"✅ AR (Automatic Rally) — кит выкупает")

        # ST найден — тест с меньшим объёмом
        if "ST" in phases:
            score += 10
            signals.append(f"✅ ST (Secondary Test) — объём падает на тесте")

        # Spring найден — самый важный!
        spring_found = phases.get("Spring", {}).get("found", False)
        if spring_found:
            score += 25
            signals.append(f"🎯 SPRING! Ложный пробой лоу с возвратом")

        # SOS найден — подтверждение разворота
        sos_found = phases.get("SOS", {}).get("found", False)
        if sos_found:
            score += 20
            signals.append(f"💪 SOS (Sign of Strength) — пробой AR с объёмом!")

        # ── 4. ОБЪЁМ СЖИМАЕТСЯ В БОКОВИКЕ ──
        all_vols = [c["volume"] for c in candles_1d[-50:-20]]
        avg_vol_trend = sum(all_vols) / len(all_vols) if all_vols else 1
        acc_vols = [c["volume"] for c in accumulation_candles]
        avg_vol_acc = sum(acc_vols) / len(acc_vols) if acc_vols else 1
        vol_compression = avg_vol_acc / avg_vol_trend if avg_vol_trend > 0 else 1

        if vol_compression < 0.7:
            score += 15
            signals.append(f"✅ Объём сжался {vol_compression:.0%} (тихое накопление)")

        # ── Минимальный порог ──
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8466', (score < 50), 'Минимальный порог', 'score < 50', 8466):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8467', 'Минимальный порог', locals(), 'score < 50', 8467)
        # Требуем Spring И SOS одновременно (AND, не OR)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8469', (not spring_found or not sos_found), 'Требуем Spring И SOS одновременно (AND, не OR)', 'not spring_found or not sos_found', 8469):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8470', 'Требуем Spring И SOS одновременно (AND, не OR)', locals(), 'not spring_found or not sos_found', 8470)

        # ── Volume/range compression перед входом ──
        # Последние 5-7 свечей должны иметь уменьшающийся диапазон и объём ниже среднего
        try:
            _last_7 = candles_1d[-7:]
            _prev_13 = candles_1d[-20:-7]
            _avg_range_prev = sum(c["high"] - c["low"] for c in _prev_13) / len(_prev_13) if _prev_13 else 1
            _avg_range_last = sum(c["high"] - c["low"] for c in _last_7) / len(_last_7)
            _avg_vol_prev = sum(c["volume"] for c in _prev_13) / len(_prev_13) if _prev_13 else 1
            _avg_vol_last = sum(c["volume"] for c in _last_7) / len(_last_7)
            _range_compress = _avg_range_last / _avg_range_prev if _avg_range_prev > 0 else 1
            _vol_compress = _avg_vol_last / _avg_vol_prev if _avg_vol_prev > 0 else 1
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8483', (_range_compress > 0.85 and _vol_compress > 0.85), '_range_compress > 0.85 and _vol_compress > 0.85', '_range_compress > 0.85 and _vol_compress > 0.85', 8483):
                # Нет сжатия — ещё рано входить
                logging.info(f"[WYCKOFF] {symbol}: нет сжатия (range {_range_compress:.2f}, vol {_vol_compress:.2f}) — ждём")
                return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8486', 'Нет сжатия — ещё рано входить', locals(), '_range_compress > 0.85 and _vol_compress > 0.85', 8486)
            if _range_compress < 0.7:
                score += 10
                signals.append(f"✅ Диапазон сжат {_range_compress:.0%}")
        except Exception:
            pass

        # ── check_entry_timing() — валидация тайминга входа ──
        try:
            _wy_timing = check_entry_timing(candles_4h, "BULLISH", price_now, "4h")
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8496', (not _wy_timing.get("valid", True)), 'check_entry_timing() — валидация тайминга входа', 'not _wy_timing.get("valid", True)', 8496):
                logging.info(f"[WYCKOFF] {symbol}: тайминг входа не подтверждён")
                return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8498', 'check_entry_timing() — валидация тайминга входа', locals(), 'not _wy_timing.get("valid", True)', 8498)
        except Exception:
            pass

        # ── Вход/Стоп/TP ──
        # Entry после pullback к Creek (верхняя граница накопления)
        creek = acc_high  # Creek = AR level / верхняя граница боковика
        # Если цена уже откатила к Creek — входим. Если нет — ждём.
        creek_tolerance = (acc_high - acc_low) * 0.15  # 15% от диапазона боковика
        if abs(price_now - creek) <= creek_tolerance:
            entry = price_now  # Цена у Creek — входим
        elif price_now < creek:
            entry = price_now  # Цена ниже Creek (ещё в зоне накопления) — входим
        else:
            # Цена далеко выше Creek — пропускаем, поезд ушёл
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8513', (price_now > creek * 1.05), 'Цена далеко выше Creek — пропускаем, поезд ушёл', 'price_now > creek * 1.05', 8513):
                return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8514', 'Цена далеко выше Creek — пропускаем, поезд ушёл', locals(), 'price_now > creek * 1.05', 8514)
            entry = price_now

        atr_1d = average_true_range(candles_1d)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8518', (not atr_1d), 'Цена далеко выше Creek — пропускаем, поезд ушёл', 'not atr_1d', 8518):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8519', 'not atr_1d', locals(), 'not atr_1d', 8519)
        spring_low = phases.get("Spring", {}).get("price")
        sc_low = phases.get("SC", {}).get("price")
        structural_low = min(level for level in (spring_low, sc_low, acc_low) if level)
        sl = smart_round(structural_low - atr_1d * 0.25)
        ar_price = phases.get("AR", {}).get("price")

        # OB/FVG для промпта и результата
        _wyk_ob = find_ob(candles_1d, "BULLISH")
        _wyk_fvg = find_fvg(candles_1d, "BULLISH")

        # AR/Creek and Fibonacci range extensions are anchored to the confirmed
        # accumulation range.  No entry-relative percentage target is allowed.
        acc_range = acc_high - acc_low
        fib_1272 = acc_low + acc_range * 1.272
        fib_1618 = acc_low + acc_range * 1.618
        tp, tp2 = select_structural_targets(
            entry, sl,
            [ar_price, fib_1272, fib_1618, price_peak],
            "BULLISH", 2.0, 4.0,
        )
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8540', (tp is None), 'tp is None', 'tp is None', 8540):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8541', 'tp is None', locals(), 'tp is None', 8541)
        logic = ""
        try:
            phase_summary = []
            for ph in ["SC", "AR", "ST", "Spring", "SOS"]:
                if ph in phases:
                    phase_summary.append(ph)
            # Объёмы фаз для Groq
            _phase_vols = []
            if "SC" in phases:
                _phase_vols.append(f"SC vol: {phases['SC'].get('vol', 0):.0f}")
            if "AR" in phases:
                _phase_vols.append(f"AR idx: {phases['AR'].get('idx', '?')}")
            if "ST" in phases:
                _phase_vols.append(f"ST price: {phases['ST'].get('price', 0):.6f}")
            _avg_vol_1d = sum(c["volume"] for c in candles_1d[-30:]) / 30 if candles_1d else 0
            _phase_vols.append(f"avg_vol_1d: {_avg_vol_1d:.0f}")
            _phase_vols.append(f"vol_compression: {vol_compression:.2f}")

            # HTF тренд, Funding, Fear&Greed
            _wy_htf_1d = smc_on_tf(symbol, "1d")
            _wy_htf_1w = smc_on_tf(symbol, "1w")
            _wy_funding = get_funding_rate(symbol)
            _wy_fg = get_fear_greed()
            _wy_fund_str = f"{_wy_funding:+.4f}%" if _wy_funding is not None else "N/A"
            _wy_fg_str = f"{_wy_fg['value']} ({_wy_fg['label']})" if _wy_fg else "N/A"
            _wy_ob_str = f"OB: {_wyk_ob['bottom']:.6f}–{_wyk_ob['top']:.6f}" if _wyk_ob else "OB: нет"
            _wy_fvg_str = f"FVG: {_wyk_fvg['bottom']:.6f}–{_wyk_fvg['top']:.6f}" if _wyk_fvg else "FVG: нет"

            # Pattern history для Groq
            _wy_pat_str = ""
            try:
                _wy_pat = _learn_patterns(symbol, "BULLISH", "1d", "accumulation", 0)
                if _wy_pat.get("found") and _wy_pat.get("samples", 0) >= 3:
                    _wy_pat_str = (f"\nИстория похожих: {_wy_pat['samples']} сделок, "
                                   f"WR: {_wy_pat['win_rate']:.0f}%, avg RR: {_wy_pat['avg_rr']:.1f}, "
                                   f"вердикт: {_wy_pat.get('verdict', '?')}")
            except Exception:
                pass

            _wy_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry else 0
            _wy_rr = round(abs(tp - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0
            _self_rules = get_relevant_rules(symbol, "BULLISH", "WYCKOFF")
            _recent_errors = get_recent_errors(symbol)
            groq_prompt = (
                "Ты SMC трейдер специализирующийся на методе Вайкоффа и накоплении/дистрибуции. "
                "Оцени качество Wyckoff Spring сетапа. "
                f'Ответь СТРОГО JSON: {{"logic": "макс 15 слов", "valid": true/false}}\n\n'
                "БЛОКИРУЙ (valid: false) если:\n"
                "- Spring или SOS отсутствуют или слабые\n"
                "- Объём на Spring не выше среднего\n"
                "- Нет compression (сжатие диапазона и объёма)\n"
                "- RR < 2.5 от текущей цены до целевой\n"
                "- Цена уже выше Creek линии (пропустили вход)\n"
                "- BTC в нисходящем тренде на 4h\n"
                "- SL выставлен математически (entry ± X%), а не за структуру\n\n"
                "ПОДТВЕРЖДАЙ (valid: true) если:\n"
                "- Spring пробил поддержку и вернулся — ликвидность собрана\n"
                "- SOS показал силу покупателей\n"
                "- Объём снижается в боковике (накопление завершается)\n"
                "- Цена у или ниже Creek — идеальный вход\n"
                "- TP = уровень AR (автоматический ралли) или выше\n\n"
                "УРОВНИ УЖЕ РАССЧИТАНЫ СТРАТЕГИЕЙ. НИКОГДА НЕ МЕНЯЙ entry, SL или TP.\n\n"
                f"Данные: Пара: {symbol} Цена: {price_now}\n"
                f"SC лоу: {phases['SC']['price']:.6f} | AR хай: {ar_price:.6f}\n"
                f"Пик до падения: {price_peak:.6f} | Даунтренд: -{drawdown_pct:.0f}%\n"
                f"Боковик: {acc_low:.6f}—{acc_high:.6f}\n"
                f"Фазы: {', '.join(phase_summary)}\n"
                f"Объёмы фаз: {', '.join(_phase_vols)}\n"
                f"Entry: {entry} SL: {sl} TP: {tp} RR: {_wy_rr} | Стоп: {_wy_sl_pct}%\n"
                f"1d: {_wy_htf_1d} | 1w: {_wy_htf_1w}\n"
                f"Funding: {_wy_fund_str} | Fear&Greed: {_wy_fg_str}\n"
                f"{_wy_ob_str} | {_wy_fvg_str}"
                f"{_wy_pat_str}"
                f"{_self_rules}"
                f"{_recent_errors}"
            )
            groq_resp = ask_groq(groq_prompt, max_tokens=120) if legacy_strategy_groq_enabled() else None
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8619', (groq_resp), 'groq_resp', 'groq_resp', 8619):
                import json as _j, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                m = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8623', (m), 'm', 'm', 8623):
                    parsed = _j.loads(m.group())
                    # Groq как фильтр — если valid=false, блокируем
                    if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8626', (not parsed.get("valid", True)), 'Groq как фильтр — если valid=false, блокируем', 'not parsed.get("valid", True)', 8626):
                        logging.info(f"[WYCKOFF Groq] {symbol} LONG: Groq отклонил сигнал")
                        return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8628', 'Groq как фильтр — если valid=false, блокируем', locals(), 'not parsed.get("valid", True)', 8628)
                    if parsed.get("logic"):
                        logic = str(parsed["logic"]).strip()
        except Exception:
            pass

        if not logic:
            logic = f"Spring после SC+AR+ST — разворот Wyckoff"

        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8639', (risk == 0 or not 2.0 <= reward / risk <= 4.0), 'risk == 0 or not 2.0 <= reward / risk <= 4.0', 'risk == 0 or not 2.0 <= reward / risk <= 4.0', 8639):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8640', 'risk == 0 or not 2.0 <= reward / risk <= 4.0', locals(), 'risk == 0 or not 2.0 <= reward / risk <= 4.0', 8640)

        rr     = round(reward / risk, 2)
        tp_pct = round((tp - entry) / entry * 100, 1)
        sl_pct = round((entry - sl) / entry * 100, 1)

        phase_names = [p for p in ["SC", "AR", "ST", "Spring", "SOS"] if p in phases and (p not in ["Spring","SOS"] or phases[p].get("found"))]

        return {
            "symbol": symbol, "direction": "BULLISH",
            "timeframe": "1d", "entry": entry,
            "sl": sl, "tp": tp, "tp2": tp2,
            "sl_pct": sl_pct, "tp_pct": tp_pct, "rr": rr,
            "logic": logic, "score": min(score, 100),
            "drawdown_pct": drawdown_pct, "acc_range": acc_range_pct,
            "spring": spring_found, "sos": sos_found,
            "phases": " → ".join(phase_names),
            "acc_low": acc_low, "acc_high": acc_high,
            "ob": _wyk_ob, "fvg": _wyk_fvg,
            "scan_type": "wyckoff",
        }

    except Exception as e:
        logging.debug(f"detect_wyckoff_spring {symbol}: {e}")
        return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8664', 'detector returned None', locals(), '', 8664)


@_audit_strategy("WYCKOFF", subtype='DISTRIBUTION')
def detect_wyckoff_distribution(symbol: str) -> dict | None:
    """
    Wyckoff Distribution UTAD — SHORT сигнал.
    Полный анализ фаз: BC → AR → ST → UTAD → SOW
    Редкий сигнал -30-200%
    """
    try:
        _skip_symbol, _skip_reason = _learn_should_skip(symbol, "BEARISH")
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8675', (_skip_symbol), '_skip_symbol', '_skip_symbol', 8675):
            logging.info(f"[WYCKOFF] {symbol}: {_skip_reason}")
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8677', '_skip_symbol', locals(), '_skip_symbol', 8677)
        raw_candles_1d = get_candles(symbol, "1d", 61)
        raw_candles_4h = get_candles(symbol, "4h", 121)
        candles_1d = get_confirmed_candles(raw_candles_1d)
        candles_4h = get_confirmed_candles(raw_candles_4h)

        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8683', (not candles_1d or len(candles_1d) < 40), 'not candles_1d or len(candles_1d) < 40', 'not candles_1d or len(candles_1d) < 40', 8683):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8684', 'not candles_1d or len(candles_1d) < 40', locals(), 'not candles_1d or len(candles_1d) < 40', 8684)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8685', (not candles_4h or len(candles_4h) < 40), 'not candles_4h or len(candles_4h) < 40', 'not candles_4h or len(candles_4h) < 40', 8685):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8686', 'not candles_4h or len(candles_4h) < 40', locals(), 'not candles_4h or len(candles_4h) < 40', 8686)

        price_now = raw_candles_1d[-1]["close"]
        score = 0
        signals = []

        # ── BTC фильтр для WYCKOFF DISTRIBUTION (4h) ──
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8693', (symbol != "BTCUSDT"), 'BTC фильтр для WYCKOFF DISTRIBUTION (4h)', 'symbol != "BTCUSDT"', 8693):
            btc_ok, btc_reason = btc_allows_signal("BEARISH", use_4h=True)
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8695', (not btc_ok), 'BTC фильтр для WYCKOFF DISTRIBUTION (4h)', 'not btc_ok', 8695):
                logging.info(f"[WYCKOFF BTC Filter] {symbol} SHORT пропущен: {btc_reason}")
                return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8697', 'BTC фильтр для WYCKOFF DISTRIBUTION (4h)', locals(), 'not btc_ok', 8697)

        # ── 1. АПТРЕНД 30+ дней ──
        price_bottom = min(c["low"] for c in candles_1d[-50:-15])
        pump_pct = (price_now - price_bottom) / price_bottom * 100 if price_bottom > 0 else 0

        if pump_pct >= 50:
            score += 30
            signals.append(f"✅ Аптренд +{pump_pct:.0f}% от основания")
        elif pump_pct >= 30:
            score += 15
            signals.append(f"⚡️ Рост +{pump_pct:.0f}% от основания")
        else:
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8710', 'pump_pct >= 30', locals(), 'pump_pct >= 30', 8710)

        # ── 2. БОКОВИК У ВЕРШИНЫ (последние 30 дней) ──
        distribution_candles = candles_1d[-30:]
        dist_high = max(c["high"] for c in distribution_candles)
        dist_low  = min(c["low"]  for c in distribution_candles)
        dist_range_pct = (dist_high - dist_low) / dist_low * 100 if dist_low > 0 else 0

        if dist_range_pct < 15:
            score += 20
            signals.append(f"✅ Боковик {dist_range_pct:.1f}% у вершины")
        elif dist_range_pct < 25:
            score += 10
            signals.append(f"⚡️ Диапазон {dist_range_pct:.1f}% у вершины")
        else:
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725', 'dist_range_pct < 25', locals(), 'dist_range_pct < 25', 8725)

        # ── 3. ФАЗЫ WYCKOFF DISTRIBUTION ──
        phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)

        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8730', (not phases), '3. ФАЗЫ WYCKOFF DISTRIBUTION', 'not phases', 8730):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8731', '3. ФАЗЫ WYCKOFF DISTRIBUTION', locals(), 'not phases', 8731)

        if "BC" in phases:
            score += 15
            signals.append(f"✅ BC (Buying Climax) — эйфория с объёмом")

        if "AR" in phases:
            score += 10
            signals.append(f"✅ AR (Automatic Reaction) — первый откат")

        if "ST" in phases:
            score += 10
            signals.append(f"✅ ST (Secondary Test) — объём падает на тесте вершины")

        utad_found = phases.get("UTAD", {}).get("found", False)
        if utad_found:
            score += 25
            signals.append(f"🎯 UTAD! Ложный пробой хая — ловушка для покупателей")

        sow_found = phases.get("SOW", {}).get("found", False)
        if sow_found:
            score += 20
            signals.append(f"💪 SOW (Sign of Weakness) — пробой AR вниз с объёмом!")

        # ── 4. ОБЪЁМ СЖИМАЕТСЯ В БОКОВИКЕ ──
        all_vols = [c["volume"] for c in candles_1d[-50:-20]]
        avg_vol_trend = sum(all_vols) / len(all_vols) if all_vols else 1
        dist_vols = [c["volume"] for c in distribution_candles]
        avg_vol_dist = sum(dist_vols) / len(dist_vols) if dist_vols else 1
        vol_compression = avg_vol_dist / avg_vol_trend if avg_vol_trend > 0 else 1

        if vol_compression < 0.7:
            score += 15
            signals.append(f"✅ Объём сжался {vol_compression:.0%} (тихое распределение)")

        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8766', (score < 50), 'score < 50', 'score < 50', 8766):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8767', 'score < 50', locals(), 'score < 50', 8767)
        # Требуем UTAD И SOW одновременно (AND, не OR)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8769', (not utad_found or not sow_found), 'Требуем UTAD И SOW одновременно (AND, не OR)', 'not utad_found or not sow_found', 8769):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8770', 'Требуем UTAD И SOW одновременно (AND, не OR)', locals(), 'not utad_found or not sow_found', 8770)

        # ── Volume/range compression перед входом ──
        try:
            _last_7d = candles_1d[-7:]
            _prev_13d = candles_1d[-20:-7]
            _avg_range_prev_d = sum(c["high"] - c["low"] for c in _prev_13d) / len(_prev_13d) if _prev_13d else 1
            _avg_range_last_d = sum(c["high"] - c["low"] for c in _last_7d) / len(_last_7d)
            _avg_vol_prev_d = sum(c["volume"] for c in _prev_13d) / len(_prev_13d) if _prev_13d else 1
            _avg_vol_last_d = sum(c["volume"] for c in _last_7d) / len(_last_7d)
            _range_comp_d = _avg_range_last_d / _avg_range_prev_d if _avg_range_prev_d > 0 else 1
            _vol_comp_d = _avg_vol_last_d / _avg_vol_prev_d if _avg_vol_prev_d > 0 else 1
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8782', (_range_comp_d > 0.85 and _vol_comp_d > 0.85), '_range_comp_d > 0.85 and _vol_comp_d > 0.85', '_range_comp_d > 0.85 and _vol_comp_d > 0.85', 8782):
                return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8783', '_range_comp_d > 0.85 and _vol_comp_d > 0.85', locals(), '_range_comp_d > 0.85 and _vol_comp_d > 0.85', 8783)  # Нет сжатия
        except Exception:
            pass

        # ── check_entry_timing() ──
        try:
            _wy_timing_d = check_entry_timing(candles_4h, "BEARISH", price_now, "4h")
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8790', (not _wy_timing_d.get("valid", True)), 'check_entry_timing()', 'not _wy_timing_d.get("valid", True)', 8790):
                return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8791', 'check_entry_timing()', locals(), 'not _wy_timing_d.get("valid", True)', 8791)
        except Exception:
            pass

        # ── Вход/Стоп/TP ──
        # Entry после pullback к Creek (нижняя граница дистрибуции)
        creek_d = dist_low  # Creek = AR level / нижняя граница боковика
        creek_tolerance_d = (dist_high - dist_low) * 0.15
        if abs(price_now - creek_d) <= creek_tolerance_d:
            entry = price_now  # Цена у Creek — входим
        elif price_now > creek_d:
            entry = price_now  # Цена выше Creek (ещё в зоне дистрибуции) — входим
        else:
            # Цена далеко ниже Creek — поезд ушёл
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8805', (price_now < creek_d * 0.95), 'Цена далеко ниже Creek — поезд ушёл', 'price_now < creek_d * 0.95', 8805):
                return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8806', 'Цена далеко ниже Creek — поезд ушёл', locals(), 'price_now < creek_d * 0.95', 8806)
            entry = price_now

        atr_1d = average_true_range(candles_1d)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8810', (not atr_1d), 'Цена далеко ниже Creek — поезд ушёл', 'not atr_1d', 8810):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8811', 'not atr_1d', locals(), 'not atr_1d', 8811)
        utad_high = phases.get("UTAD", {}).get("price")
        bc_high = phases.get("BC", {}).get("price")
        structural_high = max(level for level in (utad_high, bc_high, dist_high) if level)
        sl = smart_round(structural_high + atr_1d * 0.25)
        ar_price = phases.get("AR", {}).get("price")

        # OB/FVG для промпта и результата
        _wyk_ob = find_ob(candles_1d, "BEARISH")
        _wyk_fvg = find_fvg(candles_1d, "BEARISH")

        # AR/Ice and Fibonacci range extensions are anchored to the confirmed
        # distribution range.  No entry-relative percentage target is allowed.
        dist_range = dist_high - dist_low
        fib_1272 = dist_high - dist_range * 1.272
        fib_1618 = dist_high - dist_range * 1.618
        tp, tp2 = select_structural_targets(
            entry, sl,
            [ar_price, fib_1272, fib_1618, price_bottom],
            "BEARISH", 2.0, 4.0,
        )
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8832', (tp is None), 'tp is None', 'tp is None', 8832):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8833', 'tp is None', locals(), 'tp is None', 8833)
        logic = ""
        try:
            phase_summary = []
            for ph in ["BC", "AR", "ST", "UTAD", "SOW"]:
                if ph in phases:
                    phase_summary.append(ph)
            # Объёмы фаз для Groq
            _d_phase_vols = []
            if "BC" in phases:
                _d_phase_vols.append(f"BC vol: {phases['BC'].get('vol', 0):.0f}")
            _avg_vol_1d_d = sum(c["volume"] for c in candles_1d[-30:]) / 30 if candles_1d else 0
            _d_phase_vols.append(f"avg_vol_1d: {_avg_vol_1d_d:.0f}")
            _d_phase_vols.append(f"vol_compression: {vol_compression:.2f}")

            # HTF тренд, Funding, Fear&Greed
            _wyd_htf_1d = smc_on_tf(symbol, "1d")
            _wyd_htf_1w = smc_on_tf(symbol, "1w")
            _wyd_funding = get_funding_rate(symbol)
            _wyd_fg = get_fear_greed()
            _wyd_fund_str = f"{_wyd_funding:+.4f}%" if _wyd_funding is not None else "N/A"
            _wyd_fg_str = f"{_wyd_fg['value']} ({_wyd_fg['label']})" if _wyd_fg else "N/A"
            _wyd_ob_str = f"OB: {_wyk_ob['bottom']:.6f}–{_wyk_ob['top']:.6f}" if _wyk_ob else "OB: нет"
            _wyd_fvg_str = f"FVG: {_wyk_fvg['bottom']:.6f}–{_wyk_fvg['top']:.6f}" if _wyk_fvg else "FVG: нет"

            # Pattern history для Groq
            _wyd_pat_str = ""
            try:
                _wyd_pat = _learn_patterns(symbol, "BEARISH", "1d", "distribution", 0)
                if _wyd_pat.get("found") and _wyd_pat.get("samples", 0) >= 3:
                    _wyd_pat_str = (f"\nИстория похожих: {_wyd_pat['samples']} сделок, "
                                    f"WR: {_wyd_pat['win_rate']:.0f}%, avg RR: {_wyd_pat['avg_rr']:.1f}, "
                                    f"вердикт: {_wyd_pat.get('verdict', '?')}")
            except Exception:
                pass

            _wyd_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry else 0
            _wyd_rr = round(abs(tp - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0
            _self_rules = get_relevant_rules(symbol, "BEARISH", "WYCKOFF")
            _recent_errors = get_recent_errors(symbol)
            groq_prompt = (
                "Ты SMC трейдер специализирующийся на методе Вайкоффа Distribution (дистрибуция). "
                "Оцени качество Wyckoff Distribution сетапа для SHORT. "
                f'Ответь СТРОГО JSON: {{"logic": "макс 15 слов", "valid": true/false}}\n\n'
                "БЛОКИРУЙ (valid: false) если:\n"
                "- UTAD или SOW отсутствуют или слабые\n"
                "- Объём на UTAD не выше среднего\n"
                "- RR < 2.5 от текущей цены до целевой\n"
                "- Цена уже ниже AR лоу (пропустили вход)\n"
                "- BTC в восходящем тренде на 4h\n"
                "- SL выставлен математически (entry ± X%), а не за структуру\n\n"
                "ПОДТВЕРЖДАЙ (valid: true) если:\n"
                "- UTAD пробил вершину и вернулся — ликвидность собрана\n"
                "- SOW показал слабость покупателей\n"
                "- Объём снижается у вершины (дистрибуция завершается)\n"
                "- Цена у или выше Ice Line — идеальный SHORT\n"
                "- TP = уровень AR лоу или ниже\n\n"
                "УРОВНИ УЖЕ РАССЧИТАНЫ СТРАТЕГИЕЙ. НИКОГДА НЕ МЕНЯЙ entry, SL или TP.\n\n"
                f"Данные: Пара: {symbol} Цена: {price_now}\n"
                f"BC хай: {phases['BC']['price']:.6f} | AR лоу: {ar_price:.6f}\n"
                f"Основание до роста: {price_bottom:.6f} | Рост: +{pump_pct:.0f}%\n"
                f"Боковик: {dist_low:.6f}—{dist_high:.6f}\n"
                f"Фазы: {', '.join(phase_summary)}\n"
                f"Объёмы фаз: {', '.join(_d_phase_vols)}\n"
                f"Entry: {entry} SL: {sl} TP: {tp} RR: {_wyd_rr} | Стоп: {_wyd_sl_pct}%\n"
                f"1d: {_wyd_htf_1d} | 1w: {_wyd_htf_1w}\n"
                f"Funding: {_wyd_fund_str} | Fear&Greed: {_wyd_fg_str}\n"
                f"{_wyd_ob_str} | {_wyd_fvg_str}"
                f"{_wyd_pat_str}"
                f"{_self_rules}"
                f"{_recent_errors}"
            )
            groq_resp = ask_groq(groq_prompt, max_tokens=120) if legacy_strategy_groq_enabled() else None
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8906', (groq_resp), 'groq_resp', 'groq_resp', 8906):
                import json as _j, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                m = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8910', (m), 'm', 'm', 8910):
                    parsed = _j.loads(m.group())
                    # Groq как фильтр — если valid=false, блокируем
                    if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8913', (not parsed.get("valid", True)), 'Groq как фильтр — если valid=false, блокируем', 'not parsed.get("valid", True)', 8913):
                        logging.info(f"[WYCKOFF Groq] {symbol} SHORT: Groq отклонил сигнал")
                        return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8915', 'Groq как фильтр — если valid=false, блокируем', locals(), 'not parsed.get("valid", True)', 8915)
                    if parsed.get("logic"):
                        logic = str(parsed["logic"]).strip()
        except Exception:
            pass

        if not logic:
            logic = f"UTAD после BC+AR+ST — дистрибуция Wyckoff"

        risk   = abs(sl - entry)
        reward = abs(entry - tp)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8926', (risk == 0 or not 2.0 <= reward / risk <= 4.0), 'risk == 0 or not 2.0 <= reward / risk <= 4.0', 'risk == 0 or not 2.0 <= reward / risk <= 4.0', 8926):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8927', 'risk == 0 or not 2.0 <= reward / risk <= 4.0', locals(), 'risk == 0 or not 2.0 <= reward / risk <= 4.0', 8927)

        rr     = round(reward / risk, 2)
        tp_pct = round((entry - tp) / entry * 100, 1)
        sl_pct = round((sl - entry) / entry * 100, 1)

        phase_names = [p for p in ["BC", "AR", "ST", "UTAD", "SOW"] if p in phases and (p not in ["UTAD","SOW"] or phases[p].get("found"))]

        return {
            "symbol": symbol, "direction": "BEARISH",
            "timeframe": "1d", "entry": entry,
            "sl": sl, "tp": tp, "tp2": tp2,
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
        return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8951', 'detector returned None', locals(), '', 8951)


@_audit_strategy("WYCKOFF", subtype='REACCUMULATION')
def detect_wyckoff_reaccumulation(symbol: str) -> dict | None:
    """
    Re-accumulation: боковик после коррекции + higher lows + ликвидность выше
    Работает чаще чем классический Wyckoff (раз в неделю vs раз в полгода)
    """
    try:
        _skip_symbol, _skip_reason = _learn_should_skip(symbol, "BULLISH")
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8961', (_skip_symbol), '_skip_symbol', '_skip_symbol', 8961):
            logging.info(f"[WYCKOFF] {symbol}: {_skip_reason}")
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8963', '_skip_symbol', locals(), '_skip_symbol', 8963)
        raw_candles_1d = get_candles(symbol, "1d", 61)
        raw_candles_4h = get_candles(symbol, "4h", 101)
        candles_1d = get_confirmed_candles(raw_candles_1d)
        candles_4h = get_confirmed_candles(raw_candles_4h)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8968', (not candles_1d or len(candles_1d) < 30), 'not candles_1d or len(candles_1d) < 30', 'not candles_1d or len(candles_1d) < 30', 8968): return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8968', 'not candles_1d or len(candles_1d) < 30', locals(), 'not candles_1d or len(candles_1d) < 30', 8968)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8969', (not candles_4h or len(candles_4h) < 50), 'not candles_4h or len(candles_4h) < 50', 'not candles_4h or len(candles_4h) < 50', 8969): return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8969', 'not candles_4h or len(candles_4h) < 50', locals(), 'not candles_4h or len(candles_4h) < 50', 8969)

        price_now = raw_candles_1d[-1]["close"]

        # ── 1. Коррекция от пика (5% для BTC/ETH/BNB, 8% для остальных) ──
        price_peak = max(c["high"] for c in candles_1d[-40:-10])
        drawdown_pct = (price_peak - price_now) / price_peak * 100
        _min_drawdown = 3 if symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"] else 5
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8977', (drawdown_pct < _min_drawdown), '1. Коррекция от пика (5% для BTC/ETH/BNB, 8% для остальных)', 'drawdown_pct < _min_drawdown', 8977):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8978', '1. Коррекция от пика (5% для BTC/ETH/BNB, 8% для остальных)', locals(), 'drawdown_pct < _min_drawdown', 8978)

        # ── 2. Боковик последние 10-30 дней (range < 15%) ──
        acc_candles = candles_1d[-30:]
        acc_high = max(c["high"] for c in acc_candles)
        acc_low = min(c["low"] for c in acc_candles)
        acc_range_pct = (acc_high - acc_low) / acc_low * 100
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8985', (acc_range_pct > 15), '2. Боковик последние 10-30 дней (range < 15%)', 'acc_range_pct > 15', 8985):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8986', 'acc_range_pct > 15', locals(), 'acc_range_pct > 15', 8986)

        # ── 3. Higher lows — покупатели давят снизу ──
        lows_20 = [c["low"] for c in acc_candles]
        local_lows = []
        for i in range(1, len(lows_20)-1):
            if lows_20[i] < lows_20[i-1] and lows_20[i] < lows_20[i+1]:
                local_lows.append(lows_20[i])
        higher_lows = len(local_lows) >= 2 and local_lows[-1] > local_lows[-2]
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G8995', (not higher_lows), 'not higher_lows', 'not higher_lows', 8995):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R8996', 'not higher_lows', locals(), 'not higher_lows', 8996)

        # ── 4. Volume compression — объём снижается в боковике ──
        avg_vol_before = sum(c["volume"] for c in candles_1d[-40:-20]) / 20
        avg_vol_acc = sum(c["volume"] for c in acc_candles) / len(acc_candles)
        vol_compressed = avg_vol_acc < avg_vol_before * 0.8
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9002', (not vol_compressed), '4. Volume compression — объём снижается в боковике', 'not vol_compressed', 9002):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9003', '4. Volume compression — объём снижается в боковике', locals(), 'not vol_compressed', 9003)

        # ── 5. Volume expansion — первый взрыв объёма после compression ──
        last_vol = candles_1d[-1]["volume"]
        avg_vol_acc_last = sum(c["volume"] for c in candles_1d[-10:-1]) / 9
        vol_expanding = last_vol > avg_vol_acc_last * 1.5

        # ── 6. Ликвидность выше — EQH или swing high ──
        highs_acc = [c["high"] for c in acc_candles]
        eqh_levels = [h for h in highs_acc if abs(h - acc_high) / acc_high < 0.005]
        liquidity_target = acc_high if len(eqh_levels) >= 2 else price_peak

        # ── 7. BTC фильтр ──
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9016', (symbol != "BTCUSDT"), '7. BTC фильтр', 'symbol != "BTCUSDT"', 9016):
            btc_ok, _ = btc_allows_signal("BULLISH")
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9018', (not btc_ok), '7. BTC фильтр', 'not btc_ok', 9018): return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9018', '7. BTC фильтр', locals(), 'not btc_ok', 9018)

        # ── 8. Расчёт уровней ──
        entry = smart_round(price_now)
        atr_1d = average_true_range(candles_1d)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9023', (not atr_1d), '8. Расчёт уровней', 'not atr_1d', 9023):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9024', '8. Расчёт уровней', locals(), 'not atr_1d', 9024)
        sl = smart_round(acc_low - atr_1d * 0.25)
        acc_range = acc_high - acc_low
        fib_1272 = acc_low + acc_range * 1.272
        fib_1618 = acc_low + acc_range * 1.618
        tp, tp2 = select_structural_targets(
            entry, sl,
            [liquidity_target, fib_1272, fib_1618, price_peak],
            "BULLISH", 2.5, 4.0,
        )
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9034', (tp is None), 'tp is None', 'tp is None', 9034):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9035', 'tp is None', locals(), 'tp is None', 9035)

        risk = abs(entry - sl)
        reward = abs(tp - entry)
        rr = round(reward / risk, 2)

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
            _resp = ask_groq(_wyk_prompt, max_tokens=100) if legacy_strategy_groq_enabled() else None
            if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9084', (_resp), '_resp', '_resp', 9084):
                import json as _j, re as _re
                _m = _re.search(r'\{[^}]+\}', _resp, _re.DOTALL)
                if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9087', (_m), '_m', '_m', 9087):
                    _p = _j.loads(_m.group())
                    if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9089', (not _p.get("valid", True)), 'not _p.get("valid", True)', 'not _p.get("valid", True)', 9089): return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9089', 'not _p.get("valid", True)', locals(), 'not _p.get("valid", True)', 9089)
        except Exception:
            pass

        # TP remains the structural liquidity target calculated above.
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9096', (risk == 0), 'TP remains the structural liquidity target calculated above.', 'risk == 0', 9096):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9097', 'TP remains the structural liquidity target calculated above.', locals(), 'risk == 0', 9097)
        rr = round(reward / risk, 2)
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9099', (not 2.5 <= rr <= 4.0), 'not 2.5 <= rr <= 4.0', 'not 2.5 <= rr <= 4.0', 9099):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9100', 'not 2.5 <= rr <= 4.0', locals(), 'not 2.5 <= rr <= 4.0', 9100)
        tp_pct = round((tp - entry) / entry * 100, 1)
        sl_pct = round((entry - sl) / entry * 100, 1)
        return {
            "symbol": symbol, "direction": "BULLISH",
            "timeframe": "1d", "entry": entry, "sl": sl, "tp": tp, "tp2": tp2,
            "sl_pct": sl_pct, "tp_pct": tp_pct, "rr": rr,
            "score": 75, "signals": signals,
            "logic": f"Re-accumulation: higher lows + liquidity {smart_price_fmt(liquidity_target)}",
            "drawdown_pct": drawdown_pct, "acc_range": acc_range_pct,
            "phases": "Re-accumulation",
        }
    except Exception as e:
        logging.warning(f"detect_wyckoff_reaccumulation {symbol}: {e}")
        return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9114', 'detector returned None', locals(), '', 9114)


# ===== СТРАТЕГИЯ 4: FAST DEAL 5M СКАЛЬПИНГ =====

FAST_PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT", "DOTUSDT",
    "MATICUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT",
    "SUIUSDT", "INJUSDT", "FETUSDT", "WIFUSDT", "PEPEUSDT"
]

@_audit_strategy("FAST")
def detect_fast_deal(symbol: str) -> dict | None:
    """
    SMC FAST: 4h context, confirmed 15m setup and optional 5m control:
    1. BTC направление — синхронизация с рынком
    2. 4h/1h trend — торгуем только по тренду
    3. 4h OB/FVG — цена в зоне интереса
    4. 15m displacement/engulfing с объёмом — подтверждение
    5. SL и TP — только за структурой/ликвидностью, без фиксированных %.
    """
    try:
        # One DST-aware session clock is shared with bot.py so scheduled and
        # manual FAST scans make the same decision in summer and winter.
        if _audit_test('FAST_DETECT_FAST_DEAL_G9138', (not _fast_session()), 'manual FAST scans make the same decision in summer and winter.', 'not _fast_session()', 9138):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9139', 'manual FAST scans make the same decision in summer and winter.', locals(), 'not _fast_session()', 9139)

        # ── 1. BTC направление ──
        btc_candles_1h = get_candles("BTCUSDT", "1h", 10)
        btc_trend = "BULLISH" if btc_candles_1h and btc_candles_1h[-1]["close"] > btc_candles_1h[-3]["close"] else "BEARISH"

        # ── 2. 4h+1h consensus (мягкий — один из двух достаточно) ──
        direction_4h = smc_on_tf(symbol, "4h")
        direction_1h = smc_on_tf(symbol, "1h")
        if _audit_test('FAST_DETECT_FAST_DEAL_G9148', (not direction_4h and not direction_1h), '2. 4h+1h consensus (мягкий — один из двух достаточно)', 'not direction_4h and not direction_1h', 9148):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9149', '2. 4h+1h consensus (мягкий — один из двух достаточно)', locals(), 'not direction_4h and not direction_1h', 9149)
        # Берём направление: приоритет 4h, fallback 1h
        direction_1d = direction_4h or direction_1h
        # Для редкого скальпа не берём конфликтующие 4h/1h направления.
        if _audit_test('FAST_DETECT_FAST_DEAL_G9153', (direction_4h and direction_1h and direction_4h != direction_1h), 'Для редкого скальпа не берём конфликтующие 4h/1h направления.', 'direction_4h and direction_1h and direction_4h != direction_1h', 9153):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9154', 'Для редкого скальпа не берём конфликтующие 4h/1h направления.', locals(), 'direction_4h and direction_1h and direction_4h != direction_1h', 9154)

        # BTC фильтр: только для альткоинов — BTCUSDT не фильтруем через себя
        if _audit_test('FAST_DETECT_FAST_DEAL_G9157', (symbol != "BTCUSDT"), 'BTC фильтр: только для альткоинов — BTCUSDT не фильтруем через себя', 'symbol != "BTCUSDT"', 9157):
            if _audit_test('FAST_DETECT_FAST_DEAL_G9158', (direction_1d == "BULLISH" and btc_trend == "BEARISH"), 'BTC фильтр: только для альткоинов — BTCUSDT не фильтруем через себя', 'direction_1d == "BULLISH" and btc_trend == "BEARISH"', 9158):
                return _audit_fail('FAST_DETECT_FAST_DEAL_R9159', 'BTC фильтр: только для альткоинов — BTCUSDT не фильтруем через себя', locals(), 'direction_1d == "BULLISH" and btc_trend == "BEARISH"', 9159)
            if _audit_test('FAST_DETECT_FAST_DEAL_G9160', (direction_1d == "BEARISH" and btc_trend == "BULLISH"), 'BTC фильтр: только для альткоинов — BTCUSDT не фильтруем через себя', 'direction_1d == "BEARISH" and btc_trend == "BULLISH"', 9160):
                try:
                    btc_change = (btc_candles_1h[-1]["close"] - btc_candles_1h[-4]["close"]) / btc_candles_1h[-4]["close"] * 100
                    if _audit_test('FAST_DETECT_FAST_DEAL_G9163', (btc_change > 1.0), 'BTC фильтр: только для альткоинов — BTCUSDT не фильтруем через себя', 'btc_change > 1.0', 9163):
                        return _audit_fail('FAST_DETECT_FAST_DEAL_R9164', 'btc_change > 1.0', locals(), 'btc_change > 1.0', 9164)  # BTC растёт >1% — шорт альт опасен
                except Exception:
                    pass

        direction = direction_1d

        try:
            _skip_symbol, _skip_reason = _learn_should_skip(symbol, direction)
            if _audit_test('FAST_DETECT_FAST_DEAL_G9172', (_skip_symbol), '_skip_symbol', '_skip_symbol', 9172):
                logging.info(f"[FAST] {symbol}: {_skip_reason}")
                return _audit_fail('FAST_DETECT_FAST_DEAL_R9174', '_skip_symbol', locals(), '_skip_symbol', 9174)
        except Exception:
            pass

        # ── 2.5. Extreme funding is a warning for the final quality gate ──
        _fast_funding_warning = ""
        try:
            _fast_funding = get_funding_rate(symbol)
            if _fast_funding is not None and abs(_fast_funding) > 0.2:
                if (direction == "BULLISH" and _fast_funding > 0.2) or (direction == "BEARISH" and _fast_funding < -0.2):
                    _fast_funding_warning = f"extreme crowded funding {_fast_funding:+.4f}%"
                    logging.info("[FAST Funding Warning] %s: %s", symbol, _fast_funding_warning)
        except Exception:
            pass

        # ── 3. 4h OB/FVG зона ──
        raw_candles_4h = get_candles(symbol, "4h", 51)
        candles_4h = get_confirmed_candles(raw_candles_4h)
        if _audit_test('FAST_DETECT_FAST_DEAL_G9192', (not candles_4h or len(candles_4h) < 20), '3. 4h OB/FVG зона', 'not candles_4h or len(candles_4h) < 20', 9192):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9193', '3. 4h OB/FVG зона', locals(), 'not candles_4h or len(candles_4h) < 20', 9193)

        price_now = raw_candles_4h[-1]["close"]
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

        if _audit_test('FAST_DETECT_FAST_DEAL_G9224', (not in_zone), 'not in_zone', 'not in_zone', 9224):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9225', 'not in_zone', locals(), 'not in_zone', 9225)

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
        if _audit_test('FAST_DETECT_FAST_DEAL_G9237', (not _no_middle_ok), 'not _no_middle_ok', 'not _no_middle_ok', 9237):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9238', 'not _no_middle_ok', locals(), 'not _no_middle_ok', 9238)

        # ── 4. 15m импульсная свеча (подтверждение на младшем ТФ) ──
        candles_15m_imp = get_confirmed_candles(get_candles(symbol, "15m", 21))
        if _audit_test('FAST_DETECT_FAST_DEAL_G9242', (not candles_15m_imp or len(candles_15m_imp) < 3), '4. 15m импульсная свеча (подтверждение на младшем ТФ)', 'not candles_15m_imp or len(candles_15m_imp) < 3', 9242):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9243', '4. 15m импульсная свеча (подтверждение на младшем ТФ)', locals(), 'not candles_15m_imp or len(candles_15m_imp) < 3', 9243)

        last_15m = candles_15m_imp[-1]

        # Volume check на 15m impulse — должен быть выше среднего
        _avg_vol_15m_imp = sum(c.get("volume", 0) for c in candles_15m_imp[:-1]) / max(len(candles_15m_imp) - 1, 1)
        if _audit_test('FAST_DETECT_FAST_DEAL_G9249', (_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1), 'Volume check на 15m impulse — должен быть выше среднего', '_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1', 9249):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9250', 'Volume check на 15m impulse — должен быть выше среднего', locals(), '_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1', 9250)  # Импульс без объёма — ненадёжный

        # ── 5. 15m Engulfing + Displacement + Volume Spike ──
        candles_15m = get_confirmed_candles(get_candles(symbol, "15m", 31))
        if _audit_test('FAST_DETECT_FAST_DEAL_G9254', (not candles_15m or len(candles_15m) < 10), '5. 15m Engulfing + Displacement + Volume Spike', 'not candles_15m or len(candles_15m) < 10', 9254):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9255', '5. 15m Engulfing + Displacement + Volume Spike', locals(), 'not candles_15m or len(candles_15m) < 10', 9255)

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

        if _audit_test('FAST_DETECT_FAST_DEAL_G9303', (not engulfing_found or entry is None), 'not engulfing_found or entry is None', 'not engulfing_found or entry is None', 9303):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9304', 'not engulfing_found or entry is None', locals(), 'not engulfing_found or entry is None', 9304)

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

        if _audit_test('FAST_DETECT_FAST_DEAL_G9320', (not _acceptance), 'not _acceptance', 'not _acceptance', 9320):
            logging.debug(f"[FAST] {symbol}: нет acceptance — цена не закрылась за зоной")
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9322', 'not _acceptance', locals(), 'not _acceptance', 9322)

        # FAST still needs a real, recent close-confirmed structural break.
        # Engulfing/volume alone cannot substitute for BOS/CHoCH.
        _fast_structure_event = get_bos_choch_event(
            candles_15m,
            direction,
            lookback=15,
            max_break_age=min(3, max(1, _sweep_candles_ago)),
        )
        if _audit_test('FAST_DETECT_FAST_DEAL_G9332', (not _fast_structure_event), 'not _fast_structure_event', 'not _fast_structure_event', 9332):
            logging.debug(f"[FAST] {symbol}: нет свежего 15m BOS/CHoCH")
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9334', 'not _fast_structure_event', locals(), 'not _fast_structure_event', 9334)

        # ── TP = confirmed 15m swing liquidity ──
        _fast_highs, _fast_lows = find_swings(candles_15m, lookback=3)
        if direction == "BULLISH":
            _fast_targets = sorted({level for _, level in _fast_highs if level > entry * 1.001})
        else:
            _fast_targets = sorted(
                {level for _, level in _fast_lows if level < entry * 0.999}, reverse=True
            )
        if _audit_test('FAST_DETECT_FAST_DEAL_G9344', (not _fast_targets), 'not _fast_targets', 'not _fast_targets', 9344):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9345', 'not _fast_targets', locals(), 'not _fast_targets', 9345)
        tp1 = smart_round(_fast_targets[0])
        tp2 = smart_round(_fast_targets[1]) if len(_fast_targets) > 1 else tp1
        tp = tp2  # основной TP для RR расчёта

        # ── RR проверка ──
        risk   = abs(entry - sl)
        reward = abs(tp1 - entry)
        if _audit_test('FAST_DETECT_FAST_DEAL_G9353', (risk == 0), 'RR проверка', 'risk == 0', 9353):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9354', 'RR проверка', locals(), 'risk == 0', 9354)
        rr = round(reward / risk, 2)
        if _audit_test('FAST_DETECT_FAST_DEAL_G9356', (not 2.0 <= rr <= 4.0), 'not 2.0 <= rr <= 4.0', 'not 2.0 <= rr <= 4.0', 9356):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9357', 'not 2.0 <= rr <= 4.0', locals(), 'not 2.0 <= rr <= 4.0', 9357)

        sl_pct = round(abs(entry - sl) / entry * 100, 2)
        tp_pct = round(abs(tp1 - entry) / entry * 100, 2)
        tp2_pct = round(abs(tp2 - entry) / entry * 100, 2)
        entry_drift_pct = round(max(0.15, min(0.60, atr_15m / entry * 50)), 2)

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
                "Ты Kill Zone скальпер — торгуешь только в подтверждённой London/NY сессии.\n"
                'Отвечай СТРОГО JSON: {"logic": "макс 10 слов", "valid": true/false}\n\n'
                "КАК ДУМАТЬ:\n"
                "1. 15m engulfing + displacement — тело > 65% range, поглощение предыдущей свечи\n"
                "2. 4h OB или FVG подтверждает зону — институционалы там входили\n"
                "3. Volume spike 2.0x — реальный интерес на engulfing свече\n"
                "4. Acceptance — цена закрылась за зоной OB/FVG\n"
                "5. Закрытая 15m свеча подтверждает настоящий BOS/CHoCH\n"
                "6. BTC и 1d тренд совпадают — не иди против рынка\n\n"
                "БЛОКИРУЙ если:\n"
                f"- RR={rr} < 1.5\n"
                f"- Стоп {_fast_sl_pct}% > 1.5% от входа (скальп = узкий стоп)\n"
                "- Нет OB и нет FVG на 4h — вход без подтверждения зоны\n"
                "- Нет свежего подтверждённого BOS/CHoCH на закрытой 15m свече\n"
                "- 1d тренд ПРОТИВ направления\n"
                "- BTC тренд ПРОТИВ направления\n"
                "- Вне переданной приложением London/NY Kill Zone\n"
                "- SL выставлен математически (entry ± X%), а не за структуру\n\n"
                "ПОДТВЕРЖДАЙ если:\n"
                "- Engulfing чёткий с объёмом 2.0x+\n"
                "- Есть свежий BOS/CHoCH в направлении сделки\n"
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
                f"Структура: {_fast_structure_event['type']} @ {_fast_structure_event['level']} | closed=true\n"
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
            groq_resp = ask_groq(groq_prompt, max_tokens=80) if legacy_strategy_groq_enabled() else None
            if _audit_test('FAST_DETECT_FAST_DEAL_G9442', (groq_resp), 'groq_resp', 'groq_resp', 9442):
                import json as _j, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                m = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if _audit_test('FAST_DETECT_FAST_DEAL_G9446', (m), 'm', 'm', 9446):
                    parsed = _j.loads(m.group())
                    # Groq как фильтр — блокируем только если явно valid=false
                    if _audit_test('FAST_DETECT_FAST_DEAL_G9449', (not parsed.get("valid", True)), 'Groq как фильтр — блокируем только если явно valid=false', 'not parsed.get("valid", True)', 9449):
                        return _audit_fail('FAST_DETECT_FAST_DEAL_R9450', 'Groq как фильтр — блокируем только если явно valid=false', locals(), 'not parsed.get("valid", True)', 9450)
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
            "timeframe": "15m",
            "entry":     entry,
            "sl":        sl,
            "tp":        tp,
            "tp1":       tp1,
            "tp2":       tp2,
            "sl_pct":    sl_pct,
            "tp_pct":    tp_pct,
            "tp2_pct":   tp2_pct,
            "entry_drift_pct": entry_drift_pct,
            "rr":        rr,
            "logic":     logic,
            "zone":      zone_desc,
            "direction_1d": direction_1d,
            "funding_warning": _fast_funding_warning,
            "ob":        ob_4h,
            "fvg":       fvg_4h,
            "fast_score": 0,
            "scan_type": "fast",
            "structure_event": _fast_structure_event,
        }

    except Exception as e:
        logging.debug(f"detect_fast_deal {symbol}: {e}")
        return _audit_fail('FAST_DETECT_FAST_DEAL_R9488', 'detector returned None', locals(), '', 9488)


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
        for _period in (20, 50, 200):
            _ema = ema_value(closes, _period)
            if _ema is not None:
                result[f"ema{_period}"] = _ema

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
    Сравнивает последнюю закрытую свечу с нормой за 20 закрытых свечей.

    Последняя строка ответа биржи — ещё формирующаяся свеча. Сравнивать её
    частичный объём с полными свечами нельзя: сканы в начале часа/4h/дня
    получали искусственно низкий ratio и блокировались до запуска стратегии.
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
        if not candles or len(candles) < 22:
            return result

        closed_vol = candles[-2].get("volume", 0)
        avg_vol = sum(c.get("volume", 0) for c in candles[-22:-2]) / 20
        if avg_vol <= 0:
            return result

        ratio = round(closed_vol / avg_vol, 2)
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
    # This historical helper is not routed by any active strategy and still
    # contains fixed-percentage/ATR target fallbacks.  Fail closed if an old
    # integration calls it instead of a canonical strategy builder.
    logging.warning("smc_core_check is deprecated; canonical strategy scanner required")
    return None

    # Unreachable legacy implementation retained for source compatibility.
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
