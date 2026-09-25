# APEX_STRATEGY_STATS_V1
from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail, audit_observe as _audit_observe, emit_event as _emit_stats_event
from core.market_data_health import record_market_data as _record_market_data
import asyncio
import logging
import requests
import threading
import time
import json
from datetime import datetime, timedelta

from apex.db.connection import connect_compatibility as _connect_compatibility_db
from apex.config.settings import ApexConfig as _ApexConfig

from groq import Groq
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from core.pair_universe import (
    DEFAULT_UNIVERSE_SIZE,
)
from apex.compatibility.market_constants import (
    FAST_PAIRS, SYMBOL_ALIASES, TF_CATEGORIES, TF_LABELS,
)
from apex.market.runtime_cache import (
    get_confirmed_candles, get_global_candles,
    last_closed_candle_time as _last_closed_candle_time, prune_global_candles,
    update_global_candles,
)
from apex.ui.price_format import smart_price_fmt
from apex.ui.market_format import format_accumulation, format_market_prices, format_news
from apex.ui.risk_calculator import calc_risk
from apex.ui.price_alerts import PriceAlertService, check_alerts, configure_price_alerts
from apex.ui.live_position import (
    LivePositionService, configure_live_position_service, live_position_analysis,
)
from apex.ui.profile_extraction import (
    ProfileExtractionService, configure_profile_extraction,
    extract_and_save_profile,
)
from apex.ui.groq_runtime import (
    _GROQ_DAILY_LIMIT, _tokens_available, _track_tokens,
    configure_legacy_strategy_groq, legacy_strategy_groq_enabled,
)
from apex.db.legacy_signal_persistence import (
    LegacySignalPersistence, configure_signal_persistence, save_signal_db,
)
from apex.db.legacy_pending_signals import (
    check_pending_signals, configure_pending_signal_monitor,
)
from apex.app.health_server import run_server
from apex.db.compatibility_runtime import db_write_async, get_db_conn, start_db_writer
from apex.market.time_estimate import get_estimated_time
from apex.market.news_provider import get_crypto_news, get_market_impact_news, parse_rss
from apex.market.gate_tickers import get_all_market_pairs, get_live_prices, get_top_pairs
from apex.market.gate_orderbook import get_orderbook
from apex.market.optional_prices import (
    get_cryptocompare_candles, get_cryptocompare_prices,
    get_messari_data, get_yahoo_finance_prices,
)
from apex.market.optional_context import OptionalContextProviders
from apex.market.quote_provider import get_price_realtime
from apex.market.context_quotes import get_fear_greed, get_funding_rate, get_open_interest
from apex.market.optional_signals import OptionalSignalProviders
from apex.market.macro_context import get_dxy_signal, get_fg_history, get_upcoming_events
from apex.market.candle_router import GateCandleRouter
from apex.market.derived_context import LegacyDerivedContext
from apex.market.structure_bridge import (
    classify_swings, detect_bos_choch, detect_events, find_equal_highs_lows,
    find_swings, get_bos_choch_event,
)
from apex.market.historical_context import HistoricalContextProvider, format_historical_context
from apex.market.smc_analysis import LegacySmcAnalysis
from apex.market.legacy_zones import find_fvg, find_ob
from apex.market.indicators import average_true_range, ema_value
from apex.market.adaptive_indicators import LegacyAdaptiveIndicators
from apex.market.session_liquidity import SessionLiquidityProvider
from apex.market.btc_correlation import BtcCorrelationProvider
from apex.market.btc_direction_filter import BtcDirectionFilter
from apex.market.candle_patterns import detect_engulfing
from apex.market.entry_timing import check_entry_timing
from apex.market.structural_levels import (
    calc_smart_levels, select_structural_targets, smart_round,
)
from apex.market.regime_v2 import LegacyRegimeV2
from apex.market.liquidation_context import get_liquidation_ratio
from apex.strategies.legacy_scan_registry import (
    analyze_trade_type, full_scan, register_raw_scan_handler, run_raw_scan,
)
from apex.ui.user_memory import (
    get_chat_history, get_user_memory, save_chat_log, update_user_memory,
)
from apex.ui.context_store import (
    get_knowledge, get_recent_news, save_knowledge, save_news,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Внешние модули APEX ──────────────────────────────────────
import sys as _sys, os as _os_path
# Добавляем папку core/ в путь поиска модулей — файлы могут лежать там
_BASE_DIR = _os_path.path.dirname(_os_path.path.dirname(_os_path.path.dirname(
    _os_path.path.abspath(__file__)
)))
for _p in [_os_path.path.join(_BASE_DIR, "core"), _BASE_DIR]:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

try:
    from apex.strategies.common import fast_session as _fast_session
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
    _lifecycle_register_waiting = lambda *_args, **_kwargs: None
    _lifecycle_state_for = None
    _lifecycle_touch = None
    _lifecycle_entry_touched = None
    _lifecycle_mark_active = None
    _lifecycle_barrier_hits = None
    _lifecycle_mark_finished = None
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

_APEX_CONFIG = _ApexConfig.from_env()
configure_legacy_strategy_groq(
    _APEX_CONFIG.integrations.legacy_strategy_groq
)
DB_PATH = _APEX_CONFIG.database.compatibility_db_path
configure_signal_persistence(LegacySignalPersistence(
    _connect_compatibility_db, DB_PATH, _lifecycle_register_waiting,
    lifecycle_available=_SIGNAL_LIFECYCLE_OK,
))
TOKEN = _APEX_CONFIG.integrations.telegram_token
ADMIN_IDS = list(_APEX_CONFIG.integrations.telegram_admin_ids)
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0
SIGNAL_CHANNEL = _APEX_CONFIG.integrations.signal_channel_swing
SIGNAL_CHANNEL_MAIN = _APEX_CONFIG.integrations.signal_channel_main
SIGNAL_CHANNEL_SWING = _APEX_CONFIG.integrations.signal_channel_swing
SWING_THREAD_ID = _APEX_CONFIG.integrations.swing_thread_id
FAST_DEAL_THREAD_ID = _APEX_CONFIG.integrations.fast_thread_id
GROQ_KEY = _APEX_CONFIG.integrations.groq_api_key
GROQ_KEYS = list(_APEX_CONFIG.integrations.groq_api_keys)
_groq_key_index = 0
TAVILY_KEY = _APEX_CONFIG.integrations.tavily_api_key
_OPTIONAL_CONTEXT = OptionalContextProviders(_APEX_CONFIG.integrations)
get_twelvedata_candles = _OPTIONAL_CONTEXT.get_twelvedata_candles
get_mobula_price = _OPTIONAL_CONTEXT.get_mobula_price
get_coinalyze_data = _OPTIONAL_CONTEXT.get_coinalyze_data
get_lunarcrush_data = _OPTIONAL_CONTEXT.get_lunarcrush_data
_OPTIONAL_SIGNALS = OptionalSignalProviders(_APEX_CONFIG.integrations)
get_liquidations = _OPTIONAL_SIGNALS.get_liquidations
get_santiment_data = _OPTIONAL_SIGNALS.get_santiment_data
get_whale_alerts = _OPTIONAL_SIGNALS.get_whale_alerts

from aiohttp import ClientSession as _ClientSession, ClientTimeout as _ClientTimeout
from apex.quality.groq_schema import configured_groq_models, is_model_unavailable_error
_timeout = _ClientTimeout(total=30, connect=10)
bot = Bot(token=TOKEN)
dp = Dispatcher()
configure_price_alerts(PriceAlertService(
    _connect_compatibility_db, DB_PATH, get_live_prices, bot.send_message,
))


logging.basicConfig(level=logging.INFO)
groq_client = Groq(api_key=GROQ_KEY)
configure_profile_extraction(ProfileExtractionService(
    groq_client, configured_groq_models, get_user_memory, update_user_memory,
))

# ===== DATABASE =====

def init_db():
    conn = _connect_compatibility_db(DB_PATH, timeout=30, check_same_thread=False)
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

    # Миграция alerts — добавляем price_level если нет
    try:
        c.execute("ALTER TABLE alerts ADD COLUMN price_level REAL")
    except Exception:
        pass

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

    conn.commit()
    conn.close()

# Динамический кэш топ-100 пар
candle_cache = {}  # {symbol_interval: (candles, timestamp)}
_CANDLE_ROUTER = GateCandleRouter(
    cache=candle_cache,
    get_shared=lambda symbol, interval: get_global_candles(symbol, interval),
    update_shared=lambda symbol, interval, candles: update_global_candles(
        symbol, interval, candles,
    ),
    fetch_gate=lambda symbol, interval, limit: get_candles_smart(
        symbol, interval, limit,
    ),
    gate_available=lambda: _SMC_ENGINE_OK,
    record_health=lambda *args, **kwargs: _record_market_data(*args, **kwargs),
    last_closed_at=lambda candles: _last_closed_candle_time(candles),
)
get_candles = _CANDLE_ROUTER.get_candles
fetch_candles_batch = _CANDLE_ROUTER.fetch_candles_batch
_DERIVED_CONTEXT = LegacyDerivedContext(lambda symbol, interval, limit: get_candles(
    symbol, interval, limit,
))
get_higher_tf_context = _DERIVED_CONTEXT.get_higher_tf_context
get_market_regime = _DERIVED_CONTEXT.get_market_regime
_HISTORICAL_CONTEXT = HistoricalContextProvider(
    lambda symbol, interval, limit: get_candles(symbol, interval, limit),
)
get_historical_context = _HISTORICAL_CONTEXT.get_historical_context
_SMC_ANALYSIS = LegacySmcAnalysis(
    engine_available=lambda: _SMC_ENGINE_OK,
    smart_multi_tf=lambda symbol, timeframes: _smc_multi_tf(symbol, timeframes),
    get_candles=lambda symbol, interval, limit: get_candles(symbol, interval, limit),
    get_confirmed_candles=get_confirmed_candles,
    find_swings=find_swings,
    classify_swings=classify_swings,
    detect_events=detect_events,
    timeframe_labels=TF_LABELS,
)
smc_on_tf = _SMC_ANALYSIS.smc_on_tf
multi_tf_analysis = _SMC_ANALYSIS.multi_tf_analysis

# ── In-memory caches ──
_ADAPTIVE_INDICATORS = LegacyAdaptiveIndicators(get_candles, ema_value)
get_precomputed_indicators = _ADAPTIVE_INDICATORS.get_precomputed_indicators
get_adaptive_params = _ADAPTIVE_INDICATORS.get_adaptive_params
_SESSION_LIQUIDITY = SessionLiquidityProvider(get_candles)
check_session_liquidity = _SESSION_LIQUIDITY.check
_BTC_CORRELATION = BtcCorrelationProvider(get_candles, get_global_candles)


def clear_market_runtime_caches() -> dict[str, int]:
    """Release optional recomputable market caches under resource pressure."""
    from core.smc_engine import prune_candle_cache

    counts = {
        "router_candles": len(candle_cache),
        "shared_candles": prune_global_candles(force=True),
        "smc_candles": prune_candle_cache(force=True),
        "derived_context": len(_DERIVED_CONTEXT._higher_timeframe_cache) + len(_DERIVED_CONTEXT._regime_cache),
        "indicators": len(_ADAPTIVE_INDICATORS._indicator_cache) + len(_ADAPTIVE_INDICATORS._adaptive_cache),
        "session_liquidity": len(_SESSION_LIQUIDITY._cache),
        "btc_correlation": len(_BTC_CORRELATION._cache),
    }
    candle_cache.clear()
    _DERIVED_CONTEXT._higher_timeframe_cache.clear()
    _DERIVED_CONTEXT._higher_timeframe_cache_time.clear()
    _DERIVED_CONTEXT._regime_cache.clear()
    _DERIVED_CONTEXT._regime_cache_time.clear()
    _ADAPTIVE_INDICATORS._indicator_cache.clear()
    _ADAPTIVE_INDICATORS._adaptive_cache.clear()
    _SESSION_LIQUIDITY._cache.clear()
    _BTC_CORRELATION._cache.clear()
    return counts
get_btc_correlation = _BTC_CORRELATION.get
_BTC_DIRECTION = BtcDirectionFilter(get_candles)
get_btc_1h_change = _BTC_DIRECTION.one_hour_change
get_btc_4h_change = _BTC_DIRECTION.four_hour_change
btc_allows_signal = _BTC_DIRECTION.allows_signal
_REGIME_V2 = LegacyRegimeV2(get_candles)
detect_market_regime_v2 = _REGIME_V2.detect
configure_live_position_service(LivePositionService(
    get_candles, find_swings, classify_swings, detect_events, find_ob, find_fvg,
    TF_LABELS,
))

format_market = lambda: format_market_prices(get_live_prices())

# ===== SMC ENGINE =====

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

def _emit_trade_stats_event(action, sig_id, symbol, signal_type, direction, entry, sl, tp1, tp2=None, tp3=None, *, result="", exit_price=None, hours=None):
    """Passive OPEN/CLOSE telemetry for Strategy Lab; never affects trade state."""
    try:
        entry_f = float(entry or 0)
        sl_f = float(sl or 0)
        exit_f = float(exit_price) if exit_price is not None else None
        risk = abs(entry_f - sl_f) if entry_f and sl_f else 0.0
        pnl_pct = None
        realized_r = None
        if exit_f is not None and entry_f > 0:
            signed = (exit_f - entry_f) if str(direction).upper() == "BULLISH" else (entry_f - exit_f)
            pnl_pct = round(signed / entry_f * 100.0, 4)
            if risk > 0:
                realized_r = round(signed / risk, 3)
        strategy = str(signal_type or "UNKNOWN").upper()
        payload = {
            "action": str(action or "").upper(), "signal_id": int(sig_id),
            "symbol": str(symbol or "").upper(), "strategy": strategy,
            "direction": str(direction or "").upper(), "entry": entry_f or None,
            "sl": sl_f or None, "tp1": float(tp1) if tp1 else None,
            "tp2": float(tp2) if tp2 else None, "tp3": float(tp3) if tp3 else None,
            "result": str(result or "").lower(), "exit_price": exit_f,
            "pnl_pct": pnl_pct, "realized_r": realized_r,
            "planned_rr": round(abs(float(tp1) - entry_f) / risk, 3) if tp1 and risk > 0 else None,
            "hours": round(float(hours), 2) if hours is not None else None,
        }
        suffix = str(result or "open").lower() if str(action).upper() == "CLOSE" else str(action or "event").lower()
        _emit_stats_event("trade_event", strategy, symbol, payload, event_key=f"trade:{int(sig_id)}:{str(action).lower()}:{suffix}")
    except Exception as exc:
        logging.debug("[TradeStats] emit skipped for %s: %s", sig_id, exc)


configure_pending_signal_monitor(
    get_db_conn_fn=get_db_conn,
    get_live_prices_fn=get_live_prices,
    get_candles_fn=get_candles,
    connector=_connect_compatibility_db,
    database_path=DB_PATH,
    lifecycle_available=_SIGNAL_LIFECYCLE_OK,
    lifecycle_active=_LIFECYCLE_ACTIVE,
    lifecycle_cancelled=_LIFECYCLE_CANCELLED,
    lifecycle_waiting=_LIFECYCLE_WAITING,
    lifecycle_state_for=_lifecycle_state_for,
    lifecycle_activated_at_for=_lifecycle_activated_at_for,
    lifecycle_touch=_lifecycle_touch,
    lifecycle_entry_touched=_lifecycle_entry_touched,
    lifecycle_mark_active=_lifecycle_mark_active,
    lifecycle_barrier_hits=_lifecycle_barrier_hits,
    lifecycle_mark_finished=_lifecycle_mark_finished,
    emit_trade_stats_event=_emit_trade_stats_event,
)


# ===== TAVILY =====

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

# Словарь блокировок ключей: {key_index: timestamp когда получил rate limit}
_key_rate_limited: dict = {}

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
                _request_kwargs = {
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "timeout": 30,
                }
                if str(model).startswith("openai/gpt-oss-"):
                    # GPT-OSS can spend a small completion budget on hidden reasoning
                    # and leave message.content empty. Keep reasoning low and request
                    # only final content for APEX's machine-readable quality gates.
                    _request_kwargs.update({
                        "max_completion_tokens": max_tokens,
                        "reasoning_effort": "low",
                        "include_reasoning": False,
                    })
                else:
                    _request_kwargs["max_tokens"] = max_tokens
                r = client.chat.completions.create(**_request_kwargs)
                _track_tokens(len(prompt) // 4 + max_tokens)
                content = r.choices[0].message.content or ""
                if not content.strip():
                    logging.warning("Groq model %s returned empty final content; trying fallback", model)
                    continue
                _groq_key_index = (key_index + 1) % len(active_keys)
                return content
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
# ===== СИСТЕМА 4: УМНЫЙ ASK_AI С АВТО-РЕСЁРЧЕМ =====

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
                scan_result = run_raw_scan(found_symbol, "1h", False)
                if scan_result:
                    return scan_result.get("text") if isinstance(scan_result, dict) else scan_result
                else:
                    # Нет сигнала — объясняем почему
                    price_data = prices.get(found_symbol)
                    price_str = f"${price_data['price']:,.4f}" if price_data else "нет данных"
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



def _swing_build_ltf_entry(symbol: str, direction: str, tp: float) -> dict:
    """Refine a valid 4h SWING thesis into a fresh 1h/15m executable entry.

    The 4h layer defines direction and structural target. Entry timing is delegated
    to closed 1h/15m candles so a thesis can remain alive while the bot waits for
    a fresh BOS/CHoCH, OB/FVG retest, displacement and volume confirmation.
    """
    out = {
        "data_ok": False, "structure_ok": False, "zone_ok": False,
        "retest_ok": False, "displacement_ok": False, "volume_ok": False,
        "chase_ok": False, "target_ok": False, "ready": False,
        "entry": None, "sl": None, "structure_event": None,
        "zone_type": None, "zone": None,
    }
    try:
        c1h = get_confirmed_candles(get_candles(symbol, "1h", 61))
        c15 = get_confirmed_candles(get_candles(symbol, "15m", 81))
        if not c1h or len(c1h) < 30 or not c15 or len(c15) < 30:
            return out
        out["data_ok"] = True

        event = get_bos_choch_event(c1h, direction, lookback=8, max_break_age=2)
        if not event:
            return out
        _swing_bos_age = max(1, len(c1h) - int(event.get("candle_index", len(c1h) - 1)))
        _audit_observe("bos_event", {
            "role": "SWING_ENTRY_STRUCTURE", "timeframe": "1h", "age_bars": _swing_bos_age,
            "event_type": event.get("type"), "direction": event.get("direction"),
        })
        _audit_observe("bos_progress", {"structure_confirmed": True})
        out["structure_event"] = event
        out["structure_ok"] = True

        h1_ranges = [max(0.0, float(c["high"]) - float(c["low"])) for c in c1h[-14:]]
        atr1h = sum(h1_ranges) / len(h1_ranges) if h1_ranges else 0.0
        if atr1h <= 0:
            return out

        zones = []
        for zone_type, zone in (("OB", find_ob(c1h, direction)), ("FVG", find_fvg(c1h, direction))):
            if not isinstance(zone, dict):
                continue
            try:
                bottom, top = float(zone["bottom"]), float(zone["top"])
            except (KeyError, TypeError, ValueError):
                continue
            if top <= bottom:
                continue
            zones.append((zone_type, bottom, top))
        if not zones:
            return out
        out["zone_ok"] = True
        _audit_observe("bos_progress", {"zone_reached": True, "zone_confirmed": True})

        latest = c15[-1]
        current = float(latest["close"])
        tolerance = atr1h * 0.15
        touched = []
        for zone_type, bottom, top in zones:
            recent_touch = any(
                float(c["low"]) <= top + tolerance and float(c["high"]) >= bottom - tolerance
                for c in c15[-4:]
            )
            if not recent_touch:
                continue
            distance = 0.0 if bottom <= current <= top else min(abs(current - bottom), abs(current - top))
            touched.append((distance, zone_type, bottom, top))
        if not touched:
            return out
        touched.sort(key=lambda x: x[0])
        distance, zone_type, bottom, top = touched[0]
        out["retest_ok"] = True
        _audit_observe("bos_progress", {"retest_reached": True, "retest_confirmed": True})
        out["zone_type"] = zone_type
        out["zone"] = {"bottom": bottom, "top": top}

        candle_range = float(latest["high"]) - float(latest["low"])
        candle_body = abs(float(latest["close"]) - float(latest["open"]))
        direction_ok = (
            direction == "BULLISH" and float(latest["close"]) > float(latest["open"])
        ) or (
            direction == "BEARISH" and float(latest["close"]) < float(latest["open"])
        )
        displacement_ok = candle_range > 0 and candle_body / candle_range >= 0.50 and direction_ok
        out["displacement_ok"] = bool(displacement_ok)

        vol_window = c15[-21:-1] if len(c15) >= 21 else c15[:-1]
        avg_vol = sum(float(c.get("volume", 0) or 0) for c in vol_window) / len(vol_window) if vol_window else 0.0
        last_vol = float(latest.get("volume", 0) or 0)
        out["volume_ok"] = bool(avg_vol > 0 and last_vol >= avg_vol * 1.20)

        out["chase_ok"] = bool(distance <= atr1h * 0.75)
        _audit_observe("swing_numeric", {
            # Raw candle body/range is useful diagnostics but is not identical
            # to the gate because the gate also requires the candle direction.
            "displacement_body_ratio": round(candle_body / candle_range, 6) if candle_range > 0 else None,
            "direction_ok": bool(direction_ok),
            "directional_displacement_ratio": (
                round(candle_body / candle_range, 6) if candle_range > 0 and direction_ok else 0.0
            ),
            "displacement_gate_pass": bool(out["displacement_ok"]),
            "volume_ratio": round(last_vol / avg_vol, 6) if avg_vol > 0 else None,
            "volume_pass_1_2": bool(avg_vol > 0 and last_vol >= avg_vol * 1.20),
            "volume_pass_1_1_observed": bool(avg_vol > 0 and last_vol >= avg_vol * 1.10),
            "retest_distance_atr": round(distance / atr1h, 6) if atr1h > 0 else None,
        })
        _audit_observe("bos_progress", {
            "displacement_reached": True, "displacement_confirmed": bool(out["displacement_ok"]),
            "volume_reached": True, "volume_confirmed": bool(out["volume_ok"]),
        })
        if not out["displacement_ok"] or not out["volume_ok"] or not out["chase_ok"]:
            return out

        entry = current
        sw_highs, sw_lows = find_swings(c15, lookback=2)
        if direction == "BULLISH":
            lows_below = [float(level) for _, level in sw_lows if float(level) < entry]
            nearest_swing = max(lows_below) if lows_below else bottom
            anchor = min(bottom, nearest_swing)
            sl = anchor - atr1h * 0.10
            target_ok = float(tp) > entry
        else:
            highs_above = [float(level) for _, level in sw_highs if float(level) > entry]
            nearest_swing = min(highs_above) if highs_above else top
            anchor = max(top, nearest_swing)
            sl = anchor + atr1h * 0.10
            target_ok = float(tp) < entry
        out["target_ok"] = bool(target_ok)
        if not target_ok:
            return out
        if direction == "BULLISH" and sl >= entry:
            return out
        if direction == "BEARISH" and sl <= entry:
            return out

        out["entry"] = smart_round(entry)
        out["sl"] = smart_round(sl)
        out["ready"] = True
        _audit_observe("bos_progress", {"ltf_ready": True})
        return out
    except Exception as exc:
        logging.debug("[SWING LTF] %s refinement failed: %s", symbol, exc)
        return out


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

        # ── Swing highs/lows: still structural, but aligned with the later sweep detector. ──
        # lookback=12 rejected almost the whole universe before the actual sweep/CHoCH logic.
        # Seven bars remains selective while allowing genuine 4h swing structure to reach the trigger layer.
        swing_highs, swing_lows = find_swings(candles, lookback=7)
        if _audit_test('SWING_DETECT_SWING_SETUP_G7142', (len(swing_highs) < 2 or len(swing_lows) < 2), 'Свинг-структура найдена (lookback=7)', 'len(swing_highs) < 2 or len(swing_lows) < 2', 7142):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7143', 'Свинг-структура найдена (lookback=7)', locals(), 'len(swing_highs) < 2 or len(swing_lows) < 2', 7143)

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
        if not direction:
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

                # RR is validated only after the 1h/15m entry is refined.
                # The provisional 4h thesis levels are not executable levels.
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

        # ── BTC фильтр для SWING ──
        if symbol != 'BTCUSDT':
            btc_ok, btc_reason = btc_allows_signal(direction)
            if _audit_test('SWING_DETECT_SWING_SETUP_G7350', (not btc_ok), 'BTC фильтр для SWING', 'not btc_ok', 7350):
                logging.info(f"[SWING BTC Filter] {symbol} {direction} пропущен: {btc_reason}")
                return _audit_fail('SWING_DETECT_SWING_SETUP_R7352', 'BTC фильтр для SWING', locals(), 'not btc_ok', 7352)

        # ── 4h sweep quality is thesis context; execution quality is checked on 15m. ──
        _swing_4h_volume_ok = False
        _swing_4h_displacement_ok = False
        try:
            from datetime import datetime as _dt_vol
            _vol_hour = _dt_vol.utcnow().hour
            _vol_mult = 1.3 if 8 <= _vol_hour <= 21 else 1.2
            sweep_candle = trigger_candle or candles[-1]
            avg_vol = sum(c["volume"] for c in candles[-20:-1]) / 19 if len(candles) >= 20 else 0
            sweep_vol = sweep_candle.get("volume", 0)
            _swing_4h_volume_ok = bool(avg_vol > 0 and sweep_vol >= avg_vol * _vol_mult)
        except Exception:
            pass
        try:
            _disp_candle = candles[-trigger_lookback + 1] if trigger_lookback >= 2 else (trigger_candle or candles[-1])
            _disp_range = _disp_candle["high"] - _disp_candle["low"]
            _disp_body = abs(_disp_candle["close"] - _disp_candle["open"])
            _disp_ratio = _disp_body / _disp_range if _disp_range > 0 else 0.0
            _disp_direction_ok = (
                direction == "BULLISH" and _disp_candle["close"] > _disp_candle["open"]
            ) or (
                direction == "BEARISH" and _disp_candle["close"] < _disp_candle["open"]
            )
            _swing_4h_displacement_ok = bool(_disp_ratio >= 0.50 and _disp_direction_ok)
        except Exception:
            pass

        # ── 4h BOS/CHoCH is thesis-quality context, not a second hard trigger. ──
        # Direction already comes from a 4h sweep/EQH-EQL/OB reaction. Execution
        # still requires a fresh 1h BOS/CHoCH and the 15m entry trigger below.
        _swing_structure_event = get_bos_choch_event(
            candles,
            direction,
            lookback=30,
            max_break_age=max(1, trigger_lookback),
        )
        _audit_test(
            'SWING_4H_STRUCTURE_CONTEXT',
            (not _swing_structure_event),
            '4h BOS/CHoCH context after trigger (non-blocking)',
            'not _swing_structure_event',
            7403,
        )
        if not _swing_structure_event:
            logging.info(f"[SWING] {symbol}: 4h BOS/CHoCH не подтверждён — context weak; обязательный 1h trigger остаётся")

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

        # 1h/15m execution is refined below after the 4h context bonuses are prepared.
        _swing_1h_choch = False
        _swing_1h_structure_event = None

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

        # ── Two-stage SWING: 4h thesis -> fresh 1h/15m executable entry. ──
        _swing_thesis_entry = entry
        _swing_thesis_sl = sl
        _swing_thesis_tp = tp
        entry = sl = None  # do not count provisional 4h levels as a near/executable deal
        _ltf = _swing_build_ltf_entry(symbol, direction, tp)
        if _audit_test('SWING_LTF_DATA', (not _ltf.get("data_ok")), 'LTF data: 1h + 15m history', 'not _ltf.data_ok', 7460):
            return _audit_fail('SWING_LTF_R_DATA', 'LTF data: 1h + 15m history', locals(), 'not _ltf.data_ok', 7460)
        if _audit_test('SWING_LTF_STRUCTURE', (not _ltf.get("structure_ok")), 'LTF: fresh 1h BOS/CHoCH', 'not _ltf.structure_ok', 7461):
            return _audit_fail('SWING_LTF_R_STRUCTURE', 'LTF: fresh 1h BOS/CHoCH', locals(), 'not _ltf.structure_ok', 7461)
        if _audit_test('SWING_LTF_ZONE', (not _ltf.get("zone_ok")), 'LTF: 1h OB/FVG zone', 'not _ltf.zone_ok', 7462):
            return _audit_fail('SWING_LTF_R_ZONE', 'LTF: 1h OB/FVG zone', locals(), 'not _ltf.zone_ok', 7462)
        if _audit_test('SWING_LTF_RETEST', (not _ltf.get("retest_ok")), 'LTF: recent 15m retest of 1h OB/FVG', 'not _ltf.retest_ok', 7463):
            return _audit_fail('SWING_LTF_R_RETEST', 'LTF: recent 15m retest of 1h OB/FVG', locals(), 'not _ltf.retest_ok', 7463)
        if _audit_test('SWING_LTF_DISPLACEMENT', (not _ltf.get("displacement_ok")), 'LTF: 15m displacement >= 50% in direction', 'not _ltf.displacement_ok', 7464):
            return _audit_fail('SWING_LTF_R_DISPLACEMENT', 'LTF: 15m displacement >= 50% in direction', locals(), 'not _ltf.displacement_ok', 7464)
        if _audit_test('SWING_LTF_VOLUME', (not _ltf.get("volume_ok")), 'LTF: 15m volume >= 1.2x average', 'not _ltf.volume_ok', 7465):
            return _audit_fail('SWING_LTF_R_VOLUME', 'LTF: 15m volume >= 1.2x average', locals(), 'not _ltf.volume_ok', 7465)
        if _audit_test('SWING_LTF_NO_CHASE', (not _ltf.get("chase_ok")), 'LTF: entry still near retest zone', 'not _ltf.chase_ok', 7466):
            return _audit_fail('SWING_LTF_R_NO_CHASE', 'LTF: entry still near retest zone', locals(), 'not _ltf.chase_ok', 7466)
        if _audit_test('SWING_LTF_TARGET', (not _ltf.get("target_ok")), 'LTF: 4h structural target remains ahead of entry', 'not _ltf.target_ok', 7467):
            return _audit_fail('SWING_LTF_R_TARGET', 'LTF: 4h structural target remains ahead of entry', locals(), 'not _ltf.target_ok', 7467)
        if _audit_test('SWING_LTF_READY', (not _ltf.get("ready")), 'LTF executable entry ready', 'not _ltf.ready', 7468):
            return _audit_fail('SWING_LTF_R_READY', 'LTF executable entry ready', locals(), 'not _ltf.ready', 7468)

        entry = _ltf["entry"]
        sl = _ltf["sl"]
        _swing_1h_structure_event = _ltf.get("structure_event")
        _swing_1h_choch = bool(_swing_1h_structure_event)
        logic = logic + f" -> LTF {_ltf.get('zone_type') or 'zone'} retest + 15m trigger"

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
        _audit_observe("bos_progress", {"rr_reached": True, "rr_passed": bool(rr_check >= 2.0)})
        if _audit_test('SWING_DETECT_SWING_SETUP_G7511', (rr_check < 2.0), 'rr_check < 2.0', 'rr_check < 2.0', 7511):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7512', 'rr_check < 2.0', locals(), 'rr_check < 2.0', 7512)

        # ── Фильтр — цель должна быть реальной ──
        if _audit_test('SWING_DETECT_SWING_SETUP_G7515', (direction == "BULLISH" and tp <= entry), 'Фильтр — цель должна быть реальной', 'direction == "BULLISH" and tp <= entry', 7515):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7516', 'Фильтр — цель должна быть реальной', locals(), 'direction == "BULLISH" and tp <= entry', 7516)
        if _audit_test('SWING_DETECT_SWING_SETUP_G7517', (direction == "BEARISH" and tp >= entry), 'Фильтр — цель должна быть реальной', 'direction == "BEARISH" and tp >= entry', 7517):
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7518', 'Фильтр — цель должна быть реальной', locals(), 'direction == "BEARISH" and tp >= entry', 7518)

        # ── HTF: блок только если ОБА (4h И 1d) против ──
        htf_4h_sw = smc_on_tf(symbol, "4h")
        htf_1d_sw = smc_on_tf(symbol, "1d")
        htf_dir = htf_1d_sw  # для совместимости ниже

        if direction == 'BULLISH':
            _4h_against = htf_4h_sw and "BEARISH" in str(htf_4h_sw).upper()
            _1d_against = htf_1d_sw and "BEARISH" in str(htf_1d_sw).upper()
            if _audit_test('SWING_DETECT_SWING_SETUP_G7528', (_4h_against and _1d_against), '_4h_against and _1d_against', '_4h_against and _1d_against', 7528):
                return _audit_fail('SWING_DETECT_SWING_SETUP_R7529', '_4h_against and _1d_against', locals(), '_4h_against and _1d_against', 7529)  # оба HTF против — блок
        elif direction == 'BEARISH':
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
            _sw_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry > 0 else 0
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
            )

            groq_response = ask_groq(groq_prompt, max_tokens=100) if legacy_strategy_groq_enabled() else None
            if groq_response and len(groq_response) > 5:
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
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7796', (not candles or len(candles) < 40), 'Достаточно 4H истории (≥40 закрытых свечей)', 'not candles or len(candles) < 40', 7796):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7797', 'Достаточно 4H истории (≥40 закрытых свечей)', locals(), 'not candles or len(candles) < 40', 7797)

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

        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7811', (range_size < atr * 2), 'ZONE: range size >= 2 ATR', 'range_size < atr * 2', 7811):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7812', 'ZONE: range size >= 2 ATR', locals(), 'range_size < atr * 2', 7812)  # Диапазон слишком мал

        # Require a real range extreme and leave the middle 40% neutral.
        in_discount = price <= range_low + range_size * 0.30
        in_premium = price >= range_high - range_size * 0.30
        _audit_observe("zone_numeric", {
            "range_position_pct": round((price - range_low) / range_size * 100, 6) if range_size > 0 else None,
            "range_atr": round(range_size / atr, 6) if atr > 0 else None,
        })

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
        _audit_observe("zone_numeric", {
            "zone_distance_atr": round(abs(price - zone_level) / atr, 6) if atr > 0 else None,
        })

        # ── 2.5. Проверка свежести зоны (unmitigated + strong move away) ──
        if zone_level and zone_type:
            try:
                _test_count = 0
                _zone_top = ob["top"] if zone_type == "OB" and ob else (fvg["top"] if fvg else zone_level * 1.01)
                _zone_bot = ob["bottom"] if zone_type == "OB" and ob else (fvg["bottom"] if fvg else zone_level * 0.99)

                for c in candles[-40:-3]:
                    if _zone_bot <= c["low"] <= _zone_top or _zone_bot <= c["high"] <= _zone_top:
                        _test_count += 1

                _audit_observe("zone_numeric", {"test_count": _test_count})
                if _audit_test('ZONE_DETECT_ZONE_SETUP_G7866', (_test_count > 2), 'ZONE: fresh zone has at most 2 prior tests', '_test_count > 2', 7866):
                    logging.debug(f"[ZONE] {symbol}: зона протестирована {_test_count} раз — mitigated")
                    return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7868', 'ZONE: fresh zone has at most 2 prior tests', locals(), '_test_count > 2', 7868)

                # Strong move away: displacement ≥0.5 + body > ATR×1.0
                _strong_move = False
                _zone_best_displacement = 0.0
                _zone_best_body_atr = 0.0
                for i in range(max(-len(candles), -35), -3):
                    c = candles[i]
                    c_body = abs(c["close"] - c["open"])
                    c_range = c["high"] - c["low"]
                    _zone_directional = (direction == "BULLISH" and c["close"] > c["open"]) or (direction == "BEARISH" and c["close"] < c["open"])
                    if _zone_directional and c_range > 0:
                        _zone_best_displacement = max(_zone_best_displacement, c_body / c_range)
                        if atr > 0:
                            _zone_best_body_atr = max(_zone_best_body_atr, c_body / atr)
                    if c_range > 0 and c_body / c_range >= 0.5 and c_body > atr * _vf_zone * 0.8:
                        if direction == "BULLISH" and c["close"] > c["open"]:
                            _strong_move = True
                            break
                        elif direction == "BEARISH" and c["close"] < c["open"]:
                            _strong_move = True
                            break

                _audit_observe("zone_numeric", {
                    "best_directional_displacement_ratio": round(_zone_best_displacement, 6),
                    "best_directional_body_atr": round(_zone_best_body_atr, 6),
                })
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
        if htf_1d:
            if _audit_test('ZONE_DETECT_ZONE_SETUP_G7907', (direction == "BULLISH" and "BEARISH" in str(htf_1d).upper()), '4. HTF фильтры', 'direction == "BULLISH" and "BEARISH" in str(htf_1d).upper()', 7907):
                return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7908', '4. HTF фильтры', locals(), 'direction == "BULLISH" and "BEARISH" in str(htf_1d).upper()', 7908)
            if _audit_test('ZONE_DETECT_ZONE_SETUP_G7909', (direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()), '4. HTF фильтры', 'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()', 7909):
                return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7910', 'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()', locals(), 'direction == "BEARISH" and "BULLISH" in str(htf_1d).upper()', 7910)

        # ── 5. BTC фильтр ──
        if symbol != 'BTCUSDT':
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
        # Decision passport only: these named components mirror the existing
        # score exactly and do not add, remove, or reorder any trading gate.
        _zone_quality_components = {
            "wick_rejection": {"role": "QUALITY", "passed": False},
            "closed_1h_bos_choch": {"role": "HARD_GATE", "passed": False},
            "rsi_30_70": {"role": "QUALITY", "passed": False, "value": None},
            "directional_fvg": {"role": "QUALITY", "passed": False},
            "btc_4h_alignment": {"role": "QUALITY", "passed": False},
            "funding_neutral_or_contrarian": {"role": "QUALITY", "passed": False, "value": None},
            "rejection_volume_1_3x": {"role": "QUALITY", "passed": False, "value": None},
            "recent_imbalance": {"role": "QUALITY_PENALTY", "passed": None},
        }

        # Q0: Wick rejection — тень > тела
        try:
            if direction == "BULLISH":
                _wick_z = candles[-1]["close"] - candles[-1]["low"]
                _body_z = abs(candles[-1]["close"] - candles[-1]["open"])
                if _wick_z > _body_z:
                    q_score += 1
                    _zone_quality_components["wick_rejection"]["passed"] = True
            else:
                _wick_z = candles[-1]["high"] - candles[-1]["close"]
                _body_z = abs(candles[-1]["close"] - candles[-1]["open"])
                if _wick_z > _body_z:
                    q_score += 1
                    _zone_quality_components["wick_rejection"]["passed"] = True
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
                _zone_quality_components["closed_1h_bos_choch"]["passed"] = True
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
            _zone_quality_components["rsi_30_70"]["value"] = rsi_val
            if rsi_val is not None and 30 <= rsi_val <= 70:
                q_score += 1
                _zone_quality_components["rsi_30_70"]["passed"] = True
        except Exception:
            pass

        # Q4: FVG между entry и TP в направлении
        try:
            if fvg and direction == "BULLISH" and fvg["bottom"] > price:
                q_score += 1
                _zone_quality_components["directional_fvg"]["passed"] = True
            elif fvg and direction == "BEARISH" and fvg["top"] < price:
                q_score += 1
                _zone_quality_components["directional_fvg"]["passed"] = True
        except Exception:
            pass

        # Q5: BTC на 4h в том же направлении
        try:
            btc_4h = smc_on_tf("BTCUSDT", "4h")
            if btc_4h and direction in str(btc_4h).upper():
                q_score += 1
                _zone_quality_components["btc_4h_alignment"]["passed"] = True
        except Exception:
            pass

        # Q6: Funding rate нейтральный или против толпы. Extreme funding is
        # risk context for Groq, not a deterministic veto.
        _zone_funding_warning = ""
        try:
            fr = get_funding_rate(symbol)
            _zone_quality_components["funding_neutral_or_contrarian"]["value"] = fr
            if fr is not None:
                if direction == "BULLISH" and fr < 0:
                    q_score += 1  # Шорты накопились — хорошо для LONG
                    _zone_quality_components["funding_neutral_or_contrarian"]["passed"] = True
                elif direction == "BEARISH" and fr > 0:
                    q_score += 1  # Лонги накопились — хорошо для SHORT
                    _zone_quality_components["funding_neutral_or_contrarian"]["passed"] = True
                elif abs(fr) < 0.05:
                    q_score += 1  # Нейтральный
                    _zone_quality_components["funding_neutral_or_contrarian"]["passed"] = True
                elif (direction == "BULLISH" and fr > 0.2) or (direction == "BEARISH" and fr < -0.2):
                    _zone_funding_warning = f"extreme crowded funding {fr:+.4f}%"
        except Exception:
            pass

        # Q7: Свеча с объёмом на отбое > 1.3x avg
        try:
            avg_vol = sum(c["volume"] for c in candles[-20:-1]) / 19
            _zone_quality_components["rejection_volume_1_3x"]["value"] = (
                last["volume"] / avg_vol if avg_vol else None
            )
            if last["volume"] > avg_vol * 1.3:
                q_score += 1
                _zone_quality_components["rejection_volume_1_3x"]["passed"] = True
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
            _zone_quality_components["recent_imbalance"]["passed"] = _imbalance_found
        except Exception:
            pass

        # Structure is already mandatory above. Optional confirmations are
        # wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.
        _zone_ap = get_adaptive_params(symbol, candles)
        _zone_vf = _zone_ap.get("volatility_factor", 1.0) if _zone_ap else 1.0
        _q_min = 3
        _audit_observe("zone_numeric", {"quality_score": q_score, "quality_required": _q_min})
        if _audit_test('ZONE_DETECT_ZONE_SETUP_G8050', (q_score < _q_min), 'wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.', 'q_score < _q_min', 8050):
            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R8051', 'wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.', locals(), 'q_score < _q_min', 8051)  # Недостаточно независимых подтверждений

        # ── 8. Расчёт entry / SL / TP ──
        if direction == 'BULLISH':
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
            )
            _zone_resp = ask_groq(_zone_prompt, max_tokens=80) if legacy_strategy_groq_enabled() else None
            if _zone_resp:
                import json as _j, re as _re
                _clean = _re.sub(r'```json|```', '', _zone_resp).strip()
                _m = _re.search(r'\{[^}]+\}', _clean, _re.DOTALL)
                if _m:
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
            "quality_components": _zone_quality_components,
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
        if symbol != 'BTCUSDT':
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

        # Shadow-only structural box comparison. Production keeps using the
        # established 30d range until the downstream Spring+SOS sample proves
        # that the structural alternative is selective enough.
        _audit_observe("wyckoff_accumulation", {
            "acc_range_pct": round(acc_range_pct, 6),
            "old_range_under_25": bool(acc_range_pct < 25),
        })
        try:
            _telemetry_phases_acc = _find_wyckoff_phases_accumulation(candles_1d, candles_4h)
            _telemetry_points_acc = []
            for _telemetry_name_acc in ("SC", "AR", "ST"):
                _telemetry_phase_acc = _telemetry_phases_acc.get(_telemetry_name_acc) if isinstance(_telemetry_phases_acc, dict) else None
                if isinstance(_telemetry_phase_acc, dict) and _telemetry_phase_acc.get("price") is not None:
                    _telemetry_points_acc.append((_telemetry_name_acc, float(_telemetry_phase_acc["price"])))
            if len(_telemetry_points_acc) >= 2:
                _telemetry_prices_acc = [p for _, p in _telemetry_points_acc]
                _telemetry_box_low_acc = min(_telemetry_prices_acc)
                _telemetry_box_high_acc = max(_telemetry_prices_acc)
                _telemetry_box_width_acc = (
                    (_telemetry_box_high_acc - _telemetry_box_low_acc) / _telemetry_box_low_acc * 100
                    if _telemetry_box_low_acc > 0 else None
                )
                _telemetry_structural_acc = bool(_telemetry_box_width_acc is not None and _telemetry_box_width_acc < 25)
                _telemetry_spring_acc = bool((_telemetry_phases_acc.get("Spring") or {}).get("found"))
                _telemetry_sos_acc = bool((_telemetry_phases_acc.get("SOS") or {}).get("found"))
                _audit_observe("wyckoff_accumulation", {
                    "structural_box_width_pct": round(_telemetry_box_width_acc, 6) if _telemetry_box_width_acc is not None else None,
                    "structural_box_under_25": _telemetry_structural_acc,
                    "observed_phase_ready": bool(_telemetry_structural_acc and _telemetry_spring_acc and _telemetry_sos_acc),
                    "spring_found": _telemetry_spring_acc,
                    "sos_found": _telemetry_sos_acc,
                    "structure_points": {name: price for name, price in _telemetry_points_acc},
                })
        except Exception:
            pass

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
            "BULLISH", 2.0, None,
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
            _wy_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry else 0
            _wy_rr = round(abs(tp - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0
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
            )
            groq_resp = ask_groq(groq_prompt, max_tokens=120) if legacy_strategy_groq_enabled() else None
            if groq_resp:
                import json as _j, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                m = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if m:
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
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_SPRING_G8639', (risk == 0 or reward / risk < 2.0), 'risk == 0 or reward / risk < 2.0', 'risk == 0 or reward / risk < 2.0', 8639):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_SPRING_R8640', 'risk == 0 or reward / risk < 2.0', locals(), 'risk == 0 or reward / risk < 2.0', 8640)

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
        if symbol != 'BTCUSDT':
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
        _audit_observe("wyckoff_distribution", {
            "dist_range_pct": round(dist_range_pct, 6),
            "old_range_under_25": bool(dist_range_pct < 25),
        })
        try:
            _telemetry_phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)
            _telemetry_points = []
            for _telemetry_name in ("BC", "AR", "ST"):
                _telemetry_phase = _telemetry_phases.get(_telemetry_name) if isinstance(_telemetry_phases, dict) else None
                if isinstance(_telemetry_phase, dict) and _telemetry_phase.get("price") is not None:
                    _telemetry_points.append((_telemetry_name, float(_telemetry_phase["price"])))
            if len(_telemetry_points) >= 2:
                _telemetry_prices = [p for _, p in _telemetry_points]
                _telemetry_box_low = min(_telemetry_prices)
                _telemetry_box_high = max(_telemetry_prices)
                _telemetry_box_width_pct = (
                    (_telemetry_box_high - _telemetry_box_low) / _telemetry_box_low * 100
                    if _telemetry_box_low > 0 else None
                )
                _audit_observe("wyckoff_distribution", {
                    "distribution_box_width_pct": round(_telemetry_box_width_pct, 6) if _telemetry_box_width_pct is not None else None,
                    "structural_box_under_25": bool(_telemetry_box_width_pct < 25) if _telemetry_box_width_pct is not None else None,
                    "observed_phase_ready": bool(
                        _telemetry_box_width_pct is not None and _telemetry_box_width_pct < 25
                        and bool((_telemetry_phases.get("UTAD") or {}).get("found"))
                        and bool((_telemetry_phases.get("SOW") or {}).get("found"))
                    ),
                    "utad_found": bool((_telemetry_phases.get("UTAD") or {}).get("found")),
                    "sow_found": bool((_telemetry_phases.get("SOW") or {}).get("found")),
                    "structure_points": {name: price for name, price in _telemetry_points},
                })
        except Exception:
            pass

        _dist_range_too_wide = dist_range_pct >= 25
        if _audit_test(
            'WYCKOFF_DIST_RANGE',
            _dist_range_too_wide,
            'WYCKOFF Distribution: 30d range < 25%',
            'dist_range_pct >= 25',
            8720,
        ):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725', 'WYCKOFF Distribution: 30d range < 25%', locals(), 'dist_range_pct >= 25', 8725)
        if dist_range_pct < 15:
            score += 20
            signals.append(f"✅ Боковик {dist_range_pct:.1f}% у вершины")
        else:
            score += 10
            signals.append(f"⚡️ Диапазон {dist_range_pct:.1f}% у вершины")

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
            "BEARISH", 2.0, None,
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
            _wyd_sl_pct = round(abs(entry - sl) / entry * 100, 1) if entry else 0
            _wyd_rr = round(abs(tp - entry) / abs(entry - sl), 2) if abs(entry - sl) > 0 else 0
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
            )
            groq_resp = ask_groq(groq_prompt, max_tokens=120) if legacy_strategy_groq_enabled() else None
            if groq_resp:
                import json as _j, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                m = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if m:
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
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_G8926', (risk == 0 or reward / risk < 2.0), 'risk == 0 or reward / risk < 2.0', 'risk == 0 or reward / risk < 2.0', 8926):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8927', 'risk == 0 or reward / risk < 2.0', locals(), 'risk == 0 or reward / risk < 2.0', 8927)

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
        if symbol != 'BTCUSDT':
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
            "BULLISH", 2.0, None,
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
                f"- RR={rr} < 2.0\n"
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
            )
            _resp = ask_groq(_wyk_prompt, max_tokens=100) if legacy_strategy_groq_enabled() else None
            if _resp:
                import json as _j, re as _re
                _m = _re.search(r'\{[^}]+\}', _resp, _re.DOTALL)
                if _m:
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
        if _audit_test('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_G9099', (rr < 2.0), 'rr < 2.0', 'rr < 2.0', 9099):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_REACCUMULATION_R9100', 'rr < 2.0', locals(), 'rr < 2.0', 9100)
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

        # ── 1. 15m creates the FAST thesis; 1h/4h/BTC are context only. ──
        _fast_context_15m = get_confirmed_candles(get_candles(symbol, "15m", 61))
        if _audit_test('FAST_LTF_CONTEXT_DATA', (not _fast_context_15m or len(_fast_context_15m) < 30), 'FAST: enough closed 15m context', 'not _fast_context_15m or len(_fast_context_15m) < 30', 9142):
            return _audit_fail('FAST_LTF_CONTEXT_R_DATA', 'FAST: enough closed 15m context', locals(), 'not _fast_context_15m or len(_fast_context_15m) < 30', 9142)
        _fast_bull_event = get_bos_choch_event(_fast_context_15m, "BULLISH", lookback=15, max_break_age=4)
        _fast_bear_event = get_bos_choch_event(_fast_context_15m, "BEARISH", lookback=15, max_break_age=4)
        if _audit_test('FAST_LTF_CONTEXT_STRUCTURE', (bool(_fast_bull_event) == bool(_fast_bear_event)), 'FAST: one fresh 15m BOS/CHoCH direction', 'bool(_fast_bull_event) == bool(_fast_bear_event)', 9145):
            return _audit_fail('FAST_LTF_CONTEXT_R_STRUCTURE', 'FAST: one fresh 15m BOS/CHoCH direction', locals(), 'bool(_fast_bull_event) == bool(_fast_bear_event)', 9145)
        direction = "BULLISH" if _fast_bull_event else "BEARISH"
        _fast_thesis_event = _fast_bull_event or _fast_bear_event
        _fast_bos_age = max(1, len(_fast_context_15m) - int(_fast_thesis_event.get("candle_index", len(_fast_context_15m) - 1)))
        _audit_observe("bos_event", {
            "role": "FAST_THESIS", "timeframe": "15m", "age_bars": _fast_bos_age,
            "event_type": _fast_thesis_event.get("type"), "direction": _fast_thesis_event.get("direction"),
        })
        _audit_observe("bos_progress", {"structure_confirmed": True})

        # Balanced replay winner: at least one pair HTF supports the 15m thesis.
        direction_4h = smc_on_tf(symbol, "4h")
        direction_1h = smc_on_tf(symbol, "1h")
        _fast_htf_support = direction_1h == direction or direction_4h == direction
        if _audit_test('FAST_HTF_SUPPORT', (not _fast_htf_support), 'FAST: 1h or 4h supports 15m direction', 'not _fast_htf_support', 9150):
            return _audit_fail('FAST_HTF_R_SUPPORT', 'FAST: 1h or 4h supports 15m direction', locals(), 'not _fast_htf_support', 9150)

        # BTC is a macro veto only when both BTC 1h and 4h agree against the trade.
        btc_direction_1h = smc_on_tf("BTCUSDT", "1h")
        btc_direction_4h = smc_on_tf("BTCUSDT", "4h")
        btc_trend = btc_direction_1h or btc_direction_4h or "NEUTRAL"
        if symbol != 'BTCUSDT':
            _fast_btc_hard_conflict = (
                btc_direction_1h is not None and btc_direction_4h is not None
                and btc_direction_1h == btc_direction_4h and btc_direction_1h != direction
            )
            if _audit_test('FAST_BTC_HARD_CONFLICT', _fast_btc_hard_conflict, 'FAST: BTC 1h+4h both oppose 15m thesis', '_fast_btc_hard_conflict', 9158):
                return _audit_fail('FAST_BTC_R_HARD_CONFLICT', 'FAST: BTC 1h+4h both oppose 15m thesis', locals(), '_fast_btc_hard_conflict', 9158)

        direction_1d = direction_1h or direction_4h or direction

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

        _fast_4h_zone_context = bool(in_zone)

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
        _fast_pd_context = bool(_no_middle_ok)

        # ── 4. 15m импульсная свеча (подтверждение на младшем ТФ) ──
        candles_15m_imp = get_confirmed_candles(get_candles(symbol, "15m", 21))
        if _audit_test('FAST_DETECT_FAST_DEAL_G9242', (not candles_15m_imp or len(candles_15m_imp) < 3), '4. 15m импульсная свеча (подтверждение на младшем ТФ)', 'not candles_15m_imp or len(candles_15m_imp) < 3', 9242):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9243', '4. 15m импульсная свеча (подтверждение на младшем ТФ)', locals(), 'not candles_15m_imp or len(candles_15m_imp) < 3', 9243)

        last_15m = candles_15m_imp[-1]

        # Preliminary impulse volume is context only. The executable trigger below
        # still requires the mandatory 1.6x volume spike on the actual trigger candle.
        _avg_vol_15m_imp = sum(c.get("volume", 0) for c in candles_15m_imp[:-1]) / max(len(candles_15m_imp) - 1, 1)
        _fast_impulse_volume_context_weak = bool(
            _avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1
        )
        _audit_test(
            'FAST_IMPULSE_VOLUME_CONTEXT',
            _fast_impulse_volume_context_weak,
            '15m preliminary impulse volume >= 1.1x average (non-blocking)',
            '_fast_impulse_volume_context_weak',
            9249,
        )

        # ── 5. 15m Engulfing + Displacement + Volume Spike ──
        candles_15m = get_confirmed_candles(get_candles(symbol, "15m", 31))
        if _audit_test('FAST_DETECT_FAST_DEAL_G9254', (not candles_15m or len(candles_15m) < 10), 'FAST: enough closed 15m trigger candles', 'not candles_15m or len(candles_15m) < 10', 9254):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9255', 'FAST: enough closed 15m trigger candles', locals(), 'not candles_15m or len(candles_15m) < 10', 9255)

        atr_15m = sum(c["high"] - c["low"] for c in candles_15m[-14:]) / 14

        # LTF location is mandatory: recent retest of a real 15m OB/FVG.
        _fast_ob_15m = find_ob(candles_15m, direction)
        _fast_fvg_15m = find_fvg(candles_15m, direction)
        _fast_zone_15m = _fast_ob_15m or _fast_fvg_15m
        if _audit_test('FAST_LTF_ZONE', (not _fast_zone_15m), 'FAST: 15m OB/FVG zone exists', 'not _fast_zone_15m', 9260):
            return _audit_fail('FAST_LTF_R_ZONE', 'FAST: 15m OB/FVG zone exists', locals(), 'not _fast_zone_15m', 9260)
        _fast_zone_bottom = float(_fast_zone_15m["bottom"])
        _fast_zone_top = float(_fast_zone_15m["top"])
        _fast_zone_tol = atr_15m * 0.20
        _fast_ltf_retest = any(
            float(c["low"]) <= _fast_zone_top + _fast_zone_tol
            and float(c["high"]) >= _fast_zone_bottom - _fast_zone_tol
            for c in candles_15m[-8:]
        )
        _audit_observe("bos_progress", {"retest_reached": True, "retest_confirmed": bool(_fast_ltf_retest)})
        if _audit_test('FAST_LTF_RETEST', (not _fast_ltf_retest), 'FAST: recent 15m OB/FVG retest', 'not _fast_ltf_retest', 9261):
            return _audit_fail('FAST_LTF_R_RETEST', 'FAST: recent 15m OB/FVG retest', locals(), 'not _fast_ltf_retest', 9261)
        _fast_ltf_zone_type = "OB" if _fast_ob_15m else "FVG"
        zone_desc = f"15m {_fast_ltf_zone_type} ${_fast_zone_bottom:.4f}–${_fast_zone_top:.4f}"

        engulfing_found = False
        entry = None
        sl = None
        _fast_telem_displacement_seen = False
        _fast_telem_engulfing_seen = False
        _fast_telem_volume_confirmed = False

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
            _fast_telem_displacement_seen = True

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

            _fast_telem_engulfing_seen = True
            # Для FAST нужен заметный институциональный объём.
            _vol_threshold = 1.6
            avg_vol_15m = sum(c["volume"] for c in candles_15m[-20:-1]) / 19
            if avg_vol_15m > 0 and curr["volume"] < avg_vol_15m * _vol_threshold:
                continue

            _fast_telem_volume_confirmed = True
            engulfing_found = True
            _sweep_candles_ago = i
            break

        _audit_observe("bos_progress", {
            "displacement_reached": True, "displacement_confirmed": bool(_fast_telem_displacement_seen),
            "volume_reached": bool(_fast_telem_engulfing_seen), "volume_confirmed": bool(_fast_telem_volume_confirmed),
        })
        _audit_observe("fast_trigger", {
            "displacement_seen": bool(_fast_telem_displacement_seen),
            "engulfing_seen": bool(_fast_telem_engulfing_seen),
            "volume_1_6_confirmed": bool(_fast_telem_volume_confirmed),
        })
        if _audit_test('FAST_DETECT_FAST_DEAL_G9303', (not engulfing_found or entry is None), 'FAST: displacement + engulfing + volume >= 1.6x', 'not engulfing_found or entry is None', 9303):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9304', 'FAST: displacement + engulfing + volume >= 1.6x', locals(), 'not engulfing_found or entry is None', 9304)

        # The 4h zone is context only. Execution acceptance is the confirmed
        # 15m OB/FVG retest plus the displacement/engulfing trigger above.
        _eng_idx = _sweep_candles_ago if '_sweep_candles_ago' in dir() else 1
        _eng_candle = candles_15m[-_eng_idx] if _eng_idx < len(candles_15m) else candles_15m[-1]
        _acceptance = bool(_fast_ltf_retest and engulfing_found)

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
        _fast_exec_bos_age = max(1, len(candles_15m) - int(_fast_structure_event.get("candle_index", len(candles_15m) - 1)))
        _audit_observe("bos_execution_event", {
            "role": "FAST_EXECUTION", "timeframe": "15m", "age_bars": _fast_exec_bos_age,
            "event_type": _fast_structure_event.get("type"), "direction": _fast_structure_event.get("direction"),
        })

        # ── TP = confirmed 15m swing liquidity ──
        _fast_highs, _fast_lows = find_swings(candles_15m, lookback=3)
        if direction == "BULLISH":
            _fast_targets = sorted({level for _, level in _fast_highs if level > entry * 1.001})
        else:
            _fast_targets = sorted(
                {level for _, level in _fast_lows if level < entry * 0.999}, reverse=True
            )
        _audit_observe("fast_target_geometry", {
            "swing_high_count": len(_fast_highs),
            "swing_low_count": len(_fast_lows),
            "targets_ahead_count": len(_fast_targets),
            "entry": entry,
            "direction": direction,
            "reason": "confirmed_15m_swing_ahead" if _fast_targets else "no_confirmed_15m_swing_ahead",
        })
        if _audit_test('FAST_DETECT_FAST_DEAL_G9344', (not _fast_targets), 'FAST: confirmed 15m swing target ahead of entry', 'not _fast_targets', 9344):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9345', 'FAST: confirmed 15m swing target ahead of entry', locals(), 'not _fast_targets', 9345)
        # RR is defined from TP1 by the central integrity/evidence pipeline.
        # Therefore FAST must make TP1 the nearest *real structural* swing target
        # that itself satisfies the universal RR >= 2.0 floor.  We never invent
        # or stretch a target: every eligible target comes from _fast_targets.
        # Closer confirmed swings below 2R remain observable intermediate
        # liquidity, but they are not mislabeled as the trade's TP1.
        risk = abs(entry - sl)
        if _audit_test('FAST_DETECT_FAST_DEAL_G9353', (risk == 0), 'RR проверка', 'risk == 0', 9353):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9354', 'RR проверка', locals(), 'risk == 0', 9354)

        _fast_target_prices = []
        for _fast_target_raw in _fast_targets:
            _fast_target_price = smart_round(_fast_target_raw)
            if _fast_target_price not in _fast_target_prices:
                _fast_target_prices.append(_fast_target_price)
        _fast_target_geometry = [
            {"price": target, "rr": round(abs(target - entry) / risk, 4)}
            for target in _fast_target_prices
        ]
        _fast_qualifying_targets = [
            item for item in _fast_target_geometry if item["rr"] >= 2.0
        ]
        _fast_intermediate_targets = [
            item for item in _fast_target_geometry if item["rr"] < 2.0
        ]

        # Preserve the existing rr<2 blocker when there is no qualifying
        # structural target.  This keeps NEAR-like 1.06R setups rejected rather
        # than manufacturing a synthetic 2R take-profit after the fact.
        _fast_selected_targets = _fast_qualifying_targets or _fast_target_geometry[:1]
        tp1 = _fast_selected_targets[0]["price"]
        tp2 = _fast_qualifying_targets[1]["price"] if len(_fast_qualifying_targets) > 1 else tp1
        tp = tp1

        reward = abs(tp1 - entry)
        rr = round(reward / risk, 2)
        _audit_observe("fast_rr_geometry", {
            "target_count": len(_fast_target_geometry),
            "nearest_structural_target": _fast_target_geometry[0]["price"] if _fast_target_geometry else None,
            "nearest_structural_rr": _fast_target_geometry[0]["rr"] if _fast_target_geometry else None,
            "best_structural_rr": max((item["rr"] for item in _fast_target_geometry), default=None),
            "qualifying_target_count": len(_fast_qualifying_targets),
            "intermediate_target_count": len(_fast_intermediate_targets),
            "selected_tp1": tp1,
            "selected_tp1_rr": rr,
            "targets": _fast_target_geometry[:8],
        })
        _audit_observe("bos_progress", {"rr_reached": True, "rr_passed": bool(rr >= 2.0)})
        if _audit_test('FAST_DETECT_FAST_DEAL_G9356', (rr < 2.0), 'rr < 2.0', 'rr < 2.0', 9356):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9357', 'rr < 2.0', locals(), 'rr < 2.0', 9357)

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
            _fast_sl_pct = round(abs(entry - sl) / entry * 100, 2) if entry > 0 else 0
            groq_prompt = (
                "Ты Kill Zone скальпер — торгуешь только в подтверждённой London/NY сессии.\n"
                'Отвечай СТРОГО JSON: {"logic": "макс 10 слов", "valid": true/false}\n\n'
                "КАК ДУМАТЬ:\n"
                "1. 15m engulfing + displacement — тело > 65% range, поглощение предыдущей свечи\n"
                "2. 15m OB/FVG retest задаёт реальную LTF зону входа\n"
                "3. Volume spike 1.6x+ — реальный интерес на trigger-свече\n"
                "4. 4h/1h используются как контекст; хотя бы один HTF поддерживает 15m\n"
                "5. Закрытая 15m свеча подтверждает настоящий BOS/CHoCH\n"
                "6. 1h/4h подтверждают контекст; BTC блокирует только при согласованном 1h+4h конфликте\n\n"
                "БЛОКИРУЙ если:\n"
                f"- RR={rr} < 2.0\n"
                f"- Стоп {_fast_sl_pct}% > 1.5% от входа (скальп = узкий стоп)\n"
                "- Нет свежего 15m OB/FVG retest — вход без LTF локации\n"
                "- Нет свежего подтверждённого BOS/CHoCH на закрытой 15m свече\n"
                "- Ни 1h, ни 4h не поддерживает 15m направление\n"
                "- BTC 1h и 4h одновременно направлены против сделки\n"
                "- Вне переданной приложением London/NY Kill Zone\n"
                "- SL выставлен математически (entry ± X%), а не за структуру\n\n"
                "ПОДТВЕРЖДАЙ если:\n"
                "- Engulfing/displacement чёткий с объёмом 1.6x+\n"
                "- Есть свежий BOS/CHoCH в направлении сделки\n"
                "- Есть свежий 15m OB/FVG retest\n"
                f"- RR={rr} >= 2.0\n"
                "- 1h или 4h поддерживает 15m, без двойного BTC-конфликта\n"
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
            )
            groq_resp = ask_groq(groq_prompt, max_tokens=80) if legacy_strategy_groq_enabled() else None
            if groq_resp:
                import json as _j, re as _re
                clean = groq_resp.strip().replace("```json", "").replace("```", "").strip()
                m = _re.search(r'\{[^}]+\}', clean, _re.DOTALL)
                if m:
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
            "ob":        _fast_ob_15m,
            "fvg":       _fast_fvg_15m,
            "htf_ob":    ob_4h,
            "htf_fvg":   fvg_4h,
            "htf_1h":    direction_1h,
            "htf_4h":    direction_4h,
            "btc_1h":    btc_direction_1h,
            "btc_4h":    btc_direction_4h,
            "fast_score": 0,
            "scan_type": "fast",
            "structure_event": _fast_structure_event,
        }

    except Exception as e:
        logging.debug(f"detect_fast_deal {symbol}: {e}")
        return _audit_fail('FAST_DETECT_FAST_DEAL_R9488', 'detector returned None', locals(), '', 9488)


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

        _size_mult = 1.0  # Position sizing belongs exclusively to Risk Engine.

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
