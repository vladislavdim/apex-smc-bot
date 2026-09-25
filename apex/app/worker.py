"""Transitional APEX V3 worker composition root.

The repository-level ``bot.py`` is now launcher-only. Runtime ownership is
being split from this composition root into ``apex.app`` and domain packages.
"""
# APEX_STRATEGY_STATS_V1
from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail, audit_observe as _audit_observe
import asyncio
import functools
import logging
logging.getLogger("asyncio").setLevel(logging.CRITICAL)
logging.getLogger("aiohttp").setLevel(logging.CRITICAL)
import os
import sqlite3
import threading
import time
import json
import hashlib
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

from groq import Groq
from aiogram import Bot, Dispatcher, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ChatMemberUpdated
from apex.telemetry.scanner_metrics import (
    ensure_control_schema as _ensure_control_schema,
    begin_scan as _begin_scan_control,
    finish_scan as _finish_scan_control,
    record_scan_event as _record_scan_event,
    scan_heartbeat as _scan_heartbeat,
    set_scan_scope as _set_scan_scope,
    set_scan_round as _set_scan_round,
    mark_scan_skipped as _mark_scan_skipped,
    finish_latest_running as _finish_latest_running,
    take_strategy_round_batch as _take_strategy_round_batch,
    upsert_ltf_watch as _upsert_ltf_watch,
    due_ltf_watches as _due_ltf_watches,
    touch_ltf_watch as _touch_ltf_watch,
    rebuild_strategy_risk_states as _rebuild_strategy_risk_states,
    strategy_risk_state as _strategy_risk_state,
    scanner_dashboard as _scanner_dashboard,
)
async def _edit_message(message: types.Message, *args, **kwargs):
    """Edit one message without mutating aiogram's global Message class."""
    try:
        return await message.edit_text(*args, **kwargs)
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return None
        raise


async def _edit_message_markup(message: types.Message, *args, **kwargs):
    """Edit one reply markup while preserving unrelated Telegram errors."""
    try:
        return await message.edit_reply_markup(*args, **kwargs)
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return None
        raise


from aiohttp import web

# Bounded compatibility adapters while implementations are physically migrated.
from apex.compatibility.market_transport import (
    ADMIN_ID,
    ADMIN_IDS,
    DEFAULT_UNIVERSE_SIZE,
    FAST_DEAL_THREAD_ID,
    FAST_PAIRS,
    SIGNAL_CHANNEL_MAIN,
    SIGNAL_CHANNEL_SWING,
    SWING_THREAD_ID,
    SYMBOL_ALIASES,
    TF_CATEGORIES,
    TF_LABELS,
    bot,
    dp,
    init_db,
    run_server,
    start_db_writer,
)
from apex.compatibility.market_user_services import (
    _GROQ_DAILY_LIMIT,
    _tokens_available,
    analyze_trade_type,
    ask_ai,
    ask_groq,
    calc_risk,
    detect_accumulation,
    extract_and_save_profile,
    format_accumulation,
    format_news,
    get_crypto_news,
    get_market_impact_news,
    get_user_memory,
    groq_tokens_used,
    live_position_analysis,
    save_chat_log,
    save_news,
    update_user_memory,
)
from apex.compatibility.market_data import (
    calculate_vwap, ema_value, fetch_candles_batch, find_fvg, find_ob,
    find_swings, get_adaptive_params, get_all_market_pairs,
    get_bos_choch_event, get_candles, get_confirmed_candles, get_dxy_signal,
    get_estimated_time, get_fear_greed, get_funding_rate, get_liquidity_heatmap,
    get_live_prices, get_market_regime, get_orderbook, clear_market_runtime_caches,
    get_precomputed_indicators, get_top_pairs, get_upcoming_events,
    multi_tf_analysis, smart_price_fmt, smc_on_tf, update_global_candles,
)
from apex.compatibility.market_strategy import (
    calc_smart_levels, check_alerts, check_entry_timing, check_pending_signals,
    check_session_liquidity, detect_breaker_block, detect_fast_deal,
    detect_market_regime_v2, detect_mm_accumulation,
    detect_rsi_macd_divergence, detect_swing_setup,
    detect_wyckoff_distribution, detect_wyckoff_reaccumulation,
    detect_wyckoff_spring, detect_zone_setup,
    legacy_strategy_groq_enabled, register_raw_scan_handler, save_signal_db,
)
from apex.strategies.common import fast_session
from core.trade_views import fetch_trades as _fetch_trade_view_rows
from core.trade_views import format_trade_view as _format_trade_view
from core.trade_manager_telegram import (
    configure_manager_dashboard_state as _configure_manager_dashboard_state,
    fetch_manager_trades as _fetch_manager_trades,
    fetch_manager_trade as _fetch_manager_trade,
    format_manager_dashboard as _format_manager_dashboard,
    format_manager_trade_detail as _format_manager_trade_detail,
    format_final_trade_card as _format_final_trade_card,
    manager_trade_buttons as _manager_trade_buttons,
)
from apex.manager.engine import (
    register_pending_signals as _register_pending_manager_signals,
    manager_cycle as _trade_manager_cycle,
    load_active_states as _load_active_manager_states,
    reconcile_manager_states_from_signals as _reconcile_manager_states_from_signals,
    load_manager_message as _load_manager_message,
    store_manager_message as _store_manager_message,
    configure_manager_message_state as _configure_manager_message_state,
    configure_manager_state as _configure_manager_state,
    telegram_content_hash as _telegram_content_hash,
)
from core.apex_v2 import (
    ensure_apex_v2_schema as _ensure_apex_v2_schema,
    emit_dashboard_snapshot as _emit_apex_v2_dashboard_snapshot,
    portfolio_risk_snapshot as _apex_portfolio_risk_snapshot,
    store_portfolio_snapshot as _store_apex_portfolio_snapshot,
    store_market_state as _store_apex_market_state,
)
from core.setup_audit import emit_event as _emit_stats_event
from core.strategy_decisions import record_strategy_decision as _record_strategy_decision
from core.strategy_decisions import configure_strategy_decision_state as _configure_strategy_decision_state
from core.setup_evidence import (
    assess_candidate as _assess_setup_candidate,
    ensure_setup_evidence_schema as _ensure_setup_evidence_schema,
    persist_assessment as _persist_setup_assessment,
    setup_evidence_dashboard as _setup_evidence_dashboard,
    bind_assessment_to_signal as _bind_setup_assessment_to_signal,
)
from external_sources.aggregator import collect_external_context as _collect_external_context
from core.telegram_dashboard import (
    fetch_strategy_stats as _fetch_strategy_stats,
    fetch_system_health as _fetch_system_health,
    fetch_watchlist as _fetch_watchlist,
    fetch_groq_rejections as _fetch_groq_rejections,
    format_strategy_stats as _format_strategy_stats,
    format_watchlist as _format_watchlist,
    format_groq_rejections as _format_groq_rejections,
    format_scanner_dashboard as _format_scanner_dashboard,
    format_setup_evidence_dashboard as _format_setup_evidence_dashboard,
    configure_dashboard_state as _configure_dashboard_state,
)

# Финальная проверка внешнего рыночного контекста. Она вызывается только после
# того, как стратегия уже рассчитала готовый кандидат, и не меняет его уровни.
try:
    from core.signal_quality_gate import review_signal_candidate as _review_signal_candidate
    _SIGNAL_QUALITY_GATE_OK = True
except Exception as _quality_gate_import_error:
    _SIGNAL_QUALITY_GATE_OK = False
    logging.warning(f"Signal quality gate недоступен: {_quality_gate_import_error}")

try:
    from core.market_intelligence import (refresh_market_intelligence as _refresh_market_intelligence,
        start_market_intelligence as _start_market_intelligence, stop_market_intelligence as _stop_market_intelligence)
    _MARKET_INTELLIGENCE_OK = True
except Exception as _market_intelligence_import_error:
    _refresh_market_intelligence=None;_start_market_intelligence=None;_stop_market_intelligence=None;_MARKET_INTELLIGENCE_OK=False
    logging.warning("Market intelligence unavailable: %s", _market_intelligence_import_error)

try:
    from core.signal_integrity import validate_candidate as _validate_signal_candidate
    _SIGNAL_INTEGRITY_OK = True
except Exception as _signal_integrity_import_error:
    _SIGNAL_INTEGRITY_OK = False
    logging.error(f"Signal integrity validator недоступен: {_signal_integrity_import_error}")

try:
    from apex.execution.orders import (
        BinanceFuturesClient as _BinanceFuturesClient,
        ExecutionConfig as _ExecutionConfig,
        execute_approved_candidate as _execute_approved_candidate,
        execution_status as _execution_status,
        reconcile_live_executions as _reconcile_live_executions,
        cached_execution_snapshot as _cached_execution_snapshot,
        configure_execution_state as _configure_execution_state,
        execute_manager_review as _execute_manager_review,
    )
    _TRADE_EXECUTION_OK = True
except Exception as _trade_execution_import_error:
    _TRADE_EXECUTION_OK = False
    logging.error("Optional trade execution unavailable: %s", _trade_execution_import_error)

# Render filesystem is ephemeral.  The dedicated backup branch is the durable
# source of truth; main must never provide a competing, stale brain.db.
from apex.db.backup import BrainPersistence as _BrainPersistence
from apex.app.runtime import runtime_supervisor as _V3_RUNTIME
from apex.app.bootstrap import ProductionDependencies as _V3_PRODUCTION_DEPENDENCIES, run_production as _v3_run_production
from apex.app.cutover import (
    CutoverSpec as _V3_CUTOVER_SPEC,
    refresh_cutover as _v3_refresh_cutover,
    sync_cutover as _v3_sync_cutover,
)
from apex.app.scheduler import SchedulerCallbacks as _V3SchedulerCallbacks, build_production_scheduler as _v3_build_production_scheduler
from apex.config.settings import ApexConfig as _V3_ApexConfig
from apex.config.validation import validate_config as _v3_validate_config
from apex.db.connection import (
    connect_compatibility as _v3_connect_compatibility,
    connect_memory as _v3_connect_memory,
    connect_state as _v3_connect_state,
)
from apex.ui.telegram.learning import format_live_learning as _format_live_learning
from apex.ui.telegram.system import format_system_status as _format_system_status
from apex.ui.telegram.incidents import format_incidents as _format_incidents
from apex.ui.telegram.router import (
    TelegramHandlers as _V3TelegramHandlers,
    register_telegram_handlers as _v3_register_telegram_handlers,
)
from apex.ui.telegram.commands import (
    CommandDependencies as _V3CommandDependencies,
    CompatibilityCommandDependencies as _V3CompatibilityCommandDependencies,
    CompatibilityCommandHandlers as _V3CompatibilityCommandHandlers,
    MarketCommandDependencies as _V3MarketCommandDependencies,
    MarketCommandHandlers as _V3MarketCommandHandlers,
    TelegramCommandHandlers as _V3TelegramCommandHandlers,
)
from apex.ui.telegram.chat import (
    ChatDependencies as _V3ChatDependencies,
    TelegramChatHandlers as _V3TelegramChatHandlers,
)
from apex.ui.telegram.callbacks import (
    StateCallbackDependencies as _V3StateCallbackDependencies,
    StateCallbackHandlers as _V3StateCallbackHandlers,
)
from apex.ui.telegram.market_callbacks import (
    MarketNavigationCallbacks as _V3MarketNavigationCallbacks,
    MarketNavigationDependencies as _V3MarketNavigationDependencies,
)
from apex.db.memory_db import migrate_memory as _v3_migrate_memory
from apex.db.state_db import migrate_state as _v3_migrate_state
from apex.db.ownership import assert_schema_ownership as _v3_assert_schema_ownership
from apex.db.manager_migration import (
    import_legacy_manager as _v3_import_legacy_manager,
    manager_parity_report as _v3_manager_parity_report,
)
from apex.db.execution_migration import (
    import_legacy_executions as _v3_import_legacy_executions,
    execution_parity_report as _v3_execution_parity_report,
)
from apex.db.execution_ledger_migration import (
    import_legacy_execution_ledger as _v3_import_legacy_execution_ledger,
    execution_ledger_parity_report as _v3_execution_ledger_parity_report,
)
from apex.db.signal_lifecycle_migration import (
    import_legacy_signal_lifecycle as _v3_import_legacy_signal_lifecycle,
    signal_lifecycle_parity_report as _v3_signal_lifecycle_parity_report,
)
from apex.db.repositories.runtime import RuntimeRepository as _V3RuntimeRepository
from apex.db.repositories.manager import ManagerRepository as _V3ManagerRepository
from apex.db.maintenance import maintain_memory as _v3_maintain_memory, maintain_state as _v3_maintain_state
from apex.domain.enums import ComponentState as _V3_COMPONENT_STATE, RuntimeStatus as _V3_RUNTIME_STATUS, Strategy as _V3_STRATEGY
from apex.learning.live_bridge import LiveLearningBridge as _V3LiveLearningBridge
from apex.execution.ledger import configure_execution_ledger_state as _configure_execution_ledger_state
from apex.strategies.base import trace_payload as _v3_strategy_trace_payload
from apex.strategies.activation import (
    SnapshotEvaluationBlocked as _V3_SNAPSHOT_EVALUATION_BLOCKED,
    StrategyActivationSwitch as _V3_STRATEGY_ACTIVATION_SWITCH,
)
from apex.strategies.fast import FastStrategy as _V3_FAST_STRATEGY
from apex.strategies.legacy_bridge import snapshot_symbol_detector as _v3_snapshot_symbol_detector
from apex.strategies.mtf import MtfStrategy as _V3_MTF_STRATEGY
from apex.strategies.registry import StrategyRegistry as _V3_STRATEGY_REGISTRY_CLASS
from apex.strategies.swing import SwingStrategy as _V3_SWING_STRATEGY
from apex.strategies.wyckoff import WyckoffStrategy as _V3_WYCKOFF_STRATEGY
from apex.strategies.zone import ZoneStrategy as _V3_ZONE_STRATEGY
from apex.market.gate_client import GateMarketClient as _V3_GATE_MARKET_CLIENT
from apex.market.provider import GateSnapshotProvider as _V3_GATE_SNAPSHOT_PROVIDER
from apex.ops.resource_guard import (
    memory_snapshot as _v3_memory_snapshot,
    release_unused_memory as _v3_release_unused_memory,
)
from apex.ops.restart_guard import record_shutdown as _v3_record_shutdown, record_start as _v3_record_start
from apex.ops.watchdog import EventLoopLagMonitor as _V3EventLoopLagMonitor, ProcessCpuMonitor as _V3ProcessCpuMonitor
from apex.ops.instance_fencing import InstanceLeaseClient as _V3InstanceLeaseClient, derive_lease_url as _v3_derive_lease_url
from apex.ops.release_manifest import build_release_manifest as _v3_build_release_manifest, persist_release_manifest as _v3_persist_release_manifest
from apex.telemetry.incidents import (
    configure_incidents as _v3_configure_incidents,
    current_incidents as _v3_current_incidents,
    mark_notification_delivered as _v3_mark_incident_delivered,
    pending_notifications as _v3_pending_incident_notifications,
    recover_incident as _v3_recover_incident,
    report_incident as _v3_report_incident,
)
from apex.telemetry.job_metrics import configure_job_metrics as _v3_configure_job_metrics
_V3_CONFIG = _V3_ApexConfig.from_env()
DB_PATH = _V3_CONFIG.database.compatibility_db_path
_V3_MANAGER_CUTOVER = _V3_CUTOVER_SPEC(
    label="Manager", inhibit_code="STATE_DB_MANAGER_MIRROR_FAILED",
    parity_error="manager_state_parity_failed",
    importer=_v3_import_legacy_manager, parity_report=_v3_manager_parity_report,
    parity_counts=("positions", "events"),
)
_V3_EXECUTION_CUTOVER = _V3_CUTOVER_SPEC(
    label="Execution", inhibit_code="STATE_DB_EXECUTION_MIRROR_FAILED",
    parity_error="execution_state_parity_failed",
    importer=_v3_import_legacy_executions, parity_report=_v3_execution_parity_report,
    parity_counts=("executions", "actions"),
)
_V3_LEDGER_CUTOVER = _V3_CUTOVER_SPEC(
    label="Execution ledger", inhibit_code="STATE_DB_EXECUTION_LEDGER_FAILED",
    parity_error="execution_ledger_parity_failed",
    importer=_v3_import_legacy_execution_ledger,
    parity_report=_v3_execution_ledger_parity_report,
)
_V3_LIFECYCLE_CUTOVER = _V3_CUTOVER_SPEC(
    label="Signal lifecycle", inhibit_code="STATE_DB_SIGNAL_LIFECYCLE_FAILED",
    parity_error="signal_lifecycle_parity_failed",
    importer=_v3_import_legacy_signal_lifecycle,
    parity_report=_v3_signal_lifecycle_parity_report,
    parity_counts=("signals",),
)
if _TRADE_EXECUTION_OK:
    _configure_execution_state(lambda: _v3_connect_state(_V3_CONFIG))
_configure_execution_ledger_state(lambda: _v3_connect_state(_V3_CONFIG))
_configure_manager_message_state(lambda: _v3_connect_state(_V3_CONFIG))
_configure_manager_state(lambda: _v3_connect_state(_V3_CONFIG))
_configure_strategy_decision_state(lambda: _v3_connect_state(_V3_CONFIG))
_configure_dashboard_state(lambda: _v3_connect_state(_V3_CONFIG))
_configure_manager_dashboard_state(lambda: _v3_connect_state(_V3_CONFIG))
_v3_configure_incidents(lambda: _v3_connect_state(_V3_CONFIG))
_v3_configure_job_metrics(lambda: _v3_connect_state(_V3_CONFIG))
_V3_LIVE_BRIDGE = _V3LiveLearningBridge(
    _V3_CONFIG,
    compatibility_db_path=DB_PATH,
    state_factory=lambda: _v3_connect_state(_V3_CONFIG),
    memory_factory=lambda: _v3_connect_memory(_V3_CONFIG),
)
_V3_CPU_MONITOR = _V3ProcessCpuMonitor()
_V3_LAG_MONITOR = _V3EventLoopLagMonitor(sla_ms=_V3_CONFIG.operational.event_loop_lag_sla_ms)
_V3_LEASE_CLIENT = None
_BRAIN_PERSISTENCE = _BrainPersistence(
    DB_PATH,
    _V3_CONFIG.integrations.github_repo,
    _V3_CONFIG.integrations.github_token,
    _V3_CONFIG.integrations.backup_branch,
    compression="gzip",
)
_STATE_PERSISTENCE = _BrainPersistence(
    _V3_CONFIG.database.state_db_path,
    _V3_CONFIG.integrations.github_repo,
    _V3_CONFIG.integrations.github_token,
    _V3_CONFIG.integrations.state_backup_branch,
    remote_name="apex_state.db",
    compression="gzip",
)
_MEMORY_PERSISTENCE = _BrainPersistence(
    _V3_CONFIG.database.memory_db_path,
    _V3_CONFIG.integrations.github_repo,
    _V3_CONFIG.integrations.github_token,
    _V3_CONFIG.integrations.memory_backup_branch,
    remote_name="apex_memory.db",
)
_brain_backup_async_lock = None
_state_backup_async_lock = None
_memory_backup_async_lock = None
_v3_memory_relief_active = False


def _v3_confirmed_accounting(signal_id: int):
    """Load authoritative Binance fill accounting for Live Memory."""
    from apex.execution.ledger import ExecutionSnapshot, actual_result

    try:
        snapshot = ExecutionSnapshot.from_mapping(
            _V3_LIVE_BRIDGE.execution_accounting_snapshot(signal_id)
        )
    except Exception:
        return {"status": "UNVERIFIED_EXECUTION"}
    accounting = actual_result(DB_PATH, snapshot, authoritative_snapshot=True)
    if str(accounting.get("status") or "").upper() in {"CLOSED", "FEES_UNRESOLVED"}:
        repository = _V3ManagerRepository(lambda: _v3_connect_state(_V3_CONFIG))
        result = str(accounting.get("exit_reason") or "filled").lower()
        if accounting.get("exit_price") is not None and accounting.get("exit_time") is not None:
            repository.mark_exchange_closed(signal_id, accounting, result=result)
        if accounting.get("accounting_basis") == "confirmed_fills_after_commissions_and_funding":
            repository.close_from_accounting(signal_id, accounting, result=result)
    return accounting


def _v3_sync_live_learning():
    positions = _V3_LIVE_BRIDGE.sync_confirmed_positions()
    outcomes = _V3_LIVE_BRIDGE.sync_confirmed_outcomes(_v3_confirmed_accounting)
    return {"positions": len(positions), "outcomes": len(outcomes)}


def _v3_prepare_databases():
    """Migrate both V3 stores and persist exact production release identity."""
    state = _v3_connect_state(_V3_CONFIG)
    memory = _v3_connect_memory(_V3_CONFIG)
    try:
        state_migrations = _v3_migrate_state(state)
        memory_migrations = _v3_migrate_memory(memory)
        ownership = _v3_assert_schema_ownership(state, memory)
        manifest = _v3_build_release_manifest(_V3_CONFIG)
        if manifest.production_valid:
            _v3_persist_release_manifest(state, manifest)
        return {
            "state_migrations": state_migrations,
            "memory_migrations": memory_migrations,
            "release_sha": manifest.release_sha,
            "config_hash": manifest.config_hash,
            "strategy_config_hash": manifest.strategy_config_hash,
            "state_tables": len(ownership.state_tables),
            "memory_tables": len(ownership.memory_tables),
        }
    finally:
        state.close()
        memory.close()


def _v3_sync_manager_state():
    return _v3_sync_cutover(
        _V3_MANAGER_CUTOVER,
        lambda: _v3_connect_compatibility(DB_PATH, timeout=20, check_same_thread=False),
        lambda: _v3_connect_state(_V3_CONFIG),
    )


async def _v3_refresh_manager_state_mirror():
    return await _v3_refresh_cutover(
        _V3_MANAGER_CUTOVER, _v3_sync_manager_state,
        runtime=_V3_RUNTIME, failed_state=_V3_COMPONENT_STATE.FAILED,
        report_incident=_v3_report_incident, recover_incident=_v3_recover_incident,
    )


def _v3_sync_execution_state():
    return _v3_sync_cutover(
        _V3_EXECUTION_CUTOVER,
        lambda: _v3_connect_compatibility(DB_PATH, timeout=20, check_same_thread=False),
        lambda: _v3_connect_state(_V3_CONFIG),
    )


async def _v3_refresh_execution_state_mirror():
    return await _v3_refresh_cutover(
        _V3_EXECUTION_CUTOVER, _v3_sync_execution_state,
        runtime=_V3_RUNTIME, failed_state=_V3_COMPONENT_STATE.FAILED,
        report_incident=_v3_report_incident, recover_incident=_v3_recover_incident,
    )


def _v3_sync_execution_ledger_state():
    return _v3_sync_cutover(
        _V3_LEDGER_CUTOVER,
        lambda: _v3_connect_compatibility(DB_PATH, timeout=20, check_same_thread=False),
        lambda: _v3_connect_state(_V3_CONFIG),
    )


async def _v3_refresh_execution_ledger_mirror():
    return await _v3_refresh_cutover(
        _V3_LEDGER_CUTOVER, _v3_sync_execution_ledger_state,
        runtime=_V3_RUNTIME, failed_state=_V3_COMPONENT_STATE.FAILED,
        report_incident=_v3_report_incident, recover_incident=_v3_recover_incident,
    )


def _v3_sync_signal_lifecycle():
    return _v3_sync_cutover(
        _V3_LIFECYCLE_CUTOVER,
        lambda: _v3_connect_compatibility(DB_PATH, timeout=20, check_same_thread=False),
        lambda: _v3_connect_state(_V3_CONFIG),
    )


async def _v3_refresh_signal_lifecycle_mirror():
    return await _v3_refresh_cutover(
        _V3_LIFECYCLE_CUTOVER, _v3_sync_signal_lifecycle,
        runtime=_V3_RUNTIME, failed_state=_V3_COMPONENT_STATE.FAILED,
        report_incident=_v3_report_incident, recover_incident=_v3_recover_incident,
    )

# ── Trailing stop columns migration ──
try:
    _mig_conn = _v3_connect_compatibility(DB_PATH, timeout=30, check_same_thread=False)
    for _col, _type in [("tp1_hit", "INTEGER DEFAULT 0"),
                         ("trailing_sl", "REAL DEFAULT 0"),
                         ("best_price", "REAL DEFAULT 0")]:
        try:
            _mig_conn.execute(f"ALTER TABLE signals ADD COLUMN {_col} {_type}")
        except Exception:
            pass
    _mig_conn.commit()
    _mig_conn.close()
except Exception:
    pass

# ===== DATABASE HELPERS =====

# ===== KEYBOARDS =====

def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Сделки", callback_data="menu_trades"),
         InlineKeyboardButton(text="👀 Наблюдаемые", callback_data="menu_watchlist")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="menu_stats"),
         InlineKeyboardButton(text="📊 Рынок сейчас", callback_data="menu_market")],
        [InlineKeyboardButton(text="🛡 Система", callback_data="menu_system"),
         InlineKeyboardButton(text="📚 Live Learning", callback_data="menu_live_learning")],
        [InlineKeyboardButton(text="⚠️ Инциденты", callback_data="menu_incidents")],
        [InlineKeyboardButton(text="📡 Радар стратегий", callback_data="menu_scanners")],
        [InlineKeyboardButton(text="🧭 Качество сетапов", callback_data="menu_setup_evidence")],
        [InlineKeyboardButton(text="🛠 Менеджер сделок", callback_data="menu_trade_manager")]
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
    """Клавиатура Gate USD-M universe с пагинацией."""
    all_pairs = get_top_pairs(DEFAULT_UNIVERSE_SIZE)
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

def live_tf_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="15м — где мы?", callback_data="live_15m"),
         InlineKeyboardButton(text="1ч — где мы?",  callback_data="live_1h"),
         InlineKeyboardButton(text="4ч — где мы?",  callback_data="live_4h")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
    ])

# Хранилище состояний пользователей
user_states = {}

# ===== HANDLERS =====

async def cmd_alert(message: types.Message):
    await _v3_compatibility_commands.alert(message)


async def cmd_journal(message: types.Message):
    await _v3_compatibility_commands.journal(message)

async def cmd_news(message: types.Message):
    await _v3_market_commands.news(message)


def scan_diagnostics(symbol):
    """Explain which market conditions prevented a manual signal."""
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
            if not isinstance(regime, dict):
                regime = {"mode": str(regime) if regime else "UNKNOWN", "direction": "NONE", "confidence": 0}
            lines.append(f"🧠 Режим: {regime['mode']} (уверенность {regime['confidence']}%)")
            if regime["mode"] == "SIDEWAYS" and regime["confidence"] > 85:
                lines.append("⛔️ Заблокировано: рынок в глубоком боковике")
            lines.append(f"\n📊 Confluence набрал меньше 25 очков — сигнал слабый")

        lines.append("\n<i>Попробуй через 15-30 мин или выбери другую монету</i>")
        return "\n".join(lines)

    except Exception as e:
        return f"😴 {symbol}\n⚠️ Временная ошибка: {e}\n\n<i>Попробуй снова через минуту</i>"

# ===== CALLBACK HANDLERS =====

async def handle_callback(callback: CallbackQuery):
    try:
        await callback.answer()
    except Exception:
        pass

    if await _v3_state_callback_handlers.handle(callback):
        return
    if await _get_v3_market_navigation_callbacks().handle(callback):
        return

async def cmd_pump(message: types.Message):
    await _v3_market_commands.pump(message)

async def cmd_trade(message: types.Message):
    await _v3_market_commands.trade(message)

async def cmd_brain(message: types.Message):
    await _v3_compatibility_commands.brain(message)

async def on_new_member(event: ChatMemberUpdated):
    await _v3_chat_handlers.member(event)

async def handle_text(message: types.Message):
    await _v3_chat_handlers.text(message)

def _v3_live_analysis_markup(symbol: str, timeframe: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🔄 Обновить",
            callback_data=f"live_refresh_{symbol}_{timeframe}",
        )],
        [InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],
    ])


_v3_state_callback_handlers = _V3StateCallbackHandlers(
    _V3StateCallbackDependencies(
        edit_message=_edit_message,
        main_menu=main_menu,
        fetch_manager_trades=lambda limit: _fetch_manager_trades(DB_PATH, limit),
        fetch_manager_trade=lambda signal_id, limit: _fetch_manager_trade(
            DB_PATH, signal_id, limit
        ),
        format_manager_dashboard=_format_manager_dashboard,
        format_manager_trade_detail=_format_manager_trade_detail,
        manager_trade_buttons=_manager_trade_buttons,
        fetch_trade_rows=lambda category, limit: _fetch_trade_view_rows(
            DB_PATH, category, limit
        ),
        format_trade_view=_format_trade_view,
        fetch_watchlist=lambda limit: _fetch_watchlist(DB_PATH, limit),
        format_watchlist=_format_watchlist,
        rebuild_strategy_risk=lambda: _rebuild_strategy_risk_states(DB_PATH),
        scanner_dashboard=lambda: _scanner_dashboard(DB_PATH),
        format_scanner_dashboard=_format_scanner_dashboard,
        setup_evidence_dashboard=lambda hours, limit: _setup_evidence_dashboard(
            DB_PATH, hours, limit
        ),
        format_setup_evidence_dashboard=_format_setup_evidence_dashboard,
        fetch_groq_rejections=lambda hours, limit: _fetch_groq_rejections(
            DB_PATH, hours, limit
        ),
        format_groq_rejections=_format_groq_rejections,
        get_user_memory=get_user_memory,
        live_learning=lambda: _format_live_learning(
            lambda: _v3_connect_memory(_V3_CONFIG)
        ),
        current_incidents=_v3_current_incidents,
        format_incidents=_format_incidents,
        fetch_strategy_stats=lambda: _fetch_strategy_stats(DB_PATH),
        format_strategy_stats=_format_strategy_stats,
        system_dashboard=lambda: _format_system_status(_V3_RUNTIME.public_snapshot()),
        stats_url=_V3_CONFIG.integrations.stats_url,
        button=InlineKeyboardButton,
        markup=InlineKeyboardMarkup,
    )
)


_v3_market_navigation_callbacks = None


def _get_v3_market_navigation_callbacks():
    """Build callback dependencies lazily after scanner definitions exist."""
    global _v3_market_navigation_callbacks
    if _v3_market_navigation_callbacks is None:
        _v3_market_navigation_callbacks = _V3MarketNavigationCallbacks(
            _V3MarketNavigationDependencies(
                edit_message=_edit_message,
                edit_markup=_edit_message_markup,
                pairs_keyboard=pairs_keyboard,
                timeframe_keyboard=tf_keyboard,
                live_timeframe_keyboard=live_tf_keyboard,
                live_position_analysis=live_position_analysis,
                get_top_pairs=get_top_pairs,
                full_scan=full_scan_raw,
                scan_diagnostics=scan_diagnostics,
                get_user_memory=get_user_memory,
                calculate_risk=calc_risk,
                get_crypto_news=get_crypto_news,
                get_market_news=get_market_impact_news,
                format_news=format_news,
                ask_groq=ask_groq,
                save_news=save_news,
                detect_accumulation=detect_accumulation,
                scan_all_deals=scan_all_for_deals,
                get_fear_greed=get_fear_greed,
                get_dxy_signal=get_dxy_signal,
                get_market_regime=get_market_regime,
                get_upcoming_events=get_upcoming_events,
                get_candles=get_candles,
                universe_size=DEFAULT_UNIVERSE_SIZE,
                user_states=user_states,
                timeframe_labels=TF_LABELS,
                button=InlineKeyboardButton,
                markup=InlineKeyboardMarkup,
            )
        )
    return _v3_market_navigation_callbacks


_v3_chat_handlers = _V3TelegramChatHandlers(
    _V3ChatDependencies(
        user_states=user_states,
        timeframe_labels=TF_LABELS,
        live_position_analysis=live_position_analysis,
        live_markup=_v3_live_analysis_markup,
        save_chat_log=save_chat_log,
        ask_ai=ask_ai,
        extract_profile=extract_and_save_profile,
        ask_groq=ask_groq,
        send_message=bot.send_message,
    )
)

_v3_market_commands = _V3MarketCommandHandlers(
    _V3MarketCommandDependencies(
        get_crypto_news=get_crypto_news,
        get_market_impact_news=get_market_impact_news,
        format_news=format_news,
        ask_groq=ask_groq,
        save_news=save_news,
        detect_accumulation=detect_accumulation,
        format_accumulation=format_accumulation,
        get_top_pairs=get_top_pairs,
        analyze_trade_type=analyze_trade_type,
        symbol_aliases=SYMBOL_ALIASES,
        timeframe_categories=TF_CATEGORIES,
    )
)

_v3_compatibility_commands = _V3CompatibilityCommandHandlers(
    _V3CompatibilityCommandDependencies(
        admin_ids=frozenset(ADMIN_IDS),
        connect=lambda: _v3_connect_compatibility(
            DB_PATH, timeout=30, check_same_thread=False
        ),
        get_live_prices=get_live_prices,
        ask_groq=ask_groq,
    )
)

_v3_command_handlers = _V3TelegramCommandHandlers(
    _V3CommandDependencies(
        admin_ids=frozenset(ADMIN_IDS),
        get_user_memory=get_user_memory,
        update_user_memory=update_user_memory,
        main_menu=main_menu,
        pairs_keyboard=pairs_keyboard,
        live_stats=lambda: _format_live_learning(
            lambda: _v3_connect_memory(_V3_CONFIG)
        ),
    )
)

_v3_register_telegram_handlers(
    dp,
    _V3TelegramHandlers(
        start=_v3_command_handlers.start,
        menu=_v3_command_handlers.menu,
        scan=_v3_command_handlers.scan,
        risk=_v3_command_handlers.risk,
        setrisk=_v3_command_handlers.setrisk,
        alert=cmd_alert,
        journal=cmd_journal,
        improve=_v3_command_handlers.improve,
        stats=_v3_command_handlers.stats,
        news=cmd_news,
        pump=cmd_pump,
        trade=cmd_trade,
        brain=cmd_brain,
        callback=handle_callback,
        chat_member=on_new_member,
        text=handle_text,
    ),
    Command,
)

# ===== AUTO TASKS =====

async def deep_market_scan(limit=200):
    """
    Глубокий скан всего рынка по запросу пользователя.
    Проверяет Gate USD-M universe, ищет сигналы + накопления.
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
            if acc and acc.get("score", 0) >= 72:  # поднят порог качества
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

    parts = [f"🔍 <b>Глубокий скан</b> | {total_scanned} Gate USD-M монет\n{'━'*24}\n"]

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
                f"🎯 TP: <code>{tp1:.4f}</code>\n"
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


def _format_channel_signal(sd: dict) -> str:
    """Сообщение для канала — такой же текст как в боте"""
    text = sd.get("text", "")
    if text:
        return text

    # Fallback
    symbol    = sd.get("symbol", "???")
    direction = sd.get("direction", "")
    entry     = sd.get("entry", 0)
    tp1       = sd.get("tp1", 0)
    sl        = sd.get("sl", 0)
    tf        = sd.get("timeframe", "1h")
    score     = sd.get("confluence_score", 0)
    scan_type = sd.get("scan_type", "")

    def fmt(p):
        if not p: return "—"
        if p < 0.0001: return f"${p:.8f}"
        if p < 0.01:   return f"${p:.6f}"
        if p < 1:      return f"${p:.4f}"
        return f"${p:,.2f}"

    dir_label = "🟢LONG" if direction == "BULLISH" else "🔴SHORT"
    label     = "🔄 [SWING]" if scan_type == "swing" else "🌊 [WYCKOFF]" if scan_type == "wyckoff" else "⚡ [FAST]" if scan_type == "fast" else "📐 [MTF]"
    tf_time   = {"1h": "5-12ч", "4h": "1-3дн"}.get(tf, "1-3дн")
    risk      = "низкий" if score >= 60 else "средний"

    lines = [
        f"{label} | <b>{symbol}</b> — {dir_label}",
        f"📊 Контекст: {tf}",
        f"",
        f"🎯 TP:   <code>{fmt(tp1)}</code>",
        f"💰 Вход: <code>{fmt(entry)}</code>",
        f"🛑 Стоп: <code>{fmt(sl)}</code>",
        f"",
        f"⚡ Риск: {risk}",
        f"⏱ Горизонт: {tf_time}",
        f"",
        f"💡 Это аналитика, не совет. Торгуй осознанно",
    ]
    return "\n".join(lines)


async def _send_with_retry(chat_id, text, parse_mode="HTML", retries=3, **kwargs):
    """Отправка Telegram сообщения с retry и exponential backoff"""
    for attempt in range(retries):
        try:
            await bot.send_message(chat_id, text, parse_mode=parse_mode, **kwargs)
            return True
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
                logging.warning(f"[Telegram] Retry {attempt+1}/{retries} chat={chat_id}: {e}")
            else:
                logging.error(f"[Telegram] Не удалось отправить после {retries} попыток chat={chat_id}: {e}")
    return False


def _manager_destinations():
    destinations = [(int(admin_id), 0) for admin_id in ADMIN_IDS]
    destinations += [(int(SIGNAL_CHANNEL_MAIN), 0), (int(SIGNAL_CHANNEL_SWING), int(SWING_THREAD_ID))]
    return list(dict.fromkeys(destinations))


async def _upsert_manager_card(signal_id, text, *, is_final=False):
    """Keep exactly one durable Telegram card per trade and destination."""
    for chat_id, thread_id in _manager_destinations():
        existing = await asyncio.to_thread(
            _load_manager_message, signal_id, chat_id, thread_id, DB_PATH,
        )
        content_hash = _telegram_content_hash(text)
        if existing and (existing.get("content_hash") == content_hash or existing.get("is_final")):
            continue
        if existing:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=int(existing["message_id"]), text=text,
                    parse_mode="HTML",
                )
                await asyncio.to_thread(
                    _store_manager_message, signal_id, chat_id, int(existing["message_id"]), text,
                    thread_id=thread_id, is_final=is_final, db_path=DB_PATH,
                )
                continue
            except Exception as exc:
                if "message is not modified" in str(exc).lower():
                    continue
                logging.warning(
                    "[TradeManager] card edit failed signal=%s chat=%s: %s",
                    signal_id, chat_id, exc,
                )
        kwargs = {"message_thread_id": thread_id} if thread_id else {}
        try:
            message = await bot.send_message(chat_id, text, parse_mode="HTML", **kwargs)
            await asyncio.to_thread(
                _store_manager_message, signal_id, chat_id, int(message.message_id), text,
                thread_id=thread_id, is_final=is_final, db_path=DB_PATH,
            )
        except Exception as exc:
            logging.error(
                "[TradeManager] card send failed signal=%s chat=%s: %s",
                signal_id, chat_id, exc,
            )


def _signal_type_from_candidate(sd: dict) -> str:
    scan_type = str(sd.get("scan_type") or "").upper()
    grade = str(sd.get("grade") or sd.get("signal_type") or "MTF").upper()
    aliases = {
        "MTF": "MTF", "SWING": "SWING", "ZONE": "ZONE",
        "FAST": "FAST", "FAST_DEAL": "FAST", "WYCKOFF": "WYCKOFF",
        "MEGA": "MEGA",
    }
    return aliases.get(scan_type, aliases.get(grade, "MTF"))


def _persist_delivered_signal(sd: dict):
    """Persist only a candidate that passed review and reached Telegram."""
    if sd.get("_signal_persisted"):
        return sd.get("_signal_id")
    signal_type = _signal_type_from_candidate(sd)
    default_hours = {
        "FAST": 1, "MTF": 72, "SWING": 12, "ZONE": 12,
        "WYCKOFF": 168, "MEGA": 336,
    }
    result = save_signal_db(
        sd.get("symbol"), sd.get("direction"), signal_type,
        sd.get("entry"), sd.get("tp1", sd.get("tp")),
        sd.get("tp2", sd.get("tp1", sd.get("tp"))),
        sd.get("tp3", sd.get("tp2", sd.get("tp1", sd.get("tp")))),
        sd.get("sl"), sd.get("timeframe", "1h"),
        sd.get("estimated_hours", default_hours.get(signal_type, 72)),
        sd.get("grade", signal_type),
        confluence=sd.get("confluence_score", sd.get("score", 0)) or 0,
        regime=sd.get("regime", signal_type) or signal_type,
    )
    signal_id = result[0] if isinstance(result, tuple) else result
    if signal_id:
        sd["_signal_persisted"] = True
        sd["_signal_id"] = signal_id
        try:
            sd["_v3_candidate_id"] = _V3_LIVE_BRIDGE.register_signal(
                int(signal_id),
                groq=sd.get("_external_quality_review"),
                risk=sd.get("_strategy_risk_state"),
                snapshot_id=sd.get("_scan_run_id") or None,
            )
        except Exception as exc:
            # Learning/correlation is advisory and must never turn a delivered
            # deterministic signal into a second execution attempt.
            logging.error("[LiveMemory] candidate registration failed safely: %s", exc)
    else:
        logging.error("[SignalLifecycle] Telegram delivered but persistence failed: %s", sd.get("symbol"))
    return signal_id


def _has_pending_signal_for_symbol(symbol: str) -> bool:
    """One active thesis per pair, without imposing a weekly trade quota."""
    if not symbol:
        return False
    try:
        conn = _v3_connect_compatibility(DB_PATH, timeout=10, check_same_thread=False)
        row = conn.execute(
            "SELECT id FROM signals WHERE symbol=? AND result='pending' LIMIT 1",
            (symbol,),
        ).fetchone()
        conn.close()
        return bool(row)
    except Exception as exc:
        logging.warning("[SignalArbiter] pending-position check failed: %s", exc)
        return False


from apex.db.repositories.deliveries import (
    claim_signal_delivery as _claim_signal_delivery,
    confirm_signal_delivery as _confirm_signal_delivery,
    release_signal_delivery_claim as _release_signal_delivery_claim,
    signal_delivery_key as _signal_delivery_key,
)


async def _send_signal(sd):
    """Отправляет сигнал всем админам и в каналы"""
    logging.info(f"[_send_signal] Вызван: {sd.get('symbol')} {sd.get('direction')} {sd.get('grade')} {sd.get('timeframe')}")
    _run_id = sd.get("_scan_run_id") or _active_scan_run_id
    _strategy = _signal_type_from_candidate(sd)
    if _run_id:
        await asyncio.to_thread(
            _record_scan_event, _run_id, _strategy, sd.get("symbol", ""),
            "STRATEGY", "CANDIDATE", "TECHNICAL_SETUP", {}, DB_PATH,
        )
    risk_state = await asyncio.to_thread(_strategy_risk_state, _strategy, DB_PATH, False)
    sd["_strategy_risk_state"] = risk_state
    if risk_state.get("mode") == "PAUSED":
        reason = f"{_strategy} LIVE paused after {risk_state.get('consecutive_losses', 0)} consecutive SL"
        _record_strategy_decision(sd, "WAIT", "strategy_risk", reason, db_path=DB_PATH)
        if _run_id:
            await asyncio.to_thread(
                _record_scan_event, _run_id, _strategy, sd.get("symbol", ""),
                "RISK", "FILTERED", "STRATEGY_PAUSED", {"reason": reason}, DB_PATH,
            )
        return False
    if not _SIGNAL_INTEGRITY_OK:
        logging.error("[SignalIntegrity] validator unavailable — candidate blocked")
        _record_strategy_decision(sd, "REJECT", "integrity", "validator unavailable", db_path=DB_PATH)
        return False
    try:
        _integrity_prices = get_live_prices()
        _integrity_current = _integrity_prices.get(sd.get("symbol", ""), {}).get("price")
    except Exception:
        _integrity_current = None
    integrity = _validate_signal_candidate(sd, _integrity_current)
    if not integrity.get("valid"):
        logging.error(
            "[SignalIntegrity] %s %s blocked: %s",
            sd.get("symbol"), sd.get("direction"), integrity.get("errors"),
        )
        _record_strategy_decision(sd, "REJECT", "integrity", "; ".join(integrity.get("errors", [])), db_path=DB_PATH)
        return False
    if integrity.get("warnings"):
        logging.warning("[SignalIntegrity] %s warnings: %s", sd.get("symbol"), integrity["warnings"])
    if not sd.get("_signal_id") and _has_pending_signal_for_symbol(sd.get("symbol", "")):
        logging.info(
            "[SignalArbiter] %s blocked: an existing pending thesis already owns the pair",
            sd.get("symbol"),
        )
        _record_strategy_decision(sd, "WAIT", "arbiter", "existing pending thesis owns pair", db_path=DB_PATH)
        return False
    setup_assessment = _assess_setup_candidate(sd)
    sd["setup_assessment"] = setup_assessment
    await asyncio.to_thread(_persist_setup_assessment, sd, setup_assessment, "TECHNICAL", DB_PATH)
    if setup_assessment.get("blocking"):
        setup_state = str(setup_assessment.get("state") or "DEVELOPING")
        decision = "REJECT" if setup_state == "INVALID" else "WAIT"
        reason = str(setup_assessment.get("thesis") or f"setup evidence {setup_state}")
        _record_strategy_decision(sd, decision, "setup_evidence", reason, evidence=setup_assessment, db_path=DB_PATH)
        await asyncio.to_thread(_persist_setup_assessment, sd, setup_assessment, "FINAL", DB_PATH)
        if _run_id:
            await asyncio.to_thread(
                _record_scan_event, _run_id, _strategy, sd.get("symbol", ""),
                "SETUP_EVIDENCE", decision, setup_state, setup_assessment, DB_PATH,
            )
        logging.info("[SetupEvidence] %s %s → %s: %s", sd.get("symbol"), _strategy, setup_state, reason)
        return False
    if not _SIGNAL_QUALITY_GATE_OK:
        reason = "quality gate unavailable; final Groq confirmation required"
        logging.error("[SignalQualityGate] %s blocked: %s", sd.get("symbol"), reason)
        _record_strategy_decision(sd, "WAIT", "groq_quality_gate", reason, db_path=DB_PATH)
        if _run_id:
            await asyncio.to_thread(
                _record_scan_event, _run_id, _strategy, sd.get("symbol", ""),
                "GROQ", "GROQ_WAIT", "QUALITY_GATE_UNAVAILABLE", {"reason": reason}, DB_PATH,
            )
        return False
    if not sd.get("_external_quality_reviewed"):
        review = await _review_signal_candidate(sd, ask_groq, get_candles)
        sd["_external_quality_reviewed"] = True
        sd["_external_quality_review"] = review
        if isinstance(review.get("setup_assessment"), dict):
            sd["setup_assessment"] = review["setup_assessment"]
        decision = str(review.get("decision") or "WAIT").upper()
        min_confidence = float(risk_state.get("groq_min_confidence", 0.65) or 0.65)
        if decision == "APPROVE" and float(review.get("confidence", 0.0) or 0.0) < min_confidence:
            decision = "WAIT"
            review["decision"] = decision
            review.setdefault("reasons", []).append(
                f"strategy risk gate requires Groq confidence >= {min_confidence:.0%}"
            )
        logging.info(
            "[SignalQualityGate] %s %s → %s confidence=%.2f sources=%s reasons=%s",
            sd.get("symbol"), sd.get("direction"), decision,
            review.get("confidence", 0.0),
            review.get("context", {}).get("data_quality", {}).get("available_sources", []),
            review.get("reasons", []),
        )
        if decision in ("WAIT", "REJECT"):
            if _run_id:
                await asyncio.to_thread(
                    _record_scan_event, _run_id, _strategy, sd.get("symbol", ""),
                    "GROQ", f"GROQ_{decision}", "QUALITY_GATE",
                    {"confidence": review.get("confidence"), "reasons": review.get("reasons", [])}, DB_PATH,
                )
            _record_strategy_decision(
                sd, decision, "groq_quality_gate", "; ".join(review.get("reasons", [])),
                evidence={
                    "sources": review.get("context", {}).get("data_quality", {}),
                    "candidate": {key: sd.get(key) for key in ("entry", "sl", "tp1", "tp2")},
                }, db_path=DB_PATH,
            )
            return False
        if _run_id:
            await asyncio.to_thread(
                _record_scan_event, _run_id, _strategy, sd.get("symbol", ""),
                "GROQ", "GROQ_APPROVE", "QUALITY_GATE",
                {"confidence": review.get("confidence")}, DB_PATH,
            )
    else:
        existing_review = sd.get("_external_quality_review")
        if not isinstance(existing_review, dict):
            reason = "quality review missing; final Groq confirmation required"
            logging.error("[SignalQualityGate] %s blocked: %s", sd.get("symbol"), reason)
            _record_strategy_decision(sd, "WAIT", "groq_quality_gate", reason, db_path=DB_PATH)
            return False
        existing_decision = str(existing_review.get("decision") or "WAIT").upper()
        if existing_decision != "APPROVE":
            reason = "; ".join(existing_review.get("reasons", [])) or f"existing quality review={existing_decision}"
            _record_strategy_decision(sd, existing_decision if existing_decision in {"WAIT", "REJECT"} else "WAIT", "groq_quality_gate", reason, db_path=DB_PATH)
            return False
    setup_state = str((sd.get("setup_assessment") or {}).get("state") or "")
    if setup_state in {"VALID", "STRONG", "EXCEPTIONAL"} and sd.get("text"):
        setup_line = f"\n🧭 Класс сетапа: <b>{setup_state}</b>"
        disclaimer = "\n\n💡 Это аналитика, не совет. Торгуй осознанно"
        if setup_line not in sd["text"]:
            sd["text"] = sd["text"].replace(disclaimer, setup_line + disclaimer) if disclaimer in sd["text"] else sd["text"] + setup_line
    if not ADMIN_IDS:
        logging.error("[_send_signal] ADMIN_IDS пуст — сигнал не будет отправлен!")
        _record_strategy_decision(sd, "ERROR", "delivery", "ADMIN_IDS empty", db_path=DB_PATH)
        return False
    now_ts = time.time()
    cache_key = _signal_delivery_key(sd, _strategy)
    delivery_db_path = _V3_CONFIG.database.state_db_path
    try:
        claimed = await asyncio.to_thread(
            _claim_signal_delivery, delivery_db_path, cache_key, now_ts,
            _SIGNAL_COOLDOWN_HOURS * 3600,
        )
        _V3_RUNTIME.clear_inhibit("STATE_DB_DELIVERY_UNAVAILABLE")
        _v3_recover_incident("DELIVERY_STATE_UNAVAILABLE", "state_db")
        if not claimed:
            logging.info(f"[_send_signal] cooldown: {sd.get('symbol')} — повтор через {_SIGNAL_COOLDOWN_HOURS}ч, пропускаем")
            _record_strategy_decision(sd, "WAIT", "cooldown", "duplicate signal cooldown", db_path=DB_PATH)
            return False
    except Exception as _cde:
        logging.error("[_send_signal] State DB delivery claim failed closed: %s", _cde)
        _V3_RUNTIME.inhibit_entries("STATE_DB_DELIVERY_UNAVAILABLE")
        _v3_report_incident(
            "DELIVERY_STATE_UNAVAILABLE", "state_db", "CRITICAL",
            {"error_type": type(_cde).__name__},
        )
        return False
    _sent_signal_cache[cache_key] = now_ts
    delivered = False
    try:
        sent_destinations = set()
        for admin_id in ADMIN_IDS:
            destination = (int(admin_id), 0)
            if destination in sent_destinations:
                continue
            sent_destinations.add(destination)
            ok = await _send_with_retry(admin_id, sd["text"], parse_mode="HTML")
            if ok:
                delivered = True
                logging.info(f"[_send_signal] Отправлено admin {admin_id}: {sd.get('symbol')}")
        scan_type = str(sd.get("scan_type", "")).lower()
        if scan_type == "fast":
            destination = (int(SIGNAL_CHANNEL_SWING), int(FAST_DEAL_THREAD_ID))
            fast_ok = False
            if destination not in sent_destinations:
                sent_destinations.add(destination)
                fast_ok = await _send_with_retry(
                    SIGNAL_CHANNEL_SWING, sd["text"], parse_mode="HTML",
                    message_thread_id=FAST_DEAL_THREAD_ID,
                )
            delivered = delivered or fast_ok
            if fast_ok:
                logging.info("[_send_signal] Отправлено в FAST thread: %s", sd.get("symbol"))
        else:
            channel_text = _format_channel_signal(sd)
            destination = (int(SIGNAL_CHANNEL_MAIN), 0)
            main_ok = False
            if destination not in sent_destinations:
                sent_destinations.add(destination)
                main_ok = await _send_with_retry(SIGNAL_CHANNEL_MAIN, channel_text, parse_mode="HTML")
            delivered = delivered or main_ok
            if main_ok:
                logging.info(f"[_send_signal] Отправлено в SIGNAL_CHANNEL_MAIN ({SIGNAL_CHANNEL_MAIN}): {sd.get('symbol')}")
        if scan_type == "swing":
            channel_text = _format_channel_signal(sd)
            destination = (int(SIGNAL_CHANNEL_SWING), int(SWING_THREAD_ID))
            swing_ok = False
            if destination not in sent_destinations:
                sent_destinations.add(destination)
                swing_ok = await _send_with_retry(SIGNAL_CHANNEL_SWING, channel_text, parse_mode="HTML", message_thread_id=SWING_THREAD_ID)
            delivered = delivered or swing_ok
            if swing_ok:
                logging.info(f"[_send_signal] Отправлено в SIGNAL_CHANNEL_SWING swing thread: {sd.get('symbol')}")
    except asyncio.CancelledError:
        await asyncio.to_thread(_release_signal_delivery_claim, delivery_db_path, cache_key, now_ts)
        if _sent_signal_cache.get(cache_key) == now_ts:
            _sent_signal_cache.pop(cache_key, None)
        raise
    except Exception as ce:
        logging.error(f"[_send_signal] ОШИБКА отправки в канал: {ce}")
    if not delivered:
        await asyncio.to_thread(_release_signal_delivery_claim, delivery_db_path, cache_key, now_ts)
        if _sent_signal_cache.get(cache_key) == now_ts:
            _sent_signal_cache.pop(cache_key, None)
        logging.error(f"[_send_signal] Сигнал {sd.get('symbol')} не доставлен — cooldown не установлен")
        _record_strategy_decision(sd, "ERROR", "delivery", "Telegram delivery failed", db_path=DB_PATH)
        return False
    try:
        confirmed = await asyncio.to_thread(
            _confirm_signal_delivery, delivery_db_path, cache_key, now_ts, time.time(),
        )
        if not confirmed:
            raise RuntimeError("delivery_claim_not_current")
        _v3_recover_incident("DELIVERY_CONFIRMATION_PENDING", "telegram")
    except Exception as exc:
        # Telegram has already accepted at least one destination. Keep the
        # original claim (and therefore the cooldown) and surface reconciliation.
        logging.error("[_send_signal] delivery confirmation requires reconcile: %s", exc)
        _v3_report_incident(
            "DELIVERY_CONFIRMATION_PENDING", "telegram", "HIGH",
            {"cache_key": cache_key, "error_type": type(exc).__name__},
        )
    signal_id = await asyncio.to_thread(_persist_delivered_signal, sd)
    if signal_id:
        try: await asyncio.to_thread(_bind_setup_assessment_to_signal, sd, signal_id, DB_PATH)
        except Exception as exc: logging.warning("[SetupEvidence] bind signal %s: %s", signal_id, exc)
        try:
            await _v3_refresh_signal_lifecycle_mirror()
        except Exception as exc:
            logging.error("[APEX V3] signal lifecycle requires reconcile: %s", exc)
    if signal_id and _TRADE_EXECUTION_OK:
        execution = await asyncio.to_thread(_execute_approved_candidate, sd, signal_id, db_path=DB_PATH)
        logging.info(
            "[AutoTrading] signal=%s symbol=%s status=%s",
            signal_id, sd.get("symbol"), execution.get("status"),
        )
        try:
            await asyncio.to_thread(_V3_LIVE_BRIDGE.sync_execution, int(signal_id))
        except Exception as exc:
            logging.warning("[LiveMemory] execution correlation deferred: %s", exc)
        try:
            await _v3_refresh_execution_state_mirror()
        except Exception as exc:
            logging.error("[APEX V3] execution State mirror requires reconcile: %s", exc)
    _record_strategy_decision(sd, "ACCEPT", "delivered", "signal delivered", evidence={"signal_id": signal_id}, db_path=DB_PATH)
    if _run_id:
        await asyncio.to_thread(
            _record_scan_event, _run_id, _strategy, sd.get("symbol", ""),
            "DELIVERY", "DELIVERED", "TELEGRAM", {"signal_id": signal_id}, DB_PATH,
        )
    # Delivered signals and their cooldown are critical operational state.
    # Persist them immediately so a Render restart cannot restore a snapshot
    # from before Telegram delivery and emit the same setup again.
    await backup_db_to_github(f"signal_{_strategy.lower()}")
    return True


async def _scan_tf(timeframe: str, pairs_limit: int = 50):
    """Сканирует топ пары на одном таймфрейме, возвращает сигналы"""
    pairs = get_top_pairs(pairs_limit)
    signals = []
    logging.info(f"[_scan_tf] Начинаем скан {timeframe}, пар: {len(pairs)}")
    for symbol in pairs:
        try:
            sig_data = _v3_strategy_candidate("MTF", symbol, timeframe=timeframe, auto=True)
            if sig_data:
                sig_data["timeframe"] = timeframe
                signals.append(sig_data)
                logging.info(f"[_scan_tf] {symbol} {timeframe} → сигнал: {sig_data.get('direction')} {sig_data.get('grade')}")
            await asyncio.sleep(0.3)
        except asyncio.CancelledError:
            logging.info(f"[_scan_tf] {timeframe} прерван планировщиком")
            break
        except Exception as e:
            logging.error(f"[_scan_tf] {symbol} {timeframe} ошибка: {e}")
    logging.info(f"[_scan_tf] {timeframe} завершён: {len(signals)} сигналов из {len(pairs)} пар")
    return signals


def _is_entry_still_valid(sig_data: dict, max_drift_pct: float = 2.0) -> bool:
    """Reject malformed or materially stale entries without moving levels."""
    try:
        entry = sig_data.get("entry", 0)
        if not entry:
            return False
        prices = get_live_prices()
        symbol = sig_data.get("symbol", "")
        current = prices.get(symbol, {}).get("price", 0)
        if not current:
            return False
        integrity = _validate_signal_candidate(sig_data, current)
        if not integrity.get("valid"):
            logging.info(
                "[Актуальность] %s отклонён: %s",
                symbol, integrity.get("errors"),
            )
            return False
        drift = abs(float(current) - float(entry)) / float(entry) * 100
        if drift > max_drift_pct:
            logging.info(
                "[Актуальность] %s drift %.2f%% > %.2f%% — сигнал устарел",
                symbol, drift, max_drift_pct,
            )
            return False
        return True
    except Exception as exc:
        logging.warning("[Актуальность] validation error: %s", exc)
        return False


async def auto_scan_job():
    """Каждые 10 мин: проверка закрытых сделок"""
    logging.info("⚡ auto_scan_job ЗАПУЩЕН")
    closed = await asyncio.to_thread(check_pending_signals)
    await _v3_refresh_signal_lifecycle_mirror()
    if closed:
        await asyncio.to_thread(_rebuild_strategy_risk_states, DB_PATH)
    for c in closed:
        if c["result"] == "tp1_hit":
            # Trade Manager will fold TP1 into the existing compact card on
            # its next Gate-backed pass. Avoid a separate Telegram message.
            logging.info("[TradeManager] TP1 queued for compact card signal=%s", c.get("signal_id"))
            continue
        # Signal outcome is analytics only. Production Manager closure and its
        # final card are owned by Binance reconciliation + fill accounting;
        # an OHLC result must never become a second execution source.
        logging.info(
            "[TradeManager] signal=%s analytical result=%s awaits Binance reconciliation",
            c.get("signal_id"), c.get("result"),
        )

    # 5m и 15m убраны — используем только 1h, 4h, 1d, 1w
    pass


_auto_trade_reconcile_task = None
_trade_manager_task = None
_market_scan_lock = asyncio.Lock()
_active_market_scan = None
_active_market_scan_started = 0.0
_active_scan_run_id = None


def _auto_trade_reconcile_seconds():
    try:
        value = _V3_CONFIG.operational.execution_reconcile_seconds
    except (TypeError, ValueError):
        value = 30
    return max(15, min(value, 300))


def _scanner_strategy(name):
    return {
        "auto_scan_1h": "MTF", "auto_scan_swing": "SWING",
        "auto_zone_scan": "ZONE", "auto_fast_deal_scan": "FAST",
        "auto_wyckoff_scan": "WYCKOFF",
    }.get(name, str(name).upper())


async def _control_scan_scope(pairs, universe_size=None):
    if _active_scan_run_id:
        await asyncio.to_thread(
            _set_scan_scope, _active_scan_run_id,
            len(pairs) if universe_size is None else universe_size, len(pairs), DB_PATH,
        )


async def _control_scan_round(round_id):
    if _active_scan_run_id:
        await asyncio.to_thread(_set_scan_round, _active_scan_run_id, round_id, DB_PATH)


async def _control_scan_pair(symbol):
    if _active_scan_run_id:
        await asyncio.to_thread(_scan_heartbeat, _active_scan_run_id, symbol, DB_PATH)


async def _control_scan_outcome(symbol, outcome, reason, detail=None):
    if _active_scan_run_id:
        await asyncio.to_thread(
            _record_scan_event, _active_scan_run_id, _scanner_strategy(_active_market_scan),
            symbol, "DETECTOR", outcome, reason, detail or {}, DB_PATH,
        )


async def _run_market_scan_exclusive(name, coroutine_factory, timeout):
    """Run one heavy market-data job at a time and identify contention."""
    global _active_market_scan, _active_market_scan_started, _active_scan_run_id
    if _market_scan_lock.locked():
        elapsed = max(0.0, time.monotonic() - _active_market_scan_started)
        logging.warning(
            "[%s] market scan '%s' still running for %.1fs; cycle skipped",
            name, _active_market_scan or "unknown", elapsed,
        )
        await asyncio.to_thread(
            _mark_scan_skipped, name, _active_market_scan or "unknown", elapsed, DB_PATH
        )
        return False
    async with _market_scan_lock:
        _active_market_scan = name
        _active_market_scan_started = time.monotonic()
        _active_scan_run_id = await asyncio.to_thread(
            _begin_scan_control, _scanner_strategy(name), name, 0, 0, DB_PATH
        )
        try:
            await asyncio.wait_for(coroutine_factory(), timeout=timeout)
            await asyncio.to_thread(_finish_scan_control, _active_scan_run_id, "COMPLETED", "", DB_PATH)
            return True
        except asyncio.TimeoutError:
            await asyncio.to_thread(
                _finish_latest_running, name, "TIMEOUT", f"timeout after {timeout}s", DB_PATH
            )
            raise
        except asyncio.CancelledError:
            await asyncio.to_thread(
                _finish_latest_running, name, "CANCELLED", "process shutdown", DB_PATH
            )
            raise
        except Exception as exc:
            await asyncio.to_thread(
                _finish_latest_running, name, "ERROR", str(exc), DB_PATH
            )
            raise
        finally:
            _active_market_scan = None
            _active_market_scan_started = 0.0
            _active_scan_run_id = None


async def _run_auto_trade_reconcile_once():
    """Protect filled live entries without blocking the scheduler tick."""
    if not _TRADE_EXECUTION_OK:
        return
    try:
        outcomes = await asyncio.to_thread(_reconcile_live_executions, db_path=DB_PATH)
        for outcome in outcomes:
            logging.info(
                "[AutoTrading] reconcile signal=%s status=%s",
                outcome.get("signal_id"), outcome.get("status"),
            )
            try:
                await asyncio.to_thread(
                    _V3_LIVE_BRIDGE.sync_execution, int(outcome.get("signal_id") or 0)
                )
            except Exception as exc:
                logging.warning("[LiveMemory] reconcile correlation deferred: %s", exc)
        await _v3_refresh_execution_ledger_mirror()
        learned = await asyncio.to_thread(_V3_LIVE_BRIDGE.sync_confirmed_outcomes, _v3_confirmed_accounting)
        if learned:
            logging.info("[LiveMemory] confirmed outcomes recorded=%s", len(learned))
        await _v3_refresh_execution_state_mirror()
    except Exception as exc:
        # Exchange failures must never stop scanners or Telegram handlers.
        logging.error("[AutoTrading] reconciliation failed safely: %s", exc)


async def auto_trade_reconcile_job():
    """Start one background reconciliation; later ticks remain non-overlapping."""
    global _auto_trade_reconcile_task
    if _auto_trade_reconcile_task and not _auto_trade_reconcile_task.done():
        logging.debug("[AutoTrading] previous reconciliation still running; tick skipped")
        return
    _auto_trade_reconcile_task = asyncio.create_task(_run_auto_trade_reconcile_once())


async def _run_trade_manager_once():
    """Manage activated analytics trades using Gate data, outside the scan lock."""
    try:
        await _v3_refresh_signal_lifecycle_mirror()
        if _TRADE_EXECUTION_OK:
            await _v3_refresh_execution_state_mirror()
        await _v3_refresh_manager_state_mirror()
        await asyncio.to_thread(_reconcile_manager_states_from_signals, DB_PATH)
        await asyncio.to_thread(_register_pending_manager_signals, DB_PATH)
        await _v3_refresh_manager_state_mirror()
        manager_states = await asyncio.to_thread(_load_active_manager_states, DB_PATH)
        configured_trade_risk = _V3_CONFIG.risk.risk_pct
        actual_active_states = [
            state for state in manager_states
            if str(state.get("status") or "ACTIVE").upper() == "ACTIVE"
            and str(state.get("manager_state") or "").upper() != "CLOSED"
        ]
        portfolio_snapshot = _apex_portfolio_risk_snapshot(
            ({
                "signal_id": state.get("signal_id"), "symbol": state.get("symbol"),
                "strategy": state.get("strategy"), "direction": state.get("direction"),
                "risk_pct": configured_trade_risk,
                "protected": str(state.get("manager_state") or "").upper()
                    not in {"OPENING", "RECONCILIATION_REQUIRED"},
            } for state in actual_active_states),
            max_positions=_V3_CONFIG.risk.max_open_positions,
            max_total_risk_pct=_V3_CONFIG.risk.max_total_risk_pct,
            max_same_side_risk_pct=_V3_CONFIG.risk.max_same_side_risk_pct,
            max_daily_loss_pct=_V3_CONFIG.risk.max_daily_loss_pct,
        )
        await asyncio.to_thread(_store_apex_portfolio_snapshot, portfolio_snapshot, DB_PATH)
        context_keys = sorted({
            (
                str(state.get("symbol") or "").upper(),
                str(state.get("direction") or "").upper(),
                str(state.get("strategy") or "").upper(),
            )
            for state in manager_states if state.get("symbol")
        })
        external_results = await asyncio.gather(
            *(
                _collect_external_context(symbol, direction, strategy=strategy)
                for symbol, direction, strategy in context_keys
            ),
            return_exceptions=True,
        )
        external_by_trade = {
            key: result for key, result in zip(context_keys, external_results)
            if isinstance(result, dict)
        }
        updates = await asyncio.to_thread(
            _trade_manager_cycle,
            get_live_prices,
            get_candles,
            ask_groq,
            external_context=lambda symbol, direction, strategy: external_by_trade.get(
                (
                    str(symbol).upper(), str(direction).upper(),
                    str(strategy).upper(),
                ), {}
            ),
            execution_context=(
                (lambda signal_id: _cached_execution_snapshot(signal_id, DB_PATH))
                if _TRADE_EXECUTION_OK else None
            ),
            db_path=DB_PATH,
        )
        durable_events = False
        for update in updates:
            logging.info(
                "[TradeManager] signal=%s symbol=%s events=%s action=%s notify=%s",
                update.get("signal_id"), update.get("symbol"), update.get("events"),
                update.get("review", {}).get("action"), update.get("notify"),
            )
            durable_events = durable_events or bool(update.get("events"))
            if _TRADE_EXECUTION_OK and not update.get("degraded"):
                execution = await asyncio.to_thread(
                    _execute_manager_review, update, db_path=DB_PATH,
                )
                logging.info(
                    "[TradeManager] execution signal=%s action=%s status=%s",
                    update.get("signal_id"), execution.get("action"), execution.get("status"),
                )
                if execution.get("status") not in {"NO_EXECUTION", "LIVE_NOT_ARMED"}:
                    update["telegram"] = (
                        f"{update.get('telegram', '')}\n\n"
                        f"⚙️ Binance: <b>{execution.get('status')}</b>"
                    )[:4000]
                if execution.get("status") == "EXECUTED" and execution.get("action") == "EXIT":
                    logging.info(
                        "[TradeManager] signal=%s exit filled; final card awaits ledger accounting",
                        update.get("signal_id"),
                    )
            try:
                event_payload = {
                    "signal_id": update.get("signal_id"),
                    "events": update.get("events") or [],
                    "review": update.get("review") or {},
                    "facts": update.get("facts") or {},
                    "execution": execution if _TRADE_EXECUTION_OK and not update.get("degraded") else {},
                    "manager_version": 2,
                }
                event_seed = json.dumps(event_payload, sort_keys=True, default=str)
                event_key = "manager-v2:" + hashlib.sha256(event_seed.encode()).hexdigest()
                _emit_stats_event(
                    "manager_event", str(update.get("strategy") or ""),
                    str(update.get("symbol") or ""), event_payload, event_key=event_key,
                )
            except Exception:
                pass
            if not update.get("notify"):
                continue
            await _upsert_manager_card(
                int(update.get("signal_id") or 0), update.get("telegram", ""),
            )
        if durable_events:
            logging.info(
                "[TradeManager] %s durable event(s) committed; next scheduled/SIGTERM snapshot will persist them",
                durable_events,
            )
        if _TRADE_EXECUTION_OK:
            await _v3_refresh_execution_state_mirror()
        await _v3_refresh_signal_lifecycle_mirror()
        await _v3_refresh_manager_state_mirror()
    except asyncio.CancelledError:
        logging.info("[TradeManager] cycle stopped during process shutdown")
    except Exception as exc:
        # Management is advisory and must never stop scanners or Telegram.
        logging.warning("[TradeManager] cycle failed safely: %s", exc)


async def trade_manager_job():
    """Start one manager pass; overlapping scheduler ticks are ignored."""
    global _trade_manager_task
    if _trade_manager_task and not _trade_manager_task.done():
        logging.debug("[TradeManager] previous cycle still running; tick skipped")
        return
    _trade_manager_task = asyncio.create_task(_run_trade_manager_once())


def pick_best_signal(signals: list) -> dict | None:
    """
    Выбирает лучший сигнал из найденных по приоритету и score.
    WYCKOFF > SWING > MTF > ZONE > FAST
    """
    if not signals:
        return None

    priority_order = {
        "WYCKOFF": 5,
        "SWING":   4,
        "MTF":     3,
        "ZONE":    2,
        "FAST":    1,
    }

    valid = [s for s in signals if s and s.get("rr", 0) >= 2.0]
    if not valid:
        return None

    return sorted(
        valid,
        key=lambda x: (
            priority_order.get(x.get("grade", x.get("signal_type", "MTF")), 0),
            x.get("score", x.get("confluence_score", 0)),
            x.get("rr", 0)
        ),
        reverse=True
    )[0]


_v3_live_strategy_registry = None
_v3_strategy_activation = None
_v3_strategy_snapshot_provider = None


def _get_v3_live_strategy_registry():
    """Build the one migration registry used by manual and scheduled scans."""
    global _v3_live_strategy_registry
    if _v3_live_strategy_registry is None:
        _v3_live_strategy_registry = _V3_STRATEGY_REGISTRY_CLASS({
            _V3_STRATEGY.FAST: _V3_FAST_STRATEGY(
                detect_fast_deal,
                _v3_snapshot_symbol_detector(detect_fast_deal),
            ),
            _V3_STRATEGY.MTF: _V3_MTF_STRATEGY(
                full_scan_raw,
                _v3_snapshot_symbol_detector(full_scan_raw),
            ),
            _V3_STRATEGY.SWING: _V3_SWING_STRATEGY(
                detect_swing_setup,
                _v3_snapshot_symbol_detector(detect_swing_setup),
            ),
            _V3_STRATEGY.ZONE: _V3_ZONE_STRATEGY(
                detect_zone_setup,
                _v3_snapshot_symbol_detector(detect_zone_setup),
            ),
            _V3_STRATEGY.WYCKOFF: _V3_WYCKOFF_STRATEGY((
                detect_wyckoff_spring,
                detect_wyckoff_distribution,
                detect_wyckoff_reaccumulation,
            ), (
                _v3_snapshot_symbol_detector(detect_wyckoff_spring),
                _v3_snapshot_symbol_detector(detect_wyckoff_distribution),
                _v3_snapshot_symbol_detector(detect_wyckoff_reaccumulation),
            )),
        })
    return _v3_live_strategy_registry


def _get_v3_strategy_activation():
    global _v3_strategy_activation
    if _v3_strategy_activation is None:
        settings = _V3_CONFIG.strategies
        _v3_strategy_activation = _V3_STRATEGY_ACTIVATION_SWITCH.from_proof(
            requested=settings.snapshot_activation_requested,
            corpus_directory=settings.parity_corpus_path,
            verdict_path=settings.parity_verdict_path,
        )
        logging.info(
            "[APEX V3] strategy snapshot activation active=%s reason=%s",
            _v3_strategy_activation.active, _v3_strategy_activation.reason,
        )
    return _v3_strategy_activation


def _get_v3_strategy_snapshot_provider():
    global _v3_strategy_snapshot_provider
    if _v3_strategy_snapshot_provider is None:
        _v3_strategy_snapshot_provider = _V3_GATE_SNAPSHOT_PROVIDER(
            _V3_GATE_MARKET_CLIENT(
                base_url=_V3_CONFIG.integrations.gate_api_base,
                timeout=_V3_CONFIG.operational.gate_timeout_seconds,
            )
        )
    return _v3_strategy_snapshot_provider


def _v3_publish_strategy_activation_health():
    activation = _get_v3_strategy_activation()
    requested = _V3_CONFIG.strategies.snapshot_activation_requested
    if requested and not activation.active:
        _V3_RUNTIME.mark_component(
            "strategy_activation", _V3_COMPONENT_STATE.FAILED,
            activation.reason, required=True,
        )
        _V3_RUNTIME.inhibit_entries("SNAPSHOT_STRATEGY_PROOF_INVALID")
        _v3_report_incident(
            "SNAPSHOT_STRATEGY_PROOF_INVALID", "strategy_activation", "CRITICAL",
            {"reason": activation.reason},
        )
    else:
        detail = (
            f"snapshot READY verdict={activation.verdict_sha256[:12]}"
            if activation.active else "legacy path; snapshot activation not requested"
        )
        _V3_RUNTIME.mark_component(
            "strategy_activation", _V3_COMPONENT_STATE.READY,
            detail, required=requested,
        )
        _V3_RUNTIME.clear_inhibit("SNAPSHOT_STRATEGY_PROOF_INVALID")
        _v3_recover_incident(
            "SNAPSHOT_STRATEGY_PROOF_INVALID", "strategy_activation",
        )
    return activation


def _v3_strategy_candidate(strategy, symbol, **kwargs):
    """Return the exact legacy candidate while retaining the V3 trace."""
    candidates = _v3_strategy_candidates(strategy, symbol, **kwargs)
    return candidates[0] if candidates else None


def _v3_strategy_candidates(strategy, symbol, **kwargs):
    """Return all subtype results through the proof-gated evaluation path."""
    registry = _get_v3_live_strategy_registry()
    activation = _get_v3_strategy_activation()
    if activation.active:
        try:
            traces = activation.evaluate(
                registry, _get_v3_strategy_snapshot_provider(),
                strategy, symbol, **kwargs,
            )
        except _V3_SNAPSHOT_EVALUATION_BLOCKED as exc:
            reason = str(exc)[:300]
            _V3_RUNTIME.mark_component(
                "strategy_activation", _V3_COMPONENT_STATE.STALE,
                reason, required=True,
            )
            _V3_RUNTIME.inhibit_entries("SNAPSHOT_STRATEGY_DATA_NOT_READY")
            _v3_report_incident(
                "SNAPSHOT_STRATEGY_DATA_NOT_READY", "strategy_activation", "HIGH",
                {"strategy": str(strategy), "symbol": str(symbol), "reason": reason},
            )
            raise
        _V3_RUNTIME.mark_component(
            "strategy_activation", _V3_COMPONENT_STATE.READY,
            f"snapshot READY verdict={activation.verdict_sha256[:12]}",
            required=True,
        )
        _V3_RUNTIME.clear_inhibit("SNAPSHOT_STRATEGY_DATA_NOT_READY")
        _v3_recover_incident(
            "SNAPSHOT_STRATEGY_DATA_NOT_READY", "strategy_activation",
        )
    else:
        traces = registry.evaluate(strategy, symbol, **kwargs)
    candidates = []
    for trace in traces:
        if trace.raw_result is None:
            continue
        candidate = dict(trace.raw_result)
        candidate["_v3_strategy_trace"] = _v3_strategy_trace_payload(trace)
        candidates.append(candidate)
    return candidates


async def auto_scan_1h():
    """Раз в час после закрытия свечи: главный MTF-скан на 1h."""
    try:
        await _run_market_scan_exclusive("auto_scan_1h", _auto_scan_1h_impl, 210)
    except asyncio.TimeoutError:
        logging.warning("[auto_scan_1h] таймаут 210с — пропускаем цикл")
    except Exception as e:
        logging.error(f"[auto_scan_1h] ОШИБКА: {e}")

async def _auto_scan_1h_impl():
    logging.info("[auto_scan_1h] ЗАПУЩЕН с режимом рынка")
    universe = await asyncio.to_thread(get_top_pairs, DEFAULT_UNIVERSE_SIZE)
    batch = await asyncio.to_thread(
        _take_strategy_round_batch, "MTF", universe, (len(universe) + 2) // 3, DB_PATH
    )
    pairs = batch["pairs"]
    await _control_scan_round(batch["round_id"])
    await _control_scan_scope(pairs, batch["target"])
    all_signals = []

    for symbol in pairs:
        await _control_scan_pair(symbol)
        try:
            # Определяем режим рынка
            regime = await asyncio.to_thread(detect_market_regime_v2, symbol)
            enabled = regime.get("enabled", ["MTF"])

            # Session liquidity check
            _liq = await asyncio.to_thread(check_session_liquidity, symbol, "1h")
            if not _liq["ok"]:
                await _control_scan_outcome(symbol, "FILTERED", "LOW_LIQUIDITY")
                continue

            # MTF — если включён для этого режима
            if "MTF" in enabled:
                sig = await asyncio.to_thread(
                    _v3_strategy_candidate, "MTF", symbol,
                    timeframe="1h", auto=True, passive_watch=True,
                )
                if sig and sig.get("_pending_ltf"):
                    await asyncio.to_thread(
                        _upsert_ltf_watch, "MTF", symbol, sig.get("direction", ""),
                        sig.get("required_timeframe", "15m"), sig.get("reason", "WAIT_LTF_CONFIRMATION"),
                        4, DB_PATH,
                    )
                    await _control_scan_outcome(symbol, "FILTERED", "WAIT_LTF_CONFIRMATION", sig)
                elif sig and sig.get("confluence_score", 0) >= 35:
                    sig["grade"] = "MTF"
                    all_signals.append(sig)
                else:
                    await _control_scan_outcome(symbol, "FILTERED", "NO_STRATEGY_SETUP")
            else:
                await _control_scan_outcome(symbol, "FILTERED", "REGIME_DISABLED")

            await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            await _control_scan_outcome(symbol, "DATA_FAILED", "SCAN_ERROR", {"error": str(e)[:300]})
            logging.warning(f"[auto_scan_1h] {symbol}: {e}")

    logging.info(f"[auto_scan_1h] Скан: {len(all_signals)} сигналов из {len(pairs)} пар")

    # Фильтрация и сортировка
    valid = []
    for signal in all_signals:
        if await asyncio.to_thread(_is_entry_still_valid, signal, max_drift_pct=2.0):
            valid.append(signal)

    # Rank for delivery, but do not impose a per-scan or weekly trade quota.
    sent = 0
    valid.sort(
        key=lambda item: (item.get("confluence_score", 0), item.get("rr", 0)),
        reverse=True,
    )
    for sd in valid:
        logging.info(f"[auto_scan_1h] → _send_signal: {sd.get('symbol')} {sd.get('direction')}")
        delivered = await _send_signal(sd)
        await asyncio.sleep(1)
        sent += int(bool(delivered))
    logging.info(f"[auto_scan_1h] Завершён, отправлено {sent}")


async def auto_scan_swing():
    """Каждые 30 мин: swing сканер на 4h — торговля от экстремумов"""
    try:
        await _run_market_scan_exclusive("auto_scan_swing", _auto_scan_swing_impl, 210)
    except asyncio.TimeoutError:
        logging.warning("[auto_scan_swing] таймаут 210с — пропускаем цикл")
    except Exception as e:
        logging.error(f"[auto_scan_swing] ОШИБКА: {e}")

async def _auto_scan_swing_impl():
    universe = await asyncio.to_thread(get_top_pairs, DEFAULT_UNIVERSE_SIZE)
    batch = await asyncio.to_thread(
        _take_strategy_round_batch, "SWING", universe, (len(universe) + 1) // 2, DB_PATH
    )
    pairs = batch["pairs"]
    logging.info(
        "[auto_scan_swing] ЗАПУЩЕН batch=%s universe=%s",
        len(pairs), len(universe),
    )
    await _control_scan_round(batch["round_id"])
    await _control_scan_scope(pairs, batch["target"])
    found = []
    blocked = 0
    for symbol in pairs:
        await _control_scan_pair(symbol)
        try:
            _liq_sw = await asyncio.to_thread(check_session_liquidity, symbol, "4h")
            if not _liq_sw["ok"]:
                blocked += 1
                await _control_scan_outcome(symbol, "FILTERED", "LOW_LIQUIDITY")
                continue
            r = await asyncio.to_thread(
                _v3_strategy_candidate, "SWING", symbol, timeframe="4h",
            )
            if r:
                found.append(r)
                logging.info(f"[auto_scan_swing] {symbol} НАЙДЕН: {r.get('direction')} RR={r.get('rr')}")
            else:
                blocked += 1
                await _control_scan_outcome(symbol, "FILTERED", "NO_STRATEGY_SETUP")
            await asyncio.sleep(0.15)
        except asyncio.CancelledError:
            logging.info("[auto_scan_swing] Прерван планировщиком")
            raise
        except Exception as e:
            await _control_scan_outcome(symbol, "DATA_FAILED", "SCAN_ERROR", {"error": str(e)[:300]})
            logging.warning(f"[auto_scan_swing] {symbol}: {e}")

    if not found:
        logging.info(f"[auto_scan_swing] Swing scan 4h: сетапов нет (проверено {len(pairs)}, заблокировано фильтрами {blocked})")
        return

    # Сортируем по RR
    found.sort(key=lambda x: x["rr"], reverse=True)
    logging.info(f"[auto_scan_swing] Swing scan 4h: найдено {len(found)} сетапов")

    for r in found:
        try:
            symbol    = r["symbol"]
            direction = r["direction"]
            dir_label = "🟢LONG" if direction == "BULLISH" else "🔴SHORT"
            trend_icon = "📈" if direction == "BULLISH" else "📉"
            htf = r.get("htf_dir", "")
            htf_text = f" | 1d: {htf}" if htf else ""

            risk_label = "низкий" if r["rr"] >= 3 else "средний"

            # AI комментарий

            _sw_tp2_str = f"\n🎯 TP2:  <code>{smart_price_fmt(r['tp2'])}</code>" if r.get("tp2") else ""
            text = (
                f"🔄 <b>[SWING]</b> | <b>{symbol}</b> — {dir_label}\n"
                f"📊 Контекст: 4h{htf_text}\n"
                f"\n"
                f"🎯 TP1:  <code>{smart_price_fmt(r['tp'])}</code>{_sw_tp2_str}\n"
                f"💰 Вход: <code>{smart_price_fmt(r['entry'])}</code>\n"
                f"🛑 Стоп: <code>{smart_price_fmt(r['sl'])}</code>\n"
                f"\n"
                f"📈 Логика: {r['logic']}\n"
                f"\n"
                f"⚡ Риск: {risk_label}\n"
                f"⏱ Горизонт: ~{r.get('est_hours', 8)}ч"
            )
            text += "\n\n💡 Это аналитика, не совет. Торгуй осознанно"

            _sw_tp2_val = r.get("tp2") or r["tp"]
            sd = {
                "symbol": symbol, "direction": direction,
                "timeframe": "4h", "entry": r["entry"],
                "sl": r["sl"], "tp1": r["tp"],
                "tp2": _sw_tp2_val, "tp3": _sw_tp2_val,
                "rr": r.get("rr"),
                "grade": "SWING", "text": text,
                "confluence_score": int(r["rr"] * 20),
                "regime": "SWING",
                "scan_type": "swing",
                "technical_evidence": {key: r.get(key) for key in (
                    "logic", "htf_dir", "htf_1w", "weekly_warning", "confirms",
                    "funding_warning", "ob", "fvg", "structure_event", "structure_event_1h"
                ) if r.get(key) is not None},
            }
            sd["technical_evidence"]["causal_matrix_ready"] = True

            # Проверка актуальности цены входа
            if not await asyncio.to_thread(_is_entry_still_valid, sd, max_drift_pct=3.0):
                continue

            # Блокируем если сделка уже открыта в БД
            try:
                _chk = _v3_connect_compatibility(DB_PATH, timeout=10)
                _open = _chk.execute(
                    "SELECT id FROM signals WHERE symbol=? AND direction=? AND result='pending' LIMIT 1",
                    (symbol, direction)
                ).fetchone()
                _chk.close()
                if _open:
                    continue
            except Exception:
                pass
            delivered = await _send_signal(sd)
            if delivered:
                logging.info(f"[SwingScan] {symbol} {direction} RR={r['rr']} → отправлен")
            await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"[SwingScan] send {r.get('symbol')}: {e}")

async def auto_zone_scan():
    """Каждые 20 мин: сканирует зоны Discount/Premium с OB/FVG"""
    try:
        await _run_market_scan_exclusive("auto_zone_scan", _auto_zone_scan_impl, 210)
    except asyncio.TimeoutError:
        logging.warning("[auto_zone_scan] таймаут 210с — пропускаем цикл")
    except Exception as e:
        logging.error(f"[auto_zone_scan] ОШИБКА: {e}")


def _zone_candidate_from_setup(r):
    """Format an already validated ZONE setup without changing its levels."""
    symbol = r["symbol"]
    direction = r["direction"]
    dir_label = "🟢LONG" if direction == "BULLISH" else "🔴SHORT"
    htf = r.get("htf_dir", "")
    tp2_text = f"\n🎯 TP2:  <code>{smart_price_fmt(r['tp2'])}</code>" if r.get("tp2") else ""
    text = (
        f"📦 <b>[ZONE]</b> | <b>{symbol}</b> — {dir_label}\n"
        f"📊 Контекст: 4h | 1d: {htf} | {r['zone']} зона ({r['zone_type']})\n\n"
        f"🎯 TP1:  <code>{smart_price_fmt(r['tp'])}</code>{tp2_text}\n"
        f"💰 Вход: <code>{smart_price_fmt(r['entry'])}</code>\n"
        f"🛑 Стоп: <code>{smart_price_fmt(r['sl'])}</code>\n\n"
        f"📈 Логика: {r['logic']}\n\n"
        f"⭐ Quality: {r['q_score']}/8 | RR: {r['rr']}\n"
        f"⏱ Горизонт: ~{r.get('est_hours', 12)}ч\n\n"
        "💡 Это аналитика, не совет. Торгуй осознанно"
    )
    tp2 = r.get("tp2") or r["tp"]
    candidate = {
        "symbol": symbol, "direction": direction,
        "timeframe": "4h", "entry": r["entry"],
        "sl": r["sl"], "tp1": r["tp"], "tp2": tp2, "tp3": tp2,
        "rr": r.get("rr"), "grade": "ZONE", "text": text,
        "confluence_score": int(r["rr"] * 20), "regime": "ZONE", "scan_type": "zone",
        "technical_evidence": {key: r.get(key) for key in (
            "logic", "zone", "zone_type", "q_score", "quality_components",
            "htf_dir", "funding_warning", "structure_event"
        ) if r.get(key) is not None},
    }
    candidate["technical_evidence"]["causal_matrix_ready"] = True
    return candidate

async def _auto_zone_scan_impl():
    universe = await asyncio.to_thread(get_top_pairs, DEFAULT_UNIVERSE_SIZE)
    batch = await asyncio.to_thread(
        _take_strategy_round_batch, "ZONE", universe, (len(universe) + 2) // 3, DB_PATH
    )
    pairs = batch["pairs"]
    logging.info(
        "[auto_zone_scan] ЗАПУЩЕН batch=%s universe=%s",
        len(pairs), len(universe),
    )
    await _control_scan_round(batch["round_id"])
    await _control_scan_scope(pairs, batch["target"])
    found = []
    for symbol in pairs:
        await _control_scan_pair(symbol)
        try:
            _liq_z = await asyncio.to_thread(check_session_liquidity, symbol, "4h")
            if not _liq_z["ok"]:
                await _control_scan_outcome(symbol, "FILTERED", "LOW_LIQUIDITY")
                continue
            r = await asyncio.to_thread(
                _v3_strategy_candidate, "ZONE", symbol,
                timeframe="4h", passive_watch=True,
            )
            if r and r.get("_pending_ltf"):
                await asyncio.to_thread(
                    _upsert_ltf_watch, "ZONE", symbol, r.get("direction", ""),
                    r.get("required_timeframe", "1h"), r.get("reason", "WAIT_LTF_CONFIRMATION"),
                    8, DB_PATH,
                )
                await _control_scan_outcome(symbol, "FILTERED", "WAIT_LTF_CONFIRMATION", r)
            elif r:
                found.append(r)
                logging.info(f"[auto_zone_scan] {symbol} НАЙДЕН: {r['direction']} RR={r['rr']} zone={r['zone']}")
            else:
                await _control_scan_outcome(symbol, "FILTERED", "NO_STRATEGY_SETUP")
            await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await _control_scan_outcome(symbol, "DATA_FAILED", "SCAN_ERROR", {"error": str(e)[:300]})
            logging.warning(f"[auto_zone_scan] {symbol}: {e}")

    if not found:
        logging.info("[auto_zone_scan] Зон нет")
        return

    found.sort(key=lambda x: x["rr"], reverse=True)

    for r in found:
        try:
            symbol    = r["symbol"]
            direction = r["direction"]
            sd = _zone_candidate_from_setup(r)

            if not await asyncio.to_thread(_is_entry_still_valid, sd, max_drift_pct=2.0):
                continue

            try:
                _chk = _v3_connect_compatibility(DB_PATH, timeout=10)
                _open = _chk.execute(
                    "SELECT id FROM signals WHERE symbol=? AND direction=? AND result='pending' LIMIT 1",
                    (symbol, direction)
                ).fetchone()
                _chk.close()
                if _open:
                    continue
            except Exception:
                pass

            delivered = await _send_signal(sd)
            if delivered:
                logging.info(f"[ZoneScan] {symbol} {direction} RR={r['rr']} zone={r['zone']} → отправлен")
            await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"[ZoneScan] {r.get('symbol')}: {e}")

async def auto_scan_1d():
    """Каждый час: скан 1d таймфрейма"""
    signals = await _scan_tf("1d", pairs_limit=20)
    logging.info(f"Скан 1d: сигналов {len(signals)}")
    valid = [s for s in signals if _is_entry_still_valid(s, max_drift_pct=5.0)]
    for sd in valid:
        await _send_signal(sd)
        await asyncio.sleep(1)


async def auto_scan_1w():
    """Каждые 6 часов: скан недельного таймфрейма — долгосрочные сделки"""
    signals = await _scan_tf("1w", pairs_limit=15)
    logging.info(f"Скан 1w: сигналов {len(signals)}")
    valid = [s for s in signals if _is_entry_still_valid(s, max_drift_pct=8.0)]
    for sd in valid:
        await _send_signal(sd)
        await asyncio.sleep(1)


async def auto_scan_mega():
    """Каждые 6 часов: скан мега-сделок на 100-200% на 4h, 1d и 1w таймфреймах"""
    try:
        from smc_engine import detect_mega_trade
    except ImportError:
        logging.warning("detect_mega_trade не найден в smc_engine")
        return

    pairs = get_top_pairs(DEFAULT_UNIVERSE_SIZE)
    found = []

    for symbol in pairs:
        try:
            candles_4h = get_candles(symbol, "4h", 100)
            candles_1d = get_candles(symbol, "1d", 60)
            candles_1w = get_candles(symbol, "1w", 50)
            if len(candles_4h) < 50 or len(candles_1d) < 30:
                continue
            # Используем недельные если доступны, иначе дневные
            base_candles = candles_1w if len(candles_1w) >= 20 else candles_1d
            result = detect_mega_trade(candles_4h, base_candles, symbol)
            if result and result["score"] >= 45:
                found.append(result)
            await asyncio.sleep(0.5)
        except Exception as e:
            logging.debug(f"auto_scan_mega {symbol}: {e}")

    found.sort(key=lambda x: x["score"], reverse=True)
    logging.info(f"Мега-скан: найдено {len(found)} сигналов")

    for r in found:
        try:
            symbol    = r["symbol"]
            direction = r["direction"]
            emoji     = "🟢" if direction == "BULLISH" else "🔴"
            arrow     = "▲" if direction == "BULLISH" else "▼"

            signals_text = "\n".join(r["signals"])

            push_dir = "↑" if direction == "BULLISH" else "↓"
            text = (
                f"<b>{symbol}</b> — strong push {push_dir}\n"
                f"📊 Контекст: 4h/1d\n"
                f"\n"
                f"🎯 TP:  <code>{r['tp1']:.4f}</code>\n"
                f"💰 Вход: <code>{r['entry']:.4f}</code>\n"
                f"🛑 Стоп: <code>{r['sl']:.4f}</code>\n"
                f"\n"
                f"{'📈' if direction == 'BULLISH' else '📉'} Логика: боковик {r['days_in_range']}д →\n"
                f"давление → резкий выход {push_dir}\n"
                f"\n"
                f"⚡ Риск: низкий\n"
                f"⏱ Горизонт: 1-2 дня"
            )

            candidate = {
                "symbol": symbol,
                "direction": direction,
                "scan_type": "MEGA",
                "grade": "MEGA",
                "timeframe": "4h",
                "entry": r["entry"],
                "sl": r["sl"],
                "tp1": r["tp1"],
                "tp2": r.get("tp2", r["tp1"]),
                "tp3": r.get("tp3", r.get("tp2", r["tp1"])),
                "rr": abs(r["tp1"] - r["entry"]) / max(abs(r["entry"] - r["sl"]), 1e-12),
                "estimated_hours": 24 * 14,
                "confluence_score": r["score"],
                "regime": "MEGA",
                "text": text,
                "technical_evidence": {
                    "score": r.get("score"),
                    "days_in_range": r.get("days_in_range"),
                    "signals": r.get("signals", []),
                },
            }
            await _send_signal(candidate)
            await asyncio.sleep(2)
        except Exception as e:
            logging.error(f"auto_scan_mega send {r.get('symbol')}: {e}")



async def auto_wyckoff_scan():
    """Hourly quarter-batch; one full Wyckoff universe is covered every 4h."""
    try:
        await _run_market_scan_exclusive("auto_wyckoff_scan", _auto_wyckoff_scan_impl, 300)
    except asyncio.TimeoutError:
        logging.warning("[auto_wyckoff_scan] таймаут 300с — пропускаем цикл")
    except Exception as e:
        logging.error(f"[auto_wyckoff_scan] ОШИБКА: {e}")

async def _auto_wyckoff_scan_impl():
    logging.info("[auto_wyckoff_scan] ЗАПУЩЕН")
    universe = await asyncio.to_thread(get_top_pairs, DEFAULT_UNIVERSE_SIZE)
    batch = await asyncio.to_thread(
        _take_strategy_round_batch, "WYCKOFF", universe, max(1, (len(universe) + 3) // 4), DB_PATH
    )
    pairs = batch["pairs"]
    await _control_scan_round(batch["round_id"])
    await _control_scan_scope(pairs, batch["target"])
    found = []
    for symbol in pairs:
        await _control_scan_pair(symbol)
        try:
            # Session liquidity check
            _liq_w = await asyncio.to_thread(check_session_liquidity, symbol, "1d")
            if not _liq_w["ok"]:
                await _control_scan_outcome(symbol, "FILTERED", "LOW_LIQUIDITY")
                logging.debug(f"[WYCKOFF] {symbol}: низкая ликвидность ({_liq_w['ratio']}x) — пропускаем")
                continue
            subtype_results = await asyncio.to_thread(
                _v3_strategy_candidates, "WYCKOFF", symbol,
            )
            found.extend({**result, "scan_type": "wyckoff"} for result in subtype_results)
            if not subtype_results:
                await _control_scan_outcome(symbol, "FILTERED", "NO_STRATEGY_SETUP")
            await asyncio.sleep(0.5)
        except Exception as e:
            await _control_scan_outcome(symbol, "DATA_FAILED", "SCAN_ERROR", {"error": str(e)[:300]})
            logging.warning(f"[auto_wyckoff_scan] {symbol}: {e}")

    if not found:
        logging.info("[auto_wyckoff_scan] Wyckoff scan: паттернов нет")
        return

    found.sort(key=lambda x: x["score"], reverse=True)
    logging.info(f"[auto_wyckoff_scan] Wyckoff scan: найдено {len(found)}")

    for r in found:
        try:
            symbol    = r["symbol"]
            direction = r["direction"]
            is_long = r["direction"] == "BULLISH"
            dir_label = "🟢LONG" if is_long else "🔴SHORT"
            wyckoff_type = "Wyckoff Spring" if is_long else "Wyckoff Distribution"
            phases_txt = r.get("phases", "")

            if is_long:
                key_signal = "Spring ✅" if r.get("spring") else "SOS ✅" if r.get("sos") else "Накопление"
                trend_txt = f"📉 Даунтренд: -{r.get('drawdown_pct',0):.0f}% от пика"
                range_txt = f"📦 Боковик: {r.get('acc_range',0):.1f}%"
                tp_sign = "+"
            else:
                key_signal = "UTAD ✅" if r.get("utad") else "SOW ✅" if r.get("sow") else "Дистрибуция"
                trend_txt = f"📈 Аптренд: +{r.get('pump_pct',0):.0f}% от основания"
                range_txt = f"📦 Боковик у вершины: {r.get('dist_range',0):.1f}%"
                tp_sign = "-"

            # AI комментарий

            _w_tp2_str = f"\n🎯 TP2:  <code>{smart_price_fmt(r['tp2'])}</code>" if r.get("tp2") else ""
            text = (
                f"🌊 <b>[WYCKOFF]</b> | <b>{symbol}</b> — {dir_label}\n"
                f"📊 Контекст: 1d | {wyckoff_type}\n"
                f"\n"
                f"🎯 TP1:  <code>{smart_price_fmt(r['tp'])}</code> ({tp_sign}{r['tp_pct']}%){_w_tp2_str}\n"
                f"💰 Вход: <code>{smart_price_fmt(r['entry'])}</code>\n"
                f"🛑 Стоп: <code>{smart_price_fmt(r['sl'])}</code>\n"
                f"\n"
                f"📈 Логика: {r['logic']}\n"
                f"\n"
                f"{trend_txt}\n"
                f"{range_txt} | {key_signal}\n"
                f"🔄 Фазы: {phases_txt}\n"
                f"⭐ Скор: {r['score']}/100 | RR: {r['rr']}\n"
                f"\n"
                f"⚡ Риск: средний\n"
                f"⏱ Горизонт: ~7-21 дней"
            )
            text += "\n\n💡 Это аналитика, не совет. Торгуй осознанно"

            # Проверка актуальности цены входа
            if not await asyncio.to_thread(_is_entry_still_valid, r, max_drift_pct=5.0):
                continue

            # Блокируем если сделка уже открыта
            try:
                _chk = _v3_connect_compatibility(DB_PATH, timeout=10)
                _open = _chk.execute(
                    "SELECT id FROM signals WHERE symbol=? AND direction=? AND result=\'pending\' LIMIT 1",
                    (symbol, direction)
                ).fetchone()
                _chk.close()
                if _open:
                    continue
            except Exception:
                pass

            _w_tp2_val = r.get("tp2") or r["tp"]
            sd = {
                "symbol": symbol, "direction": direction,
                "timeframe": "1d", "entry": r["entry"],
                "sl": r["sl"], "tp1": r["tp"],
                "tp2": _w_tp2_val, "tp3": _w_tp2_val,
                "rr": r.get("rr"),
                "grade": "WYCKOFF", "text": text,
                "confluence_score": r["score"],
                "regime": "WYCKOFF",
                "scan_type": "wyckoff",
                "technical_evidence": {key: r.get(key) for key in (
                    "logic", "phases", "spring", "sos", "utad", "sow", "drawdown_pct",
                    "pump_pct", "acc_range", "dist_range", "ob", "fvg", "signals"
                ) if r.get(key) is not None},
            }
            if "RE-ACCUMULATION" in str(r.get("phases") or "").upper():
                sd["technical_evidence"]["reacc_trigger_validated"] = True
            sd["technical_evidence"]["causal_matrix_ready"] = True

            delivered = await _send_signal(sd)
            if delivered:
                logging.info(f"[WyckoffScan] {symbol} score={r['score']} RR={r['rr']} → отправлен")
            await asyncio.sleep(2)

        except Exception as e:
            logging.error(f"[WyckoffScan] {r.get('symbol')}: {e}")


async def auto_fast_deal_scan():
    """Каждые 20 минут: только ликвидные окна London/NY для точного FAST."""
    from datetime import datetime as _dt, timezone as _timezone
    _now_dt = _dt.now(_timezone.utc)
    _hour = _now_dt.hour
    _minute = _now_dt.minute
    _session = fast_session(_now_dt)
    if not _session:
        logging.debug(f"[auto_fast_deal_scan] вне Kill Zone ({_hour:02d}:{_minute:02d} UTC)")
        return
    try:
        await _run_market_scan_exclusive(
            "auto_fast_deal_scan",
            lambda: _auto_fast_deal_scan_impl(_hour, _minute, _session),
            90,
        )
    except asyncio.TimeoutError:
        logging.warning("[auto_fast_deal_scan] таймаут 90с — пропускаем цикл")
    except Exception as e:
        logging.error(f"[auto_fast_deal_scan] ОШИБКА: {e}")

async def _auto_fast_deal_scan_impl(_hour, _minute, _session="UNKNOWN"):
    logging.info(f"[auto_fast_deal_scan] ЗАПУЩЕН ({_session}, {_hour:02d}:{_minute:02d} UTC)")

    # Загружаем BTC свечи один раз для всех пар
    _shared_btc_4h = await asyncio.to_thread(get_candles, "BTCUSDT", "4h", 30)
    _shared_btc_1h = await asyncio.to_thread(get_candles, "BTCUSDT", "1h", 10)
    _shared_btc_5m = await asyncio.to_thread(get_candles, "BTCUSDT", "5m", 10)

    # Сохраняем в global storage для переиспользования другими функциями
    if _shared_btc_4h:
        update_global_candles("BTCUSDT", "4h", _shared_btc_4h)
    if _shared_btc_1h:
        update_global_candles("BTCUSDT", "1h", _shared_btc_1h)

    found = []
    batch = await asyncio.to_thread(
        _take_strategy_round_batch, "FAST", FAST_PAIRS, len(FAST_PAIRS), DB_PATH
    )
    pairs = batch["pairs"]
    await _control_scan_round(batch["round_id"])
    await _control_scan_scope(pairs, batch["target"])
    try:
        _fast_concurrency = _V3_CONFIG.operational.fast_concurrency
    except (TypeError, ValueError):
        _fast_concurrency = 6
    _fast_slots = asyncio.Semaphore(_fast_concurrency)

    async def _scan_fast_pair(symbol):
        async with _fast_slots:
            await _control_scan_pair(symbol)
            try:
                _liq_fast = await asyncio.to_thread(check_session_liquidity, symbol)
                if not _liq_fast["ok"]:
                    await _control_scan_outcome(symbol, "FILTERED", "LOW_LIQUIDITY")
                    logging.debug(
                        "[FAST] %s: низкая ликвидность (%sx) — пропускаем",
                        symbol, _liq_fast.get("ratio"),
                    )
                    return None
                result = await asyncio.to_thread(_v3_strategy_candidate, "FAST", symbol)
                if not result:
                    await _control_scan_outcome(symbol, "FILTERED", "NO_STRATEGY_SETUP")
                return result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await _control_scan_outcome(
                    symbol, "DATA_FAILED", "SCAN_ERROR", {"error": str(exc)[:300]}
                )
                logging.warning("[auto_fast_deal_scan] %s: %s", symbol, exc)
                return None

    # Gate reads are bounded, not unrestrained. Strategy predicates, candle
    # closure rules and candidate ordering remain unchanged.
    _fast_results = await asyncio.gather(*(_scan_fast_pair(symbol) for symbol in pairs))
    found.extend(result for result in _fast_results if result)

    if not found:
        return

    found.sort(key=lambda x: x["rr"], reverse=True)
    logging.info(f"Fast Deal scan: найдено {len(found)}")

    for r in found:
        try:
            symbol    = r["symbol"]
            direction = r["direction"]
            dir_label = "🟢LONG" if direction == "BULLISH" else "🔴SHORT"

            # AI комментарий

            _fast_tp1 = r.get("tp1", r["tp"])
            _fast_tp2 = r.get("tp2", r["tp"])
            _fast_tp2_pct = r.get("tp2_pct", r["tp_pct"])
            text = (
                f"⚡ <b>[FAST]</b> | <b>{symbol}</b> — {dir_label}\n"
                f"📊 Контекст: 4h | Сетап: 15m | Контроль: 5m | 1d: {r['direction_1d']}\n"
                f"\n"
                f"🎯 TP1:  <code>{smart_price_fmt(_fast_tp1)}</code> (+{r['tp_pct']}%)\n"
                f"🎯 TP2:  <code>{smart_price_fmt(_fast_tp2)}</code> (+{_fast_tp2_pct}%)\n"
                f"💰 Вход: <code>{smart_price_fmt(r['entry'])}</code>\n"
                f"🛑 Стоп: <code>{smart_price_fmt(r['sl'])}</code> (-{r['sl_pct']}%)\n"
                f"\n"
                f"📈 Логика: {r['logic']}\n"
                f"\n"
                f"🔍 Зона: {r['zone']}\n"
                f"⭐ RR: {r['rr']} | Риск: низкий\n"
                f"⏱ Горизонт: ~15-30 мин"
            )
            text += "\n\n💡 Это аналитика, не совет. Торгуй осознанно"

            # Проверка актуальности цены входа (1.5% для скальпинга)
            if not await asyncio.to_thread(
                _is_entry_still_valid,
                r,
                max_drift_pct=r.get("entry_drift_pct", 0.5),
            ):
                continue

            # Блокируем если сделка уже открыта
            try:
                _chk = _v3_connect_compatibility(DB_PATH, timeout=10)
                _open = _chk.execute(
                    "SELECT id FROM signals WHERE symbol=? AND direction=? AND result=\'pending\' AND signal_type=\'FAST\' LIMIT 1",
                    (symbol, direction)
                ).fetchone()
                _chk.close()
                if _open:
                    continue
            except Exception:
                pass

            # Cooldown: не отправляем если недавно уже был FAST сигнал по этому символу
            try:
                _cdc = _v3_connect_compatibility(DB_PATH, timeout=10)
                _cdrow = _cdc.execute(
                    "SELECT 1 FROM signals WHERE symbol=? AND signal_type='FAST' AND created_at > datetime('now', '-30 minutes') LIMIT 1",
                    (symbol,)
                ).fetchone()
                _cdc.close()
                if _cdrow:
                    logging.info(f"[FastDeal] {symbol} — cooldown 30 min, пропускаем")
                    continue
            except Exception:
                pass

            # Сохраняем в БД с tp1 и tp2 отдельно для частичного закрытия
            _f_tp1 = r.get("tp1", r["tp"])
            _f_tp2 = r.get("tp2", r["tp"])
            _fast_sd = {
                "symbol": symbol, "direction": direction, "timeframe": "15m",
                "entry": r["entry"], "sl": r["sl"], "tp1": _f_tp1,
                "tp2": _f_tp2, "tp3": _f_tp2, "rr": r.get("rr"),
                "grade": "FAST", "scan_type": "fast",
                "confluence_score": int(r["rr"] * 20), "regime": "FAST",
                "technical_evidence": {key: r.get(key) for key in (
                    "logic", "zone", "direction_1d", "funding_warning", "ob", "fvg",
                    "structure_event", "entry_drift_pct"
                ) if r.get(key) is not None},
            }
            _fast_sd["technical_evidence"]["causal_matrix_ready"] = True
            _fast_sd["text"] = text
            delivered = await _send_signal(_fast_sd)
            if delivered:
                logging.info(f"[FastDeal] {symbol} {direction} RR={r['rr']} → отправлен")
            await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"[FastDeal] {r.get('symbol')}: {e}")


async def auto_ltf_watch_scan():
    """Passively recheck only setups waiting for an existing LTF requirement."""
    if _market_scan_lock.locked():
        logging.debug("[LTFWatch] heavy scanner active; retry on the next tick")
        return
    try:
        await _run_market_scan_exclusive("auto_ltf_watch_scan", _auto_ltf_watch_scan_impl, 90)
    except asyncio.TimeoutError:
        logging.warning("[LTFWatch] timeout 90s; remaining observations stay queued")
    except Exception as exc:
        logging.warning("[LTFWatch] failed safely: %s", exc)


async def _auto_ltf_watch_scan_impl():
    watches = await asyncio.to_thread(_due_ltf_watches, 12, DB_PATH)
    if not watches:
        return
    await _control_scan_scope(watches, len(watches))
    for item in watches:
        strategy = str(item.get("strategy") or "").upper()
        symbol = str(item.get("symbol") or "")
        await _control_scan_pair(symbol)
        try:
            if strategy == "MTF":
                result = await asyncio.to_thread(full_scan_raw, symbol, "1h", True, True)
            elif strategy == "ZONE":
                result = await asyncio.to_thread(
                    _v3_strategy_candidate, "ZONE", symbol,
                    timeframe="4h", passive_watch=True,
                )
            else:
                await asyncio.to_thread(
                    _touch_ltf_watch, strategy, symbol, "unsupported passive strategy", False, DB_PATH
                )
                continue

            if result and result.get("_pending_ltf"):
                await asyncio.to_thread(
                    _touch_ltf_watch, strategy, symbol,
                    result.get("reason", "LTF confirmation is still pending"), False, DB_PATH,
                )
                continue
            if not result:
                await asyncio.to_thread(
                    _touch_ltf_watch, strategy, symbol,
                    "base setup or LTF trigger is not complete", False, DB_PATH,
                )
                continue

            candidate = result if strategy == "MTF" else _zone_candidate_from_setup(result)
            if strategy == "MTF":
                candidate["grade"] = "MTF"
            if not await asyncio.to_thread(_is_entry_still_valid, candidate, max_drift_pct=2.0):
                await asyncio.to_thread(
                    _touch_ltf_watch, strategy, symbol, "entry drift; keep observing", False, DB_PATH
                )
                continue
            await asyncio.to_thread(
                _touch_ltf_watch, strategy, symbol, "required LTF confirmation completed", True, DB_PATH
            )
            await _send_signal(candidate)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("[LTFWatch] %s %s: %s", strategy, symbol, exc)
            await asyncio.to_thread(
                _touch_ltf_watch, strategy, symbol, f"safe retry: {str(exc)[:160]}", False, DB_PATH
            )
        await asyncio.sleep(0)

async def auto_accumulation_scan():
    """Каждый час: сканируем Gate USD-M universe на накопление."""
    try:
        await asyncio.wait_for(_auto_accumulation_scan_impl(), timeout=210)
    except asyncio.TimeoutError:
        logging.warning("[auto_accumulation_scan] таймаут 210с — пропускаем цикл")
    except Exception as e:
        logging.error(f"[auto_accumulation_scan] ОШИБКА: {e}")

async def _auto_accumulation_scan_impl():
    pairs = get_top_pairs(DEFAULT_UNIVERSE_SIZE)
    found = []

    for symbol in pairs:
        try:
            acc = detect_accumulation(symbol)
            if acc and acc["score"] >= 72:
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
            return _v3_strategy_candidate("MTF", symbol, timeframe="1h")
        except:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(scan_one, pairs))

    for r in results:
        if r and r.get("grade") in ("СДЕЛКА", "СИЛЬНЫЙ СИГНАЛ", "МЕГА", "ПРИОРИТЕТ 1", "ПРЕМИУМ СИГНАЛ"):
            found.append(r)

    # Сортируем: лучшие первыми
    grade_order = {"ПРЕМИУМ СИГНАЛ": 0, "ПРИОРИТЕТ 1": 0, "МЕГА": 1, "СИЛЬНЫЙ СИГНАЛ": 2, "СДЕЛКА": 3}
    found.sort(key=lambda x: grade_order.get(x.get("grade",""), 4))
    return found


@_audit_strategy("MTF")
def full_scan_raw(symbol, timeframe="1h", auto=False, passive_watch=False):
    """Возвращает dict с текстом и grade для фильтрации"""
    try:
        # Проверяем есть ли уже открытый сигнал по этому символу в БД
        try:
            with _v3_connect_compatibility(DB_PATH) as _chk:
                _row = _chk.execute(
                    "SELECT id FROM signals WHERE symbol=? AND timeframe=? AND result=\'pending\' LIMIT 1",
                    (symbol, timeframe)
                ).fetchone()
                if _audit_test('MTF_FULL_SCAN_RAW_G4558', (_row), '_row', '_row', 4558):
                    return _audit_fail('MTF_FULL_SCAN_RAW_R4559', '_row', locals(), '_row', 4559)  # уже есть открытая сделка по этой паре+ТФ — не дублируем
        except Exception as _e:
            pass

        mtf = multi_tf_analysis(symbol, ["15m", "1h", "4h", "1d"])  # основной анализ, 1d как контекст
        if _audit_test('MTF_FULL_SCAN_RAW_G4564', (not mtf), 'not mtf', 'not mtf', 4564):
            return _audit_fail('MTF_FULL_SCAN_RAW_R4565', 'not mtf', locals(), 'not mtf', 4565)

        direction = mtf["direction"]

        # Фильтр BTC тренда — не шортим если BTC растёт, не лонгуем если BTC падает
        if symbol != 'BTCUSDT':
            try:
                btc_mtf = multi_tf_analysis("BTCUSDT", ["1h", "4h"])
                if btc_mtf:
                    btc_dir = btc_mtf.get("direction")
                    if _audit_test('MTF_FULL_SCAN_RAW_G4585', (direction == "BEARISH" and btc_dir == "BULLISH" and timeframe in ("1h",)), 'Фильтр BTC тренда — не шортим если BTC растёт, не лонгуем если BTC падает', 'direction == "BEARISH" and btc_dir == "BULLISH" and timeframe in ("1h",)', 4585):
                        return _audit_fail('MTF_FULL_SCAN_RAW_R4586', 'Фильтр BTC тренда — не шортим если BTC растёт, не лонгуем если BTC падает', locals(), 'direction == "BEARISH" and btc_dir == "BULLISH" and timeframe in ("1h",)', 4586)  # не шортим альты когда BTC растёт на 1h
                    if _audit_test('MTF_FULL_SCAN_RAW_G4587', (direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)), 'direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)', 'direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)', 4587):
                        return _audit_fail('MTF_FULL_SCAN_RAW_R4588', 'direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)', locals(), 'direction == "BULLISH" and btc_dir == "BEARISH" and timeframe in ("1h",)', 4588)  # не лонгуем альты когда BTC падает на 1h
            except Exception:
                pass
        raw_candles = get_candles(symbol, timeframe, 101)
        if _audit_test('MTF_FULL_SCAN_RAW_G4592', (len(raw_candles) < 21), 'len(raw_candles) < 21', 'len(raw_candles) < 21', 4592):
            return _audit_fail('MTF_FULL_SCAN_RAW_R4593', 'len(raw_candles) < 21', locals(), 'len(raw_candles) < 21', 4593)
        live_price = raw_candles[-1]["close"]
        candles = get_confirmed_candles(raw_candles)

        # ── MUST 1: OB/FVG зона ±ATR×vf ──
        try:
            _ob_check = find_ob(candles, direction)
            _fvg_check = find_fvg(candles, direction)
            _atr_check = sum(candles[-i]["high"] - candles[-i]["low"] for i in range(1, 15)) / 14
            _ap_mtf = get_adaptive_params(symbol, candles)
            _vf_mtf = _ap_mtf["volatility_factor"]
            _in_ob = (_ob_check and
                      abs(live_price - (_ob_check["top"] + _ob_check["bottom"]) / 2)
                      <= _atr_check * _vf_mtf)
            _in_fvg = (_fvg_check and
                       abs(live_price - (_fvg_check["top"] + _fvg_check["bottom"]) / 2)
                       <= _atr_check * _vf_mtf)
            if _audit_test('MTF_FULL_SCAN_RAW_G4610', (not _in_ob and not _in_fvg), 'not _in_ob and not _in_fvg', 'not _in_ob and not _in_fvg', 4610):
                logging.debug(f"[MTF] {symbol}: цена не у OB/FVG зоны — блок")
                return _audit_fail('MTF_FULL_SCAN_RAW_R4612', 'not _in_ob and not _in_fvg', locals(), 'not _in_ob and not _in_fvg', 4612)
        except Exception as _mtf_zone_error:
            logging.warning("[MTF] %s: обязательная проверка OB/FVG недоступна: %s", symbol, _mtf_zone_error)
            return _audit_fail('MTF_FULL_SCAN_RAW_R4615', 'detector returned None', locals(), '', 4615)

        # ── MUST 2: Тренд EMA50/EMA20 + структура HH/HL ──
        try:
            _ema_candles = get_confirmed_candles(get_candles(symbol, "4h", 61))
            if _ema_candles and len(_ema_candles) >= 50:
                _closes = [c["close"] for c in _ema_candles]
                _ema50 = ema_value(_closes, 50)
                _ema20 = ema_value(_closes, 20)
                if _audit_test('MTF_FULL_SCAN_RAW_G4624', (_ema20 is None or _ema50 is None), '_ema20 is None or _ema50 is None', '_ema20 is None or _ema50 is None', 4624):
                    return _audit_fail('MTF_FULL_SCAN_RAW_R4625', '_ema20 is None or _ema50 is None', locals(), '_ema20 is None or _ema50 is None', 4625)
                _price_4h = _closes[-1]
                _mtf_highs, _mtf_lows = find_swings(_ema_candles, lookback=5)
                _hh_hl = (
                    len(_mtf_highs) >= 2 and len(_mtf_lows) >= 2
                    and _mtf_highs[-1][1] > _mtf_highs[-2][1]
                    and _mtf_lows[-1][1] > _mtf_lows[-2][1]
                )
                _ll_lh = (
                    len(_mtf_highs) >= 2 and len(_mtf_lows) >= 2
                    and _mtf_highs[-1][1] < _mtf_highs[-2][1]
                    and _mtf_lows[-1][1] < _mtf_lows[-2][1]
                )

                _mtf_adx_weak = _ap_mtf.get("adx_weak", False)
                if direction == 'BULLISH':
                    if _mtf_adx_weak:
                        _trend_ok = _hh_hl or (_ema20 > _ema50)
                    else:
                        _trend_ok = (_price_4h > _ema50 and _ema20 > _ema50) or _hh_hl
                    if _audit_test('MTF_FULL_SCAN_RAW_G4645', (not _trend_ok), 'not _trend_ok', 'not _trend_ok', 4645):
                        logging.debug(f"[MTF] {symbol}: тренд не подтверждён для LONG — блок")
                        return _audit_fail('MTF_FULL_SCAN_RAW_R4647', 'not _trend_ok', locals(), 'not _trend_ok', 4647)
                else:
                    if _mtf_adx_weak:
                        _trend_ok = _ll_lh or (_ema20 < _ema50)
                    else:
                        _trend_ok = (_price_4h < _ema50 and _ema20 < _ema50) or _ll_lh
                    if _audit_test('MTF_FULL_SCAN_RAW_G4653', (not _trend_ok), 'not _trend_ok', 'not _trend_ok', 4653):
                        logging.debug(f"[MTF] {symbol}: тренд не подтверждён для SHORT — блок")
                        return _audit_fail('MTF_FULL_SCAN_RAW_R4655', 'not _trend_ok', locals(), 'not _trend_ok', 4655)
            else:
                return _audit_fail('MTF_FULL_SCAN_RAW_R4657', '_ema_candles and len(_ema_candles) >= 50', locals(), '_ema_candles and len(_ema_candles) >= 50', 4657)
        except Exception as _mtf_trend_error:
            logging.warning("[MTF] %s: обязательная проверка тренда недоступна: %s", symbol, _mtf_trend_error)
            return _audit_fail('MTF_FULL_SCAN_RAW_R4660', 'detector returned None', locals(), '', 4660)

        # ── Premium/Discount зона — реальный расчёт ──
        try:
            _pd_raw = get_candles(symbol, "4h", 51)
            _pd_candles = get_confirmed_candles(_pd_raw)
            if _pd_candles and len(_pd_candles) >= 20:
                _pd_high = max(c["high"] for c in _pd_candles[-20:])
                _pd_low = min(c["low"] for c in _pd_candles[-20:])
                _pd_mid = (_pd_high + _pd_low) / 2
                _pd_price = _pd_raw[-1]["close"]
                _pd_span = _pd_high - _pd_low
                _audit_observe("mtf_numeric", {
                    "pd_position_pct": round((_pd_price - _pd_low) / _pd_span * 100, 6) if _pd_span > 0 else None,
                    "pd_mid_distance_pct": round((_pd_price - _pd_mid) / _pd_span * 100, 6) if _pd_span > 0 else None,
                    "pd_price": _pd_price, "pd_low": _pd_low, "pd_mid": _pd_mid, "pd_high": _pd_high,
                })

                if _audit_test('MTF_FULL_SCAN_RAW_G4672', (direction == "BULLISH" and _pd_price > _pd_mid), 'direction == "BULLISH" and _pd_price > _pd_mid', 'direction == "BULLISH" and _pd_price > _pd_mid', 4672):
                    logging.debug(f"[MTF] {symbol}: цена в Premium зоне — LONG заблокирован")
                    return _audit_fail('MTF_FULL_SCAN_RAW_R4674', 'direction == "BULLISH" and _pd_price > _pd_mid', locals(), 'direction == "BULLISH" and _pd_price > _pd_mid', 4674)
                elif _audit_test('MTF_FULL_SCAN_RAW_G4675', (direction == "BEARISH" and _pd_price < _pd_mid), 'direction == "BEARISH" and _pd_price < _pd_mid', 'direction == "BEARISH" and _pd_price < _pd_mid', 4675):
                    logging.debug(f"[MTF] {symbol}: цена в Discount зоне — SHORT заблокирован")
                    return _audit_fail('MTF_FULL_SCAN_RAW_R4677', 'direction == "BEARISH" and _pd_price < _pd_mid', locals(), 'direction == "BEARISH" and _pd_price < _pd_mid', 4677)
        except Exception as _mtf_pd_error:
            logging.warning("[MTF] %s: обязательная Premium/Discount проверка недоступна: %s", symbol, _mtf_pd_error)
            return _audit_fail('MTF_FULL_SCAN_RAW_R4680', 'detector returned None', locals(), '', 4680)

        price = live_price
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

        # MM Accumulation Detector
        try:
            mm_acc = detect_mm_accumulation(candles)
            mm_sig = mm_acc.get("signal", "NEUTRAL")
            mm_score = mm_acc.get("score", 0)
            if mm_sig == "STRONG_ACCUMULATION":
                confluence.append(f"✅ MM Накопление СИЛЬНОЕ (score {mm_score}/4) — фондовый паттерн (+15)")
                for s in mm_acc.get("signals", []):
                    confluence.append(f"  {s}")
            elif mm_sig == "ACCUMULATION":
                confluence.append(f"✅ MM Накопление (score {mm_score}/4) — вероятен выход (+10)")
                for s in mm_acc.get("signals", []):
                    confluence.append(f"  {s}")
            elif mm_sig == "WEAK_ACCUMULATION":
                confluence.append(f"📦 MM Слабое накопление (score {mm_score}/4)")
            if mm_acc.get("pre_pump"):
                confluence.append(f"🚀 PRE-PUMP: объём↑ диапазон↓ лои↑")
        except Exception as _mm_e:
            pass

        # RSI/MACD Divergence
        try:
            rmd = detect_rsi_macd_divergence(candles, direction)
            if rmd["found"]:
                for s in rmd["signals"]:
                    confluence.append(s)
        except Exception:
            pass

        # VWAP
        try:
            vwap_data = calculate_vwap(candles)
            if vwap_data["vwap"] > 0:
                if vwap_data["near_vwap"]:
                    confluence.append(f"📍 Цена у VWAP {vwap_data['vwap']:.4f} — зона интереса")
                elif vwap_data["signal"] == direction:
                    confluence.append(f"✅ VWAP: {vwap_data['desc']} (+5)")
                else:
                    confluence.append(f"⚠️ VWAP: {vwap_data['desc']}")
        except Exception:
            pass

        # Heatmap ликвидности
        try:
            heatmap = get_liquidity_heatmap(candles)
            if direction == "BULLISH" and heatmap.get("nearest_buy_stops"):
                lvl = heatmap["nearest_buy_stops"]
                if lvl["strength"] == "HIGH":
                    confluence.append(f"🎯 Buy Stops +{lvl['dist_pct']:.1f}% выше ({lvl['touches']} касаний) — цель")
            elif direction == "BEARISH" and heatmap.get("nearest_sell_stops"):
                lvl = heatmap["nearest_sell_stops"]
                if lvl["strength"] == "HIGH":
                    confluence.append(f"🎯 Sell Stops -{lvl['dist_pct']:.1f}% ниже ({lvl['touches']} касаний) — цель")
        except Exception:
            pass

        # Breaker Block
        try:
            breaker = detect_breaker_block(candles, direction)
            if breaker:
                confluence.append(f"✅ {breaker['desc']} (+{breaker['weight']})")
        except Exception:
            pass

        # Минимальный порог confluence по таймфрейму
        # Предупреждения не являются подтверждениями. Для редких точных MTF
        # сетапов нужны реальные положительные confluence.
        _positive_confluence = [c for c in confluence if c.lstrip().startswith(("✅", "🎯", "🔥", "🚀"))]
        min_conf = {"1h": 3, "4h": 4, "1d": 4, "1w": 3}
        _audit_observe("mtf_numeric", {
            "positive_confluence_count": len(_positive_confluence),
            "positive_confluence_required": min_conf.get(timeframe, 4),
        })
        if _audit_test('MTF_FULL_SCAN_RAW_G4767', (len(_positive_confluence) < min_conf.get(timeframe, 4)), 'сетапов нужны реальные положительные confluence.', 'len(_positive_confluence) < min_conf.get(timeframe, 4)', 4767):
            logging.debug(f"[full_scan_raw] {symbol} {timeframe}: отфильтрован (positive confluence {len(_positive_confluence)} < {min_conf.get(timeframe,4)})")
            return _audit_fail('MTF_FULL_SCAN_RAW_R4769', 'сетапов нужны реальные положительные confluence.', locals(), 'len(_positive_confluence) < min_conf.get(timeframe, 4)', 4769)

        # ══════════════════════════════════════
        # 🔴 ОСНОВА — 1h+4h direction, 15m closed-bar structure trigger
        # ══════════════════════════════════════
        _dir_15m = smc_on_tf(symbol, "15m")
        _dir_1h  = smc_on_tf(symbol, "1h")
        _dir_4h  = smc_on_tf(symbol, "4h")
        _dir_1d  = smc_on_tf(symbol, "1d")  # контекст

        _tf_match = sum([_dir_15m == direction, _dir_1h == direction, _dir_4h == direction])
        _core_tf_match = sum([_dir_1h == direction, _dir_4h == direction])
        _audit_observe("mtf_numeric", {"tf_match": _tf_match, "core_tf_match": _core_tf_match})
        # Hierarchical MTF: 4h defines structure, 1h defines the setup and
        # 15m confirms that the pullback has ended.  Requiring 15m to remain
        # trend-aligned throughout the pullback rejects the very entries the
        # lower timeframe is supposed to time.
        if _audit_test('MTF_FULL_SCAN_RAW_G4785', (_core_tf_match < 2), 'lower timeframe is supposed to time.', '_core_tf_match < 2', 4785):
            logging.debug(f"[MTF] {symbol}: 1h/4h не совпадают — пропускаем")
            return _audit_fail('MTF_FULL_SCAN_RAW_R4787', 'lower timeframe is supposed to time.', locals(), '_core_tf_match < 2', 4787)

        # Для редких сделок дневной тренд должен подтверждать направление.
        _htf_1d_agrees = _dir_1d == direction
        if _audit_test('MTF_FULL_SCAN_RAW_G4791', (_dir_1d and not _htf_1d_agrees), 'Для редких сделок дневной тренд должен подтверждать направление.', '_dir_1d and not _htf_1d_agrees', 4791):
            logging.debug(f"[MTF] {symbol}: 1d против направления — блок")
            return _audit_fail('MTF_FULL_SCAN_RAW_R4793', 'Для редких сделок дневной тренд должен подтверждать направление.', locals(), '_dir_1d and not _htf_1d_agrees', 4793)

        _weak_mtf_warn = ""  # 1h/4h mandatory; 15m is the closed-bar trigger.

        # Только 1h и 4h — 1d/1w не торгуем (используем только для контекста)
        if _audit_test('MTF_FULL_SCAN_RAW_G4798', (timeframe not in ("1h", "4h")), 'Только 1h и 4h — 1d/1w не торгуем (используем только для контекста)', 'timeframe not in ("1h", "4h")', 4798):
            return _audit_fail('MTF_FULL_SCAN_RAW_R4799', 'Только 1h и 4h — 1d/1w не торгуем (используем только для контекста)', locals(), 'timeframe not in ("1h", "4h")', 4799)

        # Расчёт уровней по реальной рыночной структуре (SMC)
        levels = calc_smart_levels(candles, direction, price, timeframe)
        if _audit_test('MTF_FULL_SCAN_RAW_G4803', (not levels), 'Расчёт уровней по реальной рыночной структуре (SMC)', 'not levels', 4803):
            return _audit_fail('MTF_FULL_SCAN_RAW_R4804', 'Расчёт уровней по реальной рыночной структуре (SMC)', locals(), 'not levels', 4804)
        entry = levels["entry"]
        _ob_mitigated = levels.get("mitigated", False)
        if _audit_test('MTF_FULL_SCAN_RAW_G4807', (_ob_mitigated), '_ob_mitigated', '_ob_mitigated', 4807):
            logging.info(f"[MTF] {symbol} {direction} — OB mitigated, пропускаем")
            return _audit_fail('MTF_FULL_SCAN_RAW_R4809', '_ob_mitigated', locals(), '_ob_mitigated', 4809)
        entry = levels["entry"]
        sl    = levels["sl"]
        tp1   = levels["tp1"]
        tp2   = levels["tp2"]
        tp3   = levels["tp3"]
        # Universal RR contract: floor 2.0, no upper ceiling.
        _rr_val = levels.get("rr", 0)
        _audit_observe("mtf_numeric", {"rr_value": _rr_val})
        if _audit_test('MTF_FULL_SCAN_RAW_G4817', (_rr_val < 2.0), 'RR >= 2.0, no upper ceiling', '_rr_val < 2.0', 4817):
            logging.debug(f"[full_scan_raw] {symbol} {timeframe}: RR {_rr_val:.2f} < 2.0 — пропускаем")
            return _audit_fail('MTF_FULL_SCAN_RAW_R4819', 'RR >= 2.0, no upper ceiling', locals(), '_rr_val < 2.0', 4819)

        # OTE/структурный entry обязан быть рядом с текущей ценой. Не
        # отправляем отложенный сетап как будто это вход прямо сейчас.
        _atr_entry = sum(c["high"] - c["low"] for c in candles[-14:]) / min(14, len(candles))
        if _audit_test('MTF_FULL_SCAN_RAW_G4824', (abs(price - entry) > _atr_entry * 0.75), 'отправляем отложенный сетап как будто это вход прямо сейчас.', 'abs(price - entry) > _atr_entry * 0.75', 4824):
            logging.debug(f"[MTF] {symbol}: entry слишком далеко от текущей цены")
            return _audit_fail('MTF_FULL_SCAN_RAW_R4826', 'отправляем отложенный сетап как будто это вход прямо сейчас.', locals(), 'abs(price - entry) > _atr_entry * 0.75', 4826)

        # VWAP — контекст для Groq
        vwap_warning = any("перекуплен" in c or "перепродан" in c for c in confluence)

        est_hours, confidence, win_rate = get_estimated_time(symbol, timeframe)
        time_str = f"~{est_hours}ч" if est_hours < 24 else f"~{est_hours//24}дн"
        wr_str = f"{win_rate:.0f}% WR" if win_rate > 0 else "нет истории"
        tf_label = TF_LABELS.get(timeframe, timeframe)

        conf_score = len(_positive_confluence) * 15
        # save_signal_db вызывается ниже — только после проверки тайминга
        emoji = "🟢" if direction == "BULLISH" else "🔴"
        conf_text = "\n".join(confluence)

        # Считаем % прибыли для TP
        if direction == "BULLISH":
            tp1_pct = (tp1 - entry) / entry * 100
            tp2_pct = (tp2 - entry) / entry * 100
            tp3_pct = (tp3 - entry) / entry * 100
        else:
            tp1_pct = (entry - tp1) / entry * 100
            tp2_pct = (entry - tp2) / entry * 100
            tp3_pct = (entry - tp3) / entry * 100

        # Время по таймфрейму
        TF_TIME_LABEL = {"15m": "2-6ч", "1h": "12-48ч", "4h": "2-7дн", "1d": "1-4нед"}
        tf_time_hint = TF_TIME_LABEL.get(timeframe, time_str)

        # Название сигнала по таймфрейму
        tf_signal_names = {
            "1h":  ("🔥", "СДЕЛКА"),
            "4h":  ("🔥🔥", "СИЛЬНЫЙ СИГНАЛ"),
            "1d":  ("🔥🔥🔥", "МЕГА"),
            "1w":  ("💰", "ПРИОРИТЕТ 1"),
        }
        sig_emoji, sig_name = tf_signal_names.get(timeframe, ("🔥", "СИГНАЛ"))
        # Премиум если сильное MM накопление
        if any("MM Накопление СИЛЬНОЕ" in c for c in confluence):
            sig_emoji, sig_name = "💎", "ПРЕМИУМ СИГНАЛ"

        risk_level = "низкий" if levels.get("rr", 0) >= 3 else "средний" if levels.get("rr", 0) >= 2 else "высокий"
        dir_label = "🟢LONG" if direction == "BULLISH" else "🔴SHORT"
        trend_icon = "📈" if direction == "BULLISH" else "📉"

        # ── Groq анализирует логику позиционного входа ──
        groq_logic = ""
        groq_time = tf_time_hint
        _groq_valid = True  # default: пропускаем если Groq не ответил
        try:
            fg = get_fear_greed()
            funding = get_funding_rate(symbol)
            regime = get_market_regime(symbol)
            fg_val = f"{fg['value']} ({fg['label']})" if fg else "N/A"
            fund_val = f"{funding:+.4f}%" if funding is not None else "N/A"
            regime_val = regime.get("mode", "?") if isinstance(regime, dict) else str(regime)
            htf_1d = smc_on_tf(symbol, "1d")
            htf_1w = smc_on_tf(symbol, "1w")
            # 1w конфликт — предупреждение для Groq
            _1w_conflict = False
            if htf_1w:
                if direction == "BULLISH" and "BEARISH" in str(htf_1w).upper():
                    _1w_conflict = True
                elif direction == "BEARISH" and "BULLISH" in str(htf_1w).upper():
                    _1w_conflict = True
            _1w_warn = "⚠️ ПРОТИВ 1w тренда" if _1w_conflict else ""
            # BTC тренд
            _btc_1h = smc_on_tf("BTCUSDT", "1h")
            _btc_1d = smc_on_tf("BTCUSDT", "1d")
            _btc_str = f"BTC 1h: {_btc_1h}, 1d: {_btc_1d}"
            # OB/FVG зоны
            _ob_str = f"OB: {ob['bottom']:.6f}–{ob['top']:.6f}" if ob else "OB: нет"
            _fvg_str = f"FVG: {fvg['bottom']:.6f}–{fvg['top']:.6f}" if fvg else "FVG: нет"
            # ATR
            _candle_h = [c["high"] for c in candles[-14:]]
            _candle_l = [c["low"] for c in candles[-14:]]
            _atr_mtf = sum(_candle_h[i] - _candle_l[i] for i in range(len(_candle_h))) / len(_candle_h)
            # Volume
            _vol_last = candles[-1].get("volume", 0)
            _vol_avg = sum(c.get("volume", 0) for c in candles[-20:-1]) / 19 if len(candles) >= 20 else 0
            _vol_str = f"Vol: {_vol_last:.0f} vs avg: {_vol_avg:.0f}" if _vol_avg > 0 else ""

            conf_short = "\n".join(confluence[:5]) if confluence else "нет данных"
            _pat_str = ""
            _sl_pct_mtf = round(abs(entry - sl) / entry * 100, 1) if entry > 0 else 0
            groq_prompt = (
                "Ты профессиональный SMC трейдер с 10-летним стажем. "
                "Торгуешь только лучшие сетапы — лучше пропустить 10 хороших чем взять 1 плохой. "
                f'Ответь СТРОГО JSON: {{\"logic\": \"причина входа макс 15 слов\", \"hours\": число, \"valid\": true/false}}\n\n'
                "ПРАВИЛА БЛОКИРОВКИ (верни valid: false если):\n"
                f"- RR < 2.0 — риск не оправдан (RR сейчас: {levels.get('rr',0)})\n"
                f"- Стоп не находится за структурной инвалидацией (стоп сейчас: {_sl_pct_mtf}%)\n"
                "- Цена не у структурного уровня (OB/FVG/swing)\n"
                "- 4h или 1d против направления сигнала\n"
                "- Нет чёткого CHoCH или BOS подтверждающего вход\n"
                "- Рынок в боковике без импульса\n"
                "- Экстремальный funding — предупреждение, но не самостоятельный запрет\n\n"
                "ПРАВИЛА ПОДТВЕРЖДЕНИЯ (valid: true если):\n"
                "- Цена чётко в OB или FVG зоне\n"
                "- 1h и 4h согласованы, а закрытая 15m свеча дала BOS/CHoCH триггер\n"
                "- Есть CHoCH или BOS после sweep ликвидности\n"
                "- RR ≥ 2.0, стоп за структурным уровнем\n"
                "- TP на реальном уровне (предыдущий swing, OB, ликвидность)\n"
                "- BTC подтверждает направление\n\n"
                f"Данные: Пара: {symbol} ТФ: {tf_label} Направление: {direction}\n"
                f"Вход: {smart_price_fmt(entry)} SL: {smart_price_fmt(sl)} TP: {smart_price_fmt(tp1)}\n"
                f"1d тренд (обязательный HTF-фильтр): {_dir_1d}\n"
                f"ТФ совпадение: {_tf_match}/3 (15m={_dir_15m} 1h={_dir_1h} 4h={_dir_4h})\n"
                f"MTF: {mtf.get('match_count',0)}/3 | 1d: {htf_1d} | 1w: {htf_1w} {_1w_warn}\n"
                f"RR: {levels.get('rr',0)} | Стоп: {_sl_pct_mtf}% | Fear&Greed: {fg_val} | Funding: {fund_val}\n"
                f"Режим: {regime_val} | {_btc_str}\n"
                f"{_ob_str} | {_fvg_str} | ATR: {smart_price_fmt(_atr_mtf)}\n"
                f"{_vol_str}\n"
                f"Confluence:\n{conf_short}"
                f"{_pat_str}"
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
                    # Groq как фильтр — если valid=false, блокируем
                    if _audit_test('MTF_FULL_SCAN_RAW_G4964', (not parsed.get("valid", True)), 'Groq как фильтр — если valid=false, блокируем', 'not parsed.get("valid", True)', 4964):
                        logging.info(f"[MTF Groq] {symbol} {direction}: Groq отклонил сигнал")
                        return _audit_fail('MTF_FULL_SCAN_RAW_R4966', 'Groq как фильтр — если valid=false, блокируем', locals(), 'not parsed.get("valid", True)', 4966)
                    if parsed.get("logic") and len(str(parsed["logic"])) > 5:
                        raw_logic = str(parsed["logic"]).strip()
                        # Убираем JSON артефакты если Groq вернул сырой JSON
                        if raw_logic.startswith("{") or '"logic"' in raw_logic:
                            import re as _re2
                            m = _re2.search(r'"logic"\s*:\s*"([^"]+)"', raw_logic)
                            groq_logic = m.group(1) if m else raw_logic[:100]
                        else:
                            groq_logic = raw_logic
                    if parsed.get("hours"):
                        hrs = int(parsed["hours"])
                        groq_time = f"~{hrs}ч" if hrs < 24 else f"~{hrs//24}дн"
                except Exception:
                    clean_text = groq_response.strip().replace("\n", " ")
                    if len(clean_text) > 10 and not clean_text.upper() == clean_text:
                        groq_logic = clean_text[:80]
        except Exception:
            pass

        # Fallback если Groq не ответил
        if not groq_logic:
            logic_lines = [c for c in confluence if any(w in c.lower() for w in
                ["свип", "sweep", "импульс", "накопл", "ликвидн", "пробой", "ob ", "fvg"])]
            groq_logic = "\n".join(logic_lines[:3]) if logic_lines else "структурный вход по SMC"

        # ══════════════════════════════════════
        # 🟢 ДОП — score минимум 1/4
        # ══════════════════════════════════════
        _mtf_score = 0
        _mtf_score_vol = False
        _mtf_score_btc = False
        _mtf_score_session = False
        _mtf_score_bos = False
        _mtf_structure_event = None

        # Volume spike ≥1.2x
        try:
            _avg_vol_m = sum(c["volume"] for c in candles[-20:-1]) / 19
            if _avg_vol_m > 0 and candles[-1]["volume"] > _avg_vol_m * 1.2:
                _mtf_score += 1
                _mtf_score_vol = True
        except Exception:
            pass

        # BTC совпадает
        try:
            _btc_m = get_candles("BTCUSDT", "1h", 5)
            if _btc_m and len(_btc_m) >= 3:
                _btc_dir_m = "BULLISH" if _btc_m[-1]["close"] > _btc_m[-3]["close"] else "BEARISH"
                if _btc_dir_m == direction:
                    _mtf_score += 1
                    _mtf_score_btc = True
        except Exception:
            pass

        # Активная сессия (London/NY)
        import datetime as _dt_m
        _h_m = _dt_m.datetime.utcnow().hour
        if 8 <= _h_m <= 17:
            _mtf_score += 1
            _mtf_score_session = True

        # BOS/CHoCH на 15m
        try:
            _c15m_m = get_confirmed_candles(get_candles(symbol, "15m", 31))
            _mtf_structure_event = (
                get_bos_choch_event(_c15m_m, direction, lookback=15, max_break_age=1)
                if _c15m_m else None
            )
            if _mtf_structure_event:
                _mtf_score += 1
                _mtf_score_bos = True
        except Exception:
            pass

        # Для MTF-сетапов нужны минимум 2 из 4 подтверждений,
        # включая реальную структуру (BOS/CHoCH), а не только сессию.
        # Passive-watch may return PENDING_LTF before the hard score gate below.
        # Record that terminal state explicitly so Strategy Lab cannot make RR
        # look like the last reached gate when the candidate is actually waiting
        # for a fresh closed 15m BOS/CHoCH.
        if passive_watch:
            _audit_test(
                'MTF_PASSIVE_LTF_BOS',
                (not _mtf_score_bos),
                'MTF passive watch: closed 15m BOS/CHoCH before candidate',
                'passive_watch and not _mtf_score_bos',
                5050,
            )
        if passive_watch and not _mtf_score_bos:
            return {
                "_pending_ltf": True,
                "symbol": symbol,
                "strategy": "MTF",
                "direction": direction,
                "required_timeframe": "15m",
                "reason": "ожидается закрытый 15m BOS/CHoCH",
            }
        if _audit_test('MTF_FULL_SCAN_RAW_G5053', (_mtf_score < 2 or not _mtf_score_bos), '_mtf_score < 2 or not _mtf_score_bos', '_mtf_score < 2 or not _mtf_score_bos', 5053):
            logging.debug(f"[MTF] {symbol}: score {_mtf_score}/4 — пропускаем")
            return _audit_fail('MTF_FULL_SCAN_RAW_R5055', '_mtf_score < 2 or not _mtf_score_bos', locals(), '_mtf_score < 2 or not _mtf_score_bos', 5055)

        _signal_strength = "🔥 Сильный" if _mtf_score >= 3 else "✅ Норм" if _mtf_score >= 2 else "⚡ Базовый"

        # ══════════════════════════════════════
        # 📝 ТЕКСТ СИГНАЛА
        # ══════════════════════════════════════
        _must_text = f"1h+4h {direction} | 15m BOS/CHoCH ✅"
        _1d_text = f"1d: {'✅' if _htf_1d_agrees else '⚠️'} {_dir_1d or '?'} (контекст)"

        _confirm_items = []
        if _mtf_score_vol:     _confirm_items.append("📊 Объём")
        if _mtf_score_btc:     _confirm_items.append("₿ BTC")
        if _mtf_score_session: _confirm_items.append("⏰ Сессия")
        if _mtf_score_bos:     _confirm_items.append("🔄 BOS")
        _confirm_text = " | ".join(_confirm_items) if _confirm_items else "—"

        _sl_pct_txt = round(abs(entry - sl) / entry * 100, 2) if entry > 0 else 0
        _dir_emoji = "🟢 LONG" if direction == "BULLISH" else "🔴 SHORT"

        text = (
            f"📐 <b>MTF</b> | {symbol} — {_dir_emoji}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📌 Основа: {_must_text}\n"
            f"📋 {_1d_text}\n"
            f"✅ Доп: {_mtf_score}/4 — {_confirm_text}\n"
            f"💪 Сила: {_signal_strength}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🎯 Вход:  <code>{smart_price_fmt(entry)}</code>\n"
            f"🛑 Стоп:  <code>{smart_price_fmt(sl)}</code>  ({_sl_pct_txt}%)\n"
            f"🎯 TP1:   <code>{smart_price_fmt(tp1)}</code>\n"
            f"🎯 TP2:   <code>{smart_price_fmt(tp2)}</code>\n"
            f"📊 RR:    {levels.get('rr', 0)}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 Логика: {groq_logic}\n"
            f"⏱ Горизонт: {groq_time}"
        )
        text += "\n\n💡 Это аналитика, не совет. Торгуй осознанно"

        return {
            "symbol": symbol, "grade": sig_name, "grade_emoji": sig_emoji, "text": text,
            "direction": direction, "entry": entry, "tp1": tp1, "tp2": tp2,
            "tp3": tp3, "sl": sl, "rr": _rr_val, "timeframe": timeframe,
            "confluence_score": conf_score, "regime": "UNKNOWN",
            "estimated_hours": est_hours,
            "scan_type": "mtf",
            "technical_evidence": {
                "causal_matrix_ready": True,
                "timeframe_alignment": {"15m": _dir_15m, "1h": _dir_1h, "4h": _dir_4h, "1d": _dir_1d},
                "mtf_match": _tf_match,
                "positive_confluence": _positive_confluence[:8],
                "confirmation_score": _mtf_score,
                "bos_choch": _mtf_score_bos,
                "structure_event": _mtf_structure_event,
                "btc_confirmed": _mtf_score_btc,
                "volume_confirmed": _mtf_score_vol,
                "ob": ob,
                "fvg": fvg,
            },
        }

    except Exception as e:
        logging.error(f"full_scan_raw error {symbol}: {e}")
        return _audit_fail('MTF_FULL_SCAN_RAW_R5118', 'detector returned None', locals(), '', 5118)


# Conversational, button-triggered and scheduled scans enter the same V3
# registry. The migration adapter still delegates to this exact full_scan_raw
# implementation, so strategy predicates and geometry remain unchanged.
def _canonical_mtf_scan_handler(symbol, timeframe="1h", auto=False, passive_watch=False):
    return _v3_strategy_candidate(
        "MTF", symbol, timeframe=timeframe, auto=auto,
        passive_watch=passive_watch,
    )


register_raw_scan_handler(_canonical_mtf_scan_handler)


# Кэш отправленных сигналов — symbol:direction -> timestamp (не спамим одним сигналом)
_sent_signal_cache: dict = {}
_SIGNAL_COOLDOWN_HOURS = 4  # один и тот же сигнал не чаще раз в 4 часа

# ===== MAIN =====

async def restore_db_from_github():
    """Restore the latest verified branch snapshot, regardless of local size."""
    result = {}
    for attempt, delay in enumerate((0, 2, 5), start=1):
        if delay:
            await asyncio.sleep(delay)
        result = await asyncio.to_thread(_BRAIN_PERSISTENCE.restore)
        if result.get("ready") or result.get("status") == "not_configured":
            break
        logging.warning(
            "[BrainPersistence] restore attempt %s/3 failed: %s",
            attempt, result.get("error") or result.get("status"),
        )
    if result.get("ready"):
        counts = result.get("counts", {})
        recovery = (
            f" recovered_from={str(result.get('recovered_from'))[:12]}"
            if result.get("recovered_from") else ""
        )
        logging.warning(
            "[BrainPersistence] restored g%s sha=%s knowledge=%s size=%sKB%s",
            result.get("generation", 0), str(result.get("blob_sha", ""))[:12],
            counts.get("knowledge", 0),
            int(result.get("size", 0)) // 1024, recovery,
        )
    else:
        logging.error(
            "[BrainPersistence] restore blocked: %s",
            result.get("error") or result.get("status"),
        )
    return result


async def backup_db_to_github(reason="scheduled"):
    """Snapshot once per process at a time and never overwrite a newer remote."""
    global _brain_backup_async_lock
    if _brain_backup_async_lock is None:
        _brain_backup_async_lock = asyncio.Lock()
    async with _brain_backup_async_lock:
        result = await asyncio.to_thread(_BRAIN_PERSISTENCE.backup, reason)
    status = result.get("status")
    if result.get("saved"):
        counts = result.get("counts", {})
        logging.warning(
            "[BrainPersistence] saved g%s sha=%s knowledge=%s size=%sKB reason=%s",
            result.get("generation", 0), str(result.get("blob_sha", ""))[:12],
            counts.get("knowledge", 0),
            int(result.get("size", 0)) // 1024, reason,
        )
    elif status not in ("unchanged", "not_configured"):
        logging.warning(
            "[BrainPersistence] backup %s: %s",
            status, result.get("error") or "write safely skipped",
        )
    return result


async def restore_state_db_from_github():
    """Restore V3 critical state, bootstrapping only a proven-missing remote."""
    result = await asyncio.to_thread(_STATE_PERSISTENCE.restore)
    if result.get("reason") == "REMOTE_MISSING":
        # First V3 rollout only: create a valid local schema, then use a
        # create-without-SHA request that cannot overwrite concurrent state.
        conn = _v3_connect_state(_V3_CONFIG)
        try:
            _v3_migrate_state(conn)
        finally:
            conn.close()
        result = await asyncio.to_thread(_STATE_PERSISTENCE.initialize, "v3_initial_state")
        if result.get("status") in {"concurrent_initialize", "remote_exists"}:
            result = await asyncio.to_thread(_STATE_PERSISTENCE.restore)
    return result


async def backup_state_db_to_github(reason="scheduled"):
    """CAS-back up the small critical State DB independently from brain.db."""
    global _state_backup_async_lock
    if _state_backup_async_lock is None:
        _state_backup_async_lock = asyncio.Lock()
    async with _state_backup_async_lock:
        return await asyncio.to_thread(_STATE_PERSISTENCE.backup, reason)


async def restore_memory_db_from_github():
    """Restore optional Live Memory without ever replacing unknown remote state."""
    result = await asyncio.to_thread(_MEMORY_PERSISTENCE.restore)
    if result.get("reason") == "REMOTE_MISSING":
        conn = _v3_connect_memory(_V3_CONFIG)
        try:
            _v3_migrate_memory(conn)
        finally:
            conn.close()
        result = await asyncio.to_thread(
            _MEMORY_PERSISTENCE.initialize, "v3_initial_memory"
        )
        if result.get("status") in {"concurrent_initialize", "remote_exists"}:
            result = await asyncio.to_thread(_MEMORY_PERSISTENCE.restore)
    return result


async def backup_memory_db_to_github(reason="scheduled"):
    """CAS-back up Live Memory independently and at lower priority."""
    global _memory_backup_async_lock
    if _memory_backup_async_lock is None:
        _memory_backup_async_lock = asyncio.Lock()
    async with _memory_backup_async_lock:
        return await asyncio.to_thread(_MEMORY_PERSISTENCE.backup, reason)


async def _v3_maintenance_and_backup(reason="safety_30m"):
    """Maintain bounded V3 stores, then checkpoint compatibility state."""
    def maintain():
        state = _v3_connect_state(_V3_CONFIG)
        try:
            state_report = _v3_maintain_state(
                state, _V3_CONFIG.database.state_db_path,
                telemetry_days=_V3_CONFIG.operational.state_telemetry_retention_days,
                resolved_incident_days=_V3_CONFIG.operational.resolved_incident_retention_days,
            )
        finally:
            state.close()
        resource = _v3_memory_snapshot(
            watch_ratio=_V3_CONFIG.operational.memory_watch_ratio,
            degraded_ratio=_V3_CONFIG.operational.memory_degraded_ratio,
            stop_ratio=_V3_CONFIG.operational.memory_stop_ratio,
            limit_bytes=_V3_CONFIG.operational.memory_limit_bytes,
        )
        memory_report = {"status": "SKIPPED_MEMORY_PRESSURE", "resource_state": resource.state}
        if resource.state not in {"DEGRADED", "NEW_ENTRIES_OFF"}:
            memory = _v3_connect_memory(_V3_CONFIG)
            try:
                memory_report = _v3_maintain_memory(
                    memory, _V3_CONFIG.database.memory_db_path,
                    context_days=_V3_CONFIG.operational.memory_context_retention_days,
                )
            finally:
                memory.close()
        return {"state": state_report, "memory": memory_report}

    maintenance = await asyncio.to_thread(maintain)
    state_backup = await backup_state_db_to_github(reason)
    memory_backup = {"status": "skipped_memory_pressure"}
    if maintenance["memory"].get("status") != "SKIPPED_MEMORY_PRESSURE":
        memory_backup = await backup_memory_db_to_github(reason)
    backup = await backup_db_to_github(reason)
    if _STATE_PERSISTENCE.configured and state_backup.get("status") not in {"saved", "unchanged"}:
        raise RuntimeError(f"state_backup_{state_backup.get('status') or 'failed'}")
    if _BRAIN_PERSISTENCE.configured and backup.get("status") not in {"saved", "unchanged"}:
        raise RuntimeError(f"compatibility_backup_{backup.get('status') or 'failed'}")
    if _MEMORY_PERSISTENCE.configured and memory_backup.get("status") not in {
        "saved", "unchanged", "skipped_memory_pressure",
    }:
        _V3_RUNTIME.mark_component(
            "memory_db", _V3_COMPONENT_STATE.DEGRADED,
            f"memory backup {memory_backup.get('status') or 'failed'}", required=False,
        )
        _v3_report_incident(
            "MEMORY_BACKUP_UNAVAILABLE", "memory_db", "WARNING",
            {"status": memory_backup.get("status") or "failed"},
        )
    elif memory_backup.get("status") in {"saved", "unchanged"}:
        _v3_recover_incident("MEMORY_BACKUP_UNAVAILABLE", "memory_db")
    return {
        "maintenance": maintenance, "state_backup": state_backup,
        "memory_backup": memory_backup, "compatibility_backup": backup,
        "items_processed": 4,
    }


async def _v3_state_startup_checkpoint():
    result = await backup_state_db_to_github("startup_verified")
    if (
        _STATE_PERSISTENCE.configured
        and result.get("status") not in {"saved", "unchanged", "concurrent_update"}
    ):
        raise RuntimeError(f"state startup checkpoint failed: {result.get('status')}")
    return result


async def _v3_recover_deferred_state_checkpoint(
    compatibility_checkpoint, memory_checkpoint, *, attempts=12, delay_seconds=30,
):
    """Recover a rollout CAS collision without keeping the worker in a crash loop.

    A verified State restore has already completed.  New entries remain
    inhibited until the exact local State snapshot is durably checkpointed.
    Manager/reconciliation and Telegram can stay alive while GitHub finishes
    validating the competing backup-branch commit.
    """
    last_status = "concurrent_update"
    for attempt in range(max(1, int(attempts))):
        if attempt:
            await asyncio.sleep(max(1, int(delay_seconds)))
        result = await backup_state_db_to_github("startup_deferred_retry")
        last_status = str(result.get("status") or "failed")
        if last_status in {"saved", "unchanged", "not_configured"}:
            compatibility_ok = compatibility_checkpoint.get("status") in {
                "saved", "unchanged", "not_configured",
            }
            memory_ok = memory_checkpoint.get("status") in {
                "saved", "unchanged", "not_configured",
            }
            if compatibility_ok and memory_ok:
                _V3_RUNTIME.mark_component(
                    "backup", _V3_COMPONENT_STATE.READY,
                    f"brain={compatibility_checkpoint.get('status')} "
                    f"state={last_status} memory={memory_checkpoint.get('status')}",
                )
                _V3_RUNTIME.clear_inhibit("STATE_BACKUP_DEFERRED")
                _v3_recover_incident("STATE_BACKUP_DEFERRED", "backup")
                _V3_RUNTIME.evaluate_readiness()
                await _v3_runtime_watchdog()
                logging.warning("[StatePersistence] deferred startup checkpoint recovered")
                return result
        if last_status == "stale_remote":
            break
        logging.warning(
            "[StatePersistence] deferred checkpoint attempt %s/%s status=%s branch=%s error=%s",
            attempt + 1, attempts, last_status,
            result.get("branch") or _STATE_PERSISTENCE.branch,
            result.get("error") or "",
        )
    _v3_report_incident(
        "STATE_BACKUP_DEFERRED", "backup", "CRITICAL",
        {"status": last_status, "attempts": attempts},
    )
    return {"status": last_status, "recovered": False}


async def _v3_memory_startup_checkpoint():
    result = await backup_memory_db_to_github("startup_verified")
    if _MEMORY_PERSISTENCE.configured and result.get("status") not in {
        "saved", "unchanged",
    }:
        _V3_RUNTIME.mark_component(
            "memory_db", _V3_COMPONENT_STATE.DEGRADED,
            f"startup backup {result.get('status') or 'failed'}", required=False,
        )
    return result


async def _brain_rollout_settle():
    """Let the old Render instance finish its SIGTERM snapshot before restore."""
    seconds = _V3_CONFIG.runtime.rollout_settle_seconds
    if seconds:
        logging.warning(
            "[BrainPersistence] waiting %ss for previous instance final snapshot",
            seconds,
        )
        await asyncio.sleep(seconds)


async def _brain_startup_checkpoint():
    """Persist migrations and restart if the remote advanced during rollout."""
    result = await backup_db_to_github("startup_verified")
    if result.get("status") == "stale_remote":
        raise RuntimeError(
            "brain.db advanced during rollout; restarting to restore the newer generation"
        )
    return result


async def _v3_startup_reconcile_and_market_check():
    """Complete critical startup checks before opening the entry gate."""
    _V3_RUNTIME.transition(_V3_RUNTIME_STATUS.RECONCILING)
    execution_ok = bool(_TRADE_EXECUTION_OK)
    if execution_ok:
        try:
            _execution_config = _ExecutionConfig.from_env()
            _execution_client = None
            _account_detail = "execution disabled or paper"
            if _execution_config.enabled and _execution_config.mode == "live":
                if not _execution_config.live_armed:
                    raise RuntimeError("live execution is configured but not armed")
                _execution_client = _BinanceFuturesClient(_execution_config)
                if not await asyncio.to_thread(_execution_client.is_one_way_mode):
                    raise RuntimeError("Binance account must use One-way Mode")
                _balance = await asyncio.to_thread(_execution_client.usdt_balance_details)
                if "wallet_balance" not in _balance:
                    raise RuntimeError("Binance balance response has no wallet balance")
                _account_detail = "Binance account and balance endpoint verified"
            await asyncio.to_thread(
                _reconcile_live_executions,
                db_path=DB_PATH,
                config=_execution_config,
                client=_execution_client,
            )
            await asyncio.to_thread(_v3_sync_live_learning)
            _V3_RUNTIME.mark_component(
                "binance_reconciliation", _V3_COMPONENT_STATE.READY, _account_detail
            )
        except Exception as exc:
            execution_ok = False
            _V3_RUNTIME.mark_component(
                "binance_reconciliation", _V3_COMPONENT_STATE.FAILED, str(exc)
            )
    else:
        _V3_RUNTIME.mark_component(
            "binance_reconciliation", _V3_COMPONENT_STATE.FAILED,
            "trade execution module unavailable",
        )

    try:
        await asyncio.to_thread(_reconcile_manager_states_from_signals, DB_PATH)
        await asyncio.to_thread(_register_pending_manager_signals, DB_PATH)
        await asyncio.to_thread(_load_active_manager_states, DB_PATH)
        _V3_RUNTIME.mark_component("manager_reconciliation", _V3_COMPONENT_STATE.READY)
    except Exception as exc:
        _V3_RUNTIME.mark_component(
            "manager_reconciliation", _V3_COMPONENT_STATE.FAILED, str(exc)
        )

    try:
        pairs = await asyncio.wait_for(
            asyncio.to_thread(get_top_pairs, 1), timeout=30
        )
        if not pairs:
            raise RuntimeError("Gate returned no production symbols")
        _V3_RUNTIME.mark_component("gate", _V3_COMPONENT_STATE.FRESH)
        _V3_RUNTIME.mark_component(
            "market_data", _V3_COMPONENT_STATE.FRESH,
            f"Gate universe probe returned {len(pairs)} symbol(s)",
        )
        _v3_recover_incident("GATE_UNAVAILABLE", "gate")
    except Exception as exc:
        _V3_RUNTIME.mark_component("gate", _V3_COMPONENT_STATE.UNAVAILABLE, str(exc))
        _V3_RUNTIME.mark_component("market_data", _V3_COMPONENT_STATE.UNAVAILABLE, str(exc))
        _v3_report_incident(
            "GATE_UNAVAILABLE", "gate", "ERROR", {"error_type": type(exc).__name__}
        )
    return execution_ok


async def _v3_runtime_watchdog():
    """Resource pressure may stop entries, never Manager/reconciliation."""
    global _v3_memory_relief_active
    try:
        snapshot = await asyncio.to_thread(
            _v3_memory_snapshot,
            watch_ratio=_V3_CONFIG.operational.memory_watch_ratio,
            degraded_ratio=_V3_CONFIG.operational.memory_degraded_ratio,
            stop_ratio=_V3_CONFIG.operational.memory_stop_ratio,
            limit_bytes=_V3_CONFIG.operational.memory_limit_bytes,
        )
        detail = (
            f"rss={snapshot.rss_bytes} limit={snapshot.limit_bytes} "
            f"ratio={snapshot.ratio:.3f} state={snapshot.state}"
        )
        if snapshot.state in {"DEGRADED", "NEW_ENTRIES_OFF"}:
            if not _v3_memory_relief_active:
                cleared = clear_market_runtime_caches()
                trimmed = _v3_release_unused_memory()
                _v3_memory_relief_active = True
                logging.warning(
                    "[ResourceGuard] optional caches released=%s malloc_trim=%s",
                    cleared, trimmed,
                )
            _V3_RUNTIME.mark_component("memory", _V3_COMPONENT_STATE.DEGRADED, detail, required=False)
            _V3_RUNTIME.inhibit_entries(f"RESOURCE_MEMORY_{snapshot.state}")
            _v3_report_incident(
                "MEMORY_PRESSURE", "memory", "ERROR" if snapshot.state == "NEW_ENTRIES_OFF" else "WARNING",
                {"rss_bytes": snapshot.rss_bytes, "limit_bytes": snapshot.limit_bytes, "ratio": snapshot.ratio},
            )
        else:
            _v3_memory_relief_active = False
            _V3_RUNTIME.mark_component("memory", _V3_COMPONENT_STATE.READY, detail, required=False)
            _V3_RUNTIME.clear_inhibit("RESOURCE_MEMORY_DEGRADED")
            _V3_RUNTIME.clear_inhibit("RESOURCE_MEMORY_NEW_ENTRIES_OFF")
            _v3_recover_incident("MEMORY_PRESSURE", "memory")
        cpu = _V3_CPU_MONITOR.sample()
        _V3_RUNTIME.mark_component(
            "cpu",
            _V3_COMPONENT_STATE.DEGRADED if cpu.state == "DEGRADED" else _V3_COMPONENT_STATE.READY,
            f"ratio={cpu.ratio:.3f} state={cpu.state}",
            required=False,
        )
        if cpu.state == "DEGRADED":
            _v3_report_incident("CPU_PRESSURE", "cpu", "WARNING", {"ratio": cpu.ratio})
        else:
            _v3_recover_incident("CPU_PRESSURE", "cpu")
        lag = await _V3_LAG_MONITOR.sample()
        if lag.state == "DEGRADED":
            _V3_RUNTIME.mark_component(
                "event_loop", _V3_COMPONENT_STATE.DEGRADED,
                f"current_ms={lag.current_ms:.1f} p95_ms={lag.p95_ms:.1f}", required=False,
            )
            _V3_RUNTIME.inhibit_entries("EVENT_LOOP_DEGRADED")
            _v3_report_incident(
                "EVENT_LOOP_LAG", "event_loop", "ERROR",
                {"current_ms": lag.current_ms, "p95_ms": lag.p95_ms},
            )
        else:
            _V3_RUNTIME.mark_component(
                "event_loop", _V3_COMPONENT_STATE.READY,
                f"current_ms={lag.current_ms:.1f} p95_ms={lag.p95_ms:.1f}", required=False,
            )
            _V3_RUNTIME.clear_inhibit("EVENT_LOOP_DEGRADED")
            _v3_recover_incident("EVENT_LOOP_LAG", "event_loop")
        await _v3_refresh_runtime_lease()
        _V3_RUNTIME.evaluate_readiness()
        _v3_recover_incident("RUNTIME_WATCHDOG_FAILED", "runtime")
        public = _V3_RUNTIME.public_snapshot()
        # The unauthenticated worker endpoint returns only the 12-character
        # display SHA, but the persisted heartbeat must retain the full value
        # so a controlled release can prove exact commit identity.
        public["release_sha"] = _V3_RUNTIME.snapshot()["release_sha"]
        _emit_stats_event(
            "runtime_status", "SYSTEM", "", public,
            event_key=f"runtime-status:{_V3_CONFIG.runtime.instance_id}",
        )
        _emit_stats_event(
            "incident_snapshot", "SYSTEM", "",
            {
                "release_sha": _V3_RUNTIME.snapshot()["release_sha"],
                "instance_id": _V3_CONFIG.runtime.instance_id,
                "incidents": _v3_current_incidents(),
            },
            event_key=f"incident-snapshot:{_V3_CONFIG.runtime.instance_id}",
        )
    except Exception as exc:
        _V3_RUNTIME.mark_component("memory", _V3_COMPONENT_STATE.UNKNOWN, str(exc), required=False)
        _v3_report_incident(
            "RUNTIME_WATCHDOG_FAILED", "runtime", "ERROR", {"error_type": type(exc).__name__}
        )


def _v3_get_lease_client():
    global _V3_LEASE_CLIENT
    if _V3_LEASE_CLIENT is None:
        runtime = _V3_RUNTIME.snapshot()
        _V3_LEASE_CLIENT = _V3InstanceLeaseClient(
            _v3_derive_lease_url(
                _V3_CONFIG.integrations.runtime_lease_url,
                _V3_CONFIG.integrations.stats_ingest_url,
            ),
            _V3_CONFIG.integrations.stats_ingest_token,
            str(runtime.get("instance_id") or ""),
            str(runtime.get("release_sha") or ""),
            ttl_seconds=_V3_CONFIG.operational.runtime_lease_ttl_seconds,
        )
    return _V3_LEASE_CLIENT


async def _v3_refresh_runtime_lease(*, acquire: bool = False) -> bool:
    """Acquire/renew shared fencing; transient errors respect current TTL."""
    client = _v3_get_lease_client()
    try:
        state = await asyncio.to_thread(client.acquire if acquire else client.renew)
    except Exception as exc:
        current = client.state
        if current.valid_at():
            _V3_RUNTIME.mark_component(
                "instance_fencing", _V3_COMPONENT_STATE.READY,
                f"renew_error={type(exc).__name__}; generation={current.generation}",
            )
            return True
        _V3_RUNTIME.clear_instance_lease()
        _V3_RUNTIME.mark_component("instance_fencing", _V3_COMPONENT_STATE.UNAVAILABLE, str(exc))
        _V3_RUNTIME.inhibit_entries("INSTANCE_LEASE_UNAVAILABLE")
        return False
    if state.granted and state.valid_at():
        _V3_RUNTIME.set_instance_lease(int(state.generation), str(state.expires_at))
        _V3_RUNTIME.mark_component(
            "instance_fencing", _V3_COMPONENT_STATE.READY,
            f"generation={state.generation}; expires_at={state.expires_at}",
        )
        _V3_RUNTIME.clear_inhibit("INSTANCE_LEASE_UNAVAILABLE")
        _V3_RUNTIME.clear_inhibit("INSTANCE_LEASE_HELD")
        return True
    _V3_RUNTIME.mark_component(
        "instance_fencing", _V3_COMPONENT_STATE.UNAVAILABLE,
        f"reason={state.reason}; generation={state.generation}",
    )
    _V3_RUNTIME.clear_instance_lease()
    _V3_RUNTIME.inhibit_entries("INSTANCE_LEASE_HELD")
    return False


async def _v3_release_runtime_lease() -> None:
    client = _V3_LEASE_CLIENT
    if client is None:
        return
    try:
        await asyncio.wait_for(asyncio.to_thread(client.release), timeout=5)
    except Exception as exc:
        logging.warning("[APEX V3] runtime lease release failed safely: %s", exc)
    finally:
        _V3_RUNTIME.clear_instance_lease()


async def keepalive_heartbeat():
    """Каждые 10 минут — не даёт Render усыплять сервис"""
    try:
        await asyncio.to_thread(
            _V3RuntimeRepository(lambda: _v3_connect_state(_V3_CONFIG)).heartbeat,
            _V3_CONFIG.runtime.instance_id,
            _V3_CONFIG.runtime.release_sha,
        )
    except Exception as e:
        logging.error(f"Heartbeat: {e}")

async def market_intelligence_job():
    if not _MARKET_INTELLIGENCE_OK:return
    async def refresh():
        pairs = await asyncio.to_thread(get_top_pairs, DEFAULT_UNIVERSE_SIZE)
        intelligence = await _refresh_market_intelligence(pairs, get_candles)
        btc_regime = await asyncio.to_thread(get_market_regime, "BTCUSDT")
        btc_regime = btc_regime if isinstance(btc_regime, dict) else {}
        await asyncio.to_thread(
            _store_apex_market_state,
            {
                "snapshot_key": f"market-intelligence:{datetime.utcnow().strftime('%Y%m%d%H')}",
                "regime": btc_regime.get("mode") or "UNKNOWN",
                "btc_direction": btc_regime.get("direction") or "UNKNOWN",
                "volatility": btc_regime.get("mode") or "UNKNOWN",
                "data_quality": {"state": "FRESH", "source": "Gate"},
                "coverage": intelligence.get("coverage") if isinstance(intelligence, dict) else {},
            },
            DB_PATH,
        )
        await asyncio.to_thread(_emit_apex_v2_dashboard_snapshot, DB_PATH)
    try:
        await _run_market_scan_exclusive("market_intelligence", refresh, 180)
    except Exception as exc:logging.warning("[MarketIntelligence] refresh failed safely: %s",exc)


async def _start_market_intelligence_background():
    """Load the Gate universe once without blocking the scheduler event loop."""
    async def start():
        pairs = await asyncio.to_thread(get_top_pairs, DEFAULT_UNIVERSE_SIZE)
        if _MARKET_INTELLIGENCE_OK:
            await _start_market_intelligence(pairs, get_candles)
    try:
        await _run_market_scan_exclusive("market_intelligence_startup", start, 240)
    except Exception as exc:
        logging.warning("[MarketIntelligence] startup failed safely: %s", exc)


async def _v3_alerts_job():
    """Send durable incident transitions once, alongside legacy price alerts."""
    alert_error = None
    try:
        await check_alerts()
    except Exception as exc:
        # Operational incident delivery must not be skipped just because a
        # legacy price-alert query failed during the same scheduler cycle.
        alert_error = exc
        logging.warning("[Alerts] price alerts failed; delivering incidents: %s", exc)
    notifications = await asyncio.to_thread(_v3_pending_incident_notifications, 20)
    recipients = sorted({int(value) for value in (ADMIN_IDS or []) if value})
    if not recipients and ADMIN_ID:
        recipients = [int(ADMIN_ID)]
    for notification in notifications:
        payload = notification.get("payload") or {}
        event_type = str(notification.get("event_type") or "INCIDENT")
        icon = "✅" if event_type == "RESOLVED" else "🚨"
        details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
        detail_text = ", ".join(f"{key}={value}" for key, value in sorted(details.items()))[:500]
        text = (
            f"{icon} APEX INCIDENT · {event_type}\n"
            f"{payload.get('severity', 'UNKNOWN')} · {payload.get('component', 'system')}\n"
            f"{payload.get('code', 'UNKNOWN')}"
        )
        if detail_text:
            text += f"\n{detail_text}"
        delivered = False
        for recipient in recipients:
            delivered = bool(await _send_with_retry(recipient, text)) or delivered
        if delivered:
            await asyncio.to_thread(
                _v3_mark_incident_delivered, int(notification["notification_id"])
            )
    if alert_error is not None:
        raise alert_error


def _build_v3_scheduler():
    """Build the only production scheduler; Telegram transport is irrelevant."""
    return _v3_build_production_scheduler(
        _V3SchedulerCallbacks(
            signal_outcome_refresh=auto_scan_job,
            execution_reconcile=auto_trade_reconcile_job,
            trade_manager=trade_manager_job,
            market_intelligence_primary=market_intelligence_job,
            market_fast=auto_fast_deal_scan,
            market_mtf_1h=auto_scan_1h,
            market_zone=auto_zone_scan,
            market_swing=auto_scan_swing,
            market_wyckoff=auto_wyckoff_scan,
            market_ltf_watch=auto_ltf_watch_scan,
            keepalive=keepalive_heartbeat,
            dashboard_telemetry=functools.partial(_emit_apex_v2_dashboard_snapshot, DB_PATH),
            alerts=_v3_alerts_job,
            state_backup=functools.partial(_v3_maintenance_and_backup, "safety_30m"),
            runtime_watchdog=_v3_runtime_watchdog,
        ),
        execution_reconcile_seconds=_auto_trade_reconcile_seconds(),
    )


async def _warmup_market_cache() -> None:
    try:
        logging.info("[Cache] Прогрев кеша...")
        top = await asyncio.to_thread(get_top_pairs, 20)
        candles_map = await fetch_candles_batch(top, "4h", 100)
        for symbol, candles in candles_map.items():
            if candles:
                get_precomputed_indicators(symbol, "4h")
            await asyncio.sleep(0.05)
        logging.info("[Cache] Прогрев завершён: %s пар", len(candles_map))
    except Exception as exc:
        logging.warning("[Cache] Ошибка прогрева: %s", exc)


async def _delete_webhook_safely() -> None:
    for attempt, delay in enumerate((0, 2, 4, 8, 12), start=1):
        if delay:
            await asyncio.sleep(delay)
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logging.info("Webhook удалён")
            return
        except Exception as exc:
            logging.warning("delete_webhook попытка %s/5: %s", attempt, exc)
    raise RuntimeError("telegram_webhook_delete_failed")


async def _initialize_production_runtime(transport: str):
    """Run the one production bootstrap for webhook and polling transports."""
    _V3_RUNTIME.activate(
        release_sha=_V3_CONFIG.runtime.release_sha,
        instance_id=_V3_CONFIG.runtime.instance_id,
    )
    _config_validation = _v3_validate_config(_V3_CONFIG)
    _V3_RUNTIME.mark_component(
        "config",
        _V3_COMPONENT_STATE.READY if _config_validation.valid else _V3_COMPONENT_STATE.FAILED,
        ";".join(_config_validation.errors),
    )
    if not _config_validation.valid:
        _V3_RUNTIME.inhibit_entries("CONFIG_INVALID")
    _V3_RUNTIME.mark_component("telegram", _V3_COMPONENT_STATE.STARTING)
    await _brain_rollout_settle()
    _restore_result = await asyncio.wait_for(restore_db_from_github(), timeout=180)
    if _BRAIN_PERSISTENCE.configured and not _restore_result.get("ready"):
        raise RuntimeError("verified brain.db restore is required before APEX startup")
    init_db()                        # потом применяем миграции к восстановленной БД
    _state_restore = await restore_state_db_from_github()
    if _STATE_PERSISTENCE.configured and not _state_restore.get("ready"):
        raise RuntimeError("verified apex_state.db restore is required before APEX startup")
    _memory_restore = await restore_memory_db_from_github()
    _v3_db_status = await asyncio.to_thread(_v3_prepare_databases)
    _V3_RUNTIME.mark_component("state_db", _V3_COMPONENT_STATE.READY, json.dumps(_v3_db_status))
    _v3_publish_strategy_activation_health()
    _memory_ready = (
        not _MEMORY_PERSISTENCE.configured or bool(_memory_restore.get("ready"))
    )
    _V3_RUNTIME.mark_component(
        "memory_db",
        _V3_COMPONENT_STATE.READY if _memory_ready else _V3_COMPONENT_STATE.DEGRADED,
        "live-only memory restored and migrated" if _memory_ready else (
            "live memory restore unavailable; deterministic trading unaffected"
        ),
        required=False,
    )
    _restart = await asyncio.to_thread(
        _v3_record_start, _V3_CONFIG.database.state_db_path,
        instance_id=_V3_CONFIG.runtime.instance_id,
        release_sha=_V3_CONFIG.runtime.release_sha,
    )
    _V3_RUNTIME.mark_component("restart_guard", _V3_COMPONENT_STATE.READY, json.dumps(_restart), required=False)
    if _restart.get("restart_count_1h", 0) >= _V3_CONFIG.operational.restart_limit_1h:
        _V3_RUNTIME.inhibit_entries("WORKER_RESTART_LOOP")
        _v3_report_incident("WORKER_RESTART_LOOP", "worker", "CRITICAL", _restart)
    else:
        _v3_recover_incident("WORKER_RESTART_LOOP", "worker")
    await _v3_refresh_runtime_lease(acquire=True)
    _ensure_control_schema(DB_PATH)
    _ensure_setup_evidence_schema(DB_PATH)
    _ensure_apex_v2_schema(DB_PATH)
    _lifecycle_import = await _v3_refresh_signal_lifecycle_mirror()
    logging.info("[APEX V3] Signal lifecycle State mirror: %s", _lifecycle_import)
    if _TRADE_EXECUTION_OK:
        _execution_import = await _v3_refresh_execution_state_mirror()
        logging.info("[APEX V3] Execution State mirror: %s", _execution_import)
        _ledger_import = await _v3_refresh_execution_ledger_mirror()
        logging.info("[APEX V3] Execution ledger State mirror: %s", _ledger_import)
    _manager_import = await _v3_refresh_manager_state_mirror()
    logging.info("[APEX V3] Manager State mirror: %s", _manager_import)
    _register_pending_manager_signals(DB_PATH)
    _manager_registration = await _v3_refresh_manager_state_mirror()
    logging.info("[APEX V3] Manager registration mirror: %s", _manager_registration)
    _rebuild_strategy_risk_states(DB_PATH)
    _emit_apex_v2_dashboard_snapshot(DB_PATH)
    _V3_RUNTIME.mark_component(
        "dashboard_telemetry", _V3_COMPONENT_STATE.READY,
        "startup production snapshot emitted", required=False,
    )
    start_db_writer()
    _checkpoint = await _brain_startup_checkpoint()
    _state_checkpoint = await _v3_state_startup_checkpoint()
    _memory_checkpoint = await _v3_memory_startup_checkpoint()
    _state_checkpoint_deferred = _state_checkpoint.get("status") == "concurrent_update"
    if _state_checkpoint_deferred:
        _V3_RUNTIME.inhibit_entries("STATE_BACKUP_DEFERRED")
        _v3_report_incident(
            "STATE_BACKUP_DEFERRED", "backup", "ERROR",
            {"status": "concurrent_update", "phase": "startup"},
        )
    _backup_ready = (
        _checkpoint.get("status") in {"saved", "unchanged", "not_configured"}
        and _state_checkpoint.get("status") in {"saved", "unchanged", "not_configured"}
        and _memory_checkpoint.get("status") in {"saved", "unchanged", "not_configured"}
    )
    _V3_RUNTIME.mark_component(
        "backup",
        _V3_COMPONENT_STATE.READY if _backup_ready else _V3_COMPONENT_STATE.DEGRADED,
        f"brain={_checkpoint.get('status')} state={_state_checkpoint.get('status')} "
        f"memory={_memory_checkpoint.get('status')}",
    )
    if _backup_ready:
        _v3_recover_incident("JOB_FAILED", "backup")
        _v3_recover_incident("JOB_TIMEOUT", "backup")
    await _v3_startup_reconcile_and_market_check()
    if transport == "polling":
        threading.Thread(target=run_server, daemon=True).start()
    asyncio.create_task(_start_market_intelligence_background())

    webhook_url = _V3_CONFIG.integrations.webhook_url
    if transport == "webhook":
        await bot.set_webhook(f"{webhook_url}/webhook", drop_pending_updates=True)
        logging.info("Webhook установлен: %s/webhook", webhook_url)
    else:
        await _delete_webhook_safely()
    _V3_RUNTIME.mark_component("telegram", _V3_COMPONENT_STATE.READY)

    scheduler = _build_v3_scheduler()
    scheduler.start()
    _V3_RUNTIME.mark_component("scheduler", _V3_COMPONENT_STATE.READY)
    _V3_RUNTIME.mark_component(
        "worker", _V3_COMPONENT_STATE.READY,
        f"production runtime started transport={transport}", required=False,
    )
    _V3_RUNTIME.evaluate_readiness()
    if _state_checkpoint_deferred:
        asyncio.create_task(
            _v3_recover_deferred_state_checkpoint(_checkpoint, _memory_checkpoint)
        )
    logging.warning("[APEX V3] runtime=%s", _V3_RUNTIME.snapshot())
    asyncio.create_task(_warmup_market_cache())
    logging.info("APEX запущен (%s mode)", transport)
    return scheduler


async def _shutdown_production_runtime(reason: str) -> None:
    _V3_RUNTIME.inhibit_entries("GRACEFUL_SHUTDOWN")
    await _v3_release_runtime_lease()
    try:
        await asyncio.to_thread(
            _v3_record_shutdown, _V3_CONFIG.database.state_db_path, reason,
            instance_id=_V3_CONFIG.runtime.instance_id,
        )
    except Exception as exc:
        logging.warning("[APEX V3] shutdown marker failed safely: %s", exc)
    try:
        await asyncio.wait_for(
            _v3_maintenance_and_backup("render_sigterm"), timeout=30
        )
    except asyncio.TimeoutError:
        logging.warning("[BrainPersistence] final SIGTERM snapshot timed out safely")
    except Exception as exc:
        logging.warning("[BrainPersistence] final SIGTERM snapshot failed safely: %s", exc)
    if _MARKET_INTELLIGENCE_OK:
        try:await _stop_market_intelligence()
        except Exception:pass


def _v3_token_snapshot():
    tokens_used = groq_tokens_used()
    token_pct = round(tokens_used / _GROQ_DAILY_LIMIT * 100) if _GROQ_DAILY_LIMIT > 0 else 0
    return {
        "tokens_used": tokens_used,
        "tokens_limit": _GROQ_DAILY_LIMIT,
        "percent": token_pct,
        "available": _tokens_available(),
    }


def main():
    _v3_run_production(_V3_PRODUCTION_DEPENDENCIES(
        config=_V3_CONFIG,
        runtime=_V3_RUNTIME,
        telegram_bot=bot,
        dispatcher=dp,
        update_type=types.Update,
        web=web,
        initialize=_initialize_production_runtime,
        shutdown=_shutdown_production_runtime,
        token_snapshot=_v3_token_snapshot,
    ))


if __name__ == "__main__":
    main()
