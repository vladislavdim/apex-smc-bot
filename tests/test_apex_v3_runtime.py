import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from apex.app.job_registry import PRODUCTION_JOBS, validate_registry
from apex.app.runtime import RuntimeSupervisor, runtime_supervisor
from apex.domain.enums import ComponentState, Direction, RuntimeStatus, SourceMode, Strategy
from apex.market.source_registry import SourcePolicyError, authorize, registry_snapshot
from apex.market.time_estimate import get_estimated_time
from apex.ops.restart_guard import record_shutdown, record_start
from apex.strategies.data_contracts import CONTRACTS, Relevance, contract_for
from apex.strategies.specifications import SPECIFICATIONS
from apex.ui.price_format import smart_price_fmt
from apex.ui.market_format import format_accumulation, format_market_prices, format_news
from apex.execution.orders import ExecutionConfig, execute_approved_candidate


CANDIDATE = {
    "symbol": "BTCUSDT",
    "direction": "BULLISH",
    "entry": 100,
    "sl": 95,
    "tp1": 110,
    "tp2": 115,
    "rr": 2,
    "_external_quality_reviewed": True,
    "_external_quality_review": {"decision": "APPROVE", "confidence": 0.9, "degraded": False},
}


class NeverCalledClient:
    def __getattr__(self, name):
        raise AssertionError(f"Binance client must not be called before READY: {name}")


class ApexV3RuntimeTests(unittest.TestCase):
    def tearDown(self):
        runtime_supervisor.deactivate()

    def test_direction_boundary_normalization(self):
        self.assertIs(Direction.normalize("BULLISH"), Direction.LONG)
        self.assertIs(Direction.normalize("sell"), Direction.SHORT)

    def test_price_format_is_presentation_only_and_scale_aware(self):
        self.assertEqual(smart_price_fmt(None), "нет данных")
        self.assertEqual(smart_price_fmt(12_345), "12,345.00")
        self.assertEqual(smart_price_fmt(0.01234567), "0.012346")

    def test_market_card_formatters_are_owned_outside_market_monolith(self):
        self.assertEqual(format_market_prices({}), "Данные недоступны")
        self.assertEqual(
            format_market_prices({
                "BTCUSDT": {"price": 12_345.0, "change": 1.25},
                "XRPUSDT": {"price": 0.5123456, "change": -2.5},
            }),
            "🟢 BTC: $12,345.00 (+1.25%)\n🔴 XRP: $0.512346 (-2.50%)",
        )
        self.assertEqual(format_news([]), "Новости временно недоступны")
        self.assertIn("CoinDesk", format_news([{
            "date": "2026-09-20", "title": "Market", "source": "CoinDesk",
        }]))
        card = format_accumulation({
            "score": 60, "symbol": "BTCUSDT", "signals": ["volume"],
            "price": 100.0, "low_min": 95.0, "high_max": 105.0,
        })
        self.assertIn("СИЛЬНОЕ НАКОПЛЕНИЕ", card)
        self.assertIn("BTCUSDT", card)

    def test_display_horizon_never_uses_virtual_performance(self):
        self.assertEqual(get_estimated_time("BTCUSDT", "4h"), (48, "нет live-выборки", 0))
        self.assertEqual(get_estimated_time("BTCUSDT", "unknown"), (24, "нет live-выборки", 0))

    def test_runtime_needs_every_required_component(self):
        supervisor = RuntimeSupervisor()
        supervisor.activate(release_sha="abc", instance_id="one")
        self.assertFalse(supervisor.evaluate_readiness())
        self.assertFalse(supervisor.allows_new_entries)
        for component in supervisor.REQUIRED_COMPONENTS:
            supervisor.mark_component(component, ComponentState.READY)
        supervisor.set_instance_lease(
            1, (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        )
        self.assertTrue(supervisor.evaluate_readiness())
        self.assertTrue(supervisor.allows_new_entries)
        self.assertEqual(supervisor.snapshot()["status"], RuntimeStatus.READY.value)
        self.assertEqual(supervisor.snapshot()["health"], "HEALTHY")
        self.assertEqual(set(supervisor.snapshot()["components"]), supervisor.COMPONENTS)
        supervisor.mark_component("binance_reconciliation", ComponentState.READY, "private detail")
        self.assertNotIn("detail", supervisor.public_snapshot()["components"]["binance_reconciliation"])

    def test_health_is_distinct_from_entry_readiness(self):
        supervisor = RuntimeSupervisor()
        supervisor.activate()
        for component in supervisor.REQUIRED_COMPONENTS:
            supervisor.mark_component(component, ComponentState.READY)
        supervisor.mark_component("groq", ComponentState.DEGRADED, required=False)
        supervisor.set_instance_lease(
            1, (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        )
        self.assertTrue(supervisor.evaluate_readiness())
        self.assertTrue(supervisor.allows_new_entries)
        self.assertEqual(supervisor.public_snapshot()["health"], "DEGRADED")

    def test_runtime_inhibit_is_fail_closed(self):
        supervisor = RuntimeSupervisor()
        supervisor.activate()
        for component in supervisor.REQUIRED_COMPONENTS:
            supervisor.mark_component(component, ComponentState.READY)
        supervisor.set_instance_lease(
            1, (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        )
        self.assertTrue(supervisor.evaluate_readiness())
        supervisor.inhibit_entries("WORKER_RESTART_LOOP")
        self.assertFalse(supervisor.allows_new_entries)
        self.assertEqual(supervisor.snapshot()["new_entries"], "OFF")

    def test_execution_is_blocked_before_any_binance_call(self):
        runtime_supervisor.activate()
        with tempfile.TemporaryDirectory() as folder:
            result = execute_approved_candidate(
                CANDIDATE,
                7001,
                db_path=os.path.join(folder, "state.db"),
                config=ExecutionConfig(enabled=True, mode="live"),
                client=NeverCalledClient(),
            )
        self.assertEqual(result["status"], "BLOCKED_RUNTIME_NOT_READY")

    def test_restart_history_and_shutdown_marker_are_persistent(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "state.db")
            first = record_start(path, instance_id="a", release_sha="one")
            record_shutdown(path, "SIGTERM", instance_id="a")
            second = record_start(path, instance_id="b", release_sha="two")
        self.assertEqual(first["restart_count_1h"], 1)
        self.assertEqual(second["previous_instance"], "a")
        self.assertEqual(second["previous_shutdown_reason"], "SIGTERM")
        self.assertEqual(second["restart_count_1h"], 2)

    def test_worker_restart_guard_uses_durable_v3_state(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        with open(os.path.join(root, "apex", "app", "cutover.py"), encoding="utf-8") as source:
            cutover_source = source.read()
        self.assertNotIn("_v3_record_start, DB_PATH", bot_source)
        self.assertNotIn("_v3_record_shutdown, DB_PATH", bot_source)
        self.assertIn("restore_state_db_from_github()", bot_source)
        self.assertIn("_v3_state_startup_checkpoint()", bot_source)
        self.assertIn("_v3_recover_deferred_state_checkpoint", bot_source)
        self.assertIn('inhibit_entries("STATE_BACKUP_DEFERRED")', bot_source)
        self.assertIn('clear_inhibit("STATE_BACKUP_DEFERRED")', bot_source)
        self.assertIn("restore_memory_db_from_github()", bot_source)
        self.assertIn("_v3_memory_startup_checkpoint()", bot_source)
        self.assertIn("_v3_sync_manager_state()", bot_source)
        self.assertIn("refresh=True", cutover_source)
        self.assertIn("return await _v3_refresh_cutover(", bot_source)
        self.assertIn("STATE_DB_MANAGER_MIRROR_FAILED", bot_source)
        self.assertIn("_v3_sync_execution_state()", bot_source)
        self.assertIn('parity_error="execution_state_parity_failed"', bot_source)
        self.assertIn('spec.parity_error + ":"', cutover_source)
        self.assertIn("STATE_DB_EXECUTION_MIRROR_FAILED", bot_source)
        self.assertIn("await _v3_refresh_execution_state_mirror()", bot_source)
        self.assertIn("repository.mark_exchange_closed(signal_id, accounting", bot_source)
        self.assertIn("repository.close_from_accounting(signal_id, accounting", bot_source)
        self.assertIn("analytical result=%s awaits Binance reconciliation", bot_source)
        self.assertNotIn("finalize_manager_trade as _finalize_manager_trade", bot_source)
        self.assertNotIn("_finalize_manager_trade,", bot_source)
        self.assertIn("final card awaits ledger accounting", bot_source)
        self.assertIn("_v3_sync_signal_lifecycle()", bot_source)
        self.assertIn('parity_error="signal_lifecycle_parity_failed"', bot_source)
        self.assertIn("STATE_DB_SIGNAL_LIFECYCLE_FAILED", bot_source)

    def test_worker_compatibility_db_path_comes_from_typed_config(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        with open(os.path.join(root, "apex", "compatibility", "legacy_market_runtime.py"), encoding="utf-8") as source:
            market_source = source.read()
        self.assertIn("DB_PATH = _V3_CONFIG.database.compatibility_db_path", bot_source)
        self.assertIn("DB_PATH = _APEX_CONFIG.database.compatibility_db_path", market_source)
        self.assertNotIn("DB_PATH = _os_bot.path.join", bot_source)
        self.assertNotIn('_v3_connect_compatibility("brain.db"', bot_source)
        self.assertNotIn('_connect_compatibility_db("brain.db"', market_source)

    def test_state_projections_are_ready_before_manager_registration_and_reconcile(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        startup = bot_source[
            bot_source.index("async def _initialize_production_runtime"):
            bot_source.index("async def _shutdown_production_runtime")
        ]
        self.assertLess(
            startup.index("_v3_refresh_signal_lifecycle_mirror"),
            startup.index("_v3_refresh_execution_state_mirror"),
        )
        self.assertLess(
            startup.index("_v3_refresh_execution_state_mirror"),
            startup.index("_v3_refresh_manager_state_mirror"),
        )
        self.assertLess(
            startup.index("_v3_refresh_manager_state_mirror"),
            startup.index("_register_pending_manager_signals"),
        )
        manager_cycle = bot_source[
            bot_source.index("async def _run_trade_manager_once"):
            bot_source.index("async def trade_manager_job")
        ]
        self.assertLess(
            manager_cycle.index("_v3_refresh_signal_lifecycle_mirror"),
            manager_cycle.index("_reconcile_manager_states_from_signals"),
        )
        self.assertLess(
            manager_cycle.index("_v3_refresh_execution_state_mirror"),
            manager_cycle.index("_reconcile_manager_states_from_signals"),
        )

    def test_registry_contains_production_jobs_only(self):
        validate_registry()
        forbidden = ("shadow", "research", "replay", "backtest")
        self.assertTrue(PRODUCTION_JOBS)
        self.assertFalse(any(word in job.id.lower() for job in PRODUCTION_JOBS for word in forbidden))

    def test_legacy_scheduler_no_longer_runs_non_production_learning(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        forbidden_registrations = (
            "add_job(shadow_experience_job",
            "add_job(auto_research",
            "add_job(autonomous_learning_cycle",
            "add_job(night_brain_tasks",
            "add_job(_run_strategy_update",
            "add_job(_run_self_improve",
            "add_job(_run_autopilot_fast",
            "add_job(_run_autopilot_deep",
        )
        for registration in forbidden_registrations:
            self.assertNotIn(registration, bot_source)
        self.assertNotIn("AsyncIOScheduler(", bot_source)
        self.assertEqual(bot_source.count("= _build_v3_scheduler()"), 1)
        self.assertIn("_v3_run_production(_V3_PRODUCTION_DEPENDENCIES(", bot_source)
        self.assertIn("initialize=_initialize_production_runtime", bot_source)
        self.assertIn("shutdown=_shutdown_production_runtime", bot_source)
        with open(os.path.join(root, "apex", "app", "bootstrap.py"), encoding="utf-8") as source:
            bootstrap_source = source.read()
        self.assertIn('await deps.initialize("webhook")', bootstrap_source)
        self.assertIn('await deps.initialize("polling")', bootstrap_source)

    def test_unregistered_legacy_timing_queue_runtime_is_removed(self):
        sources = [
            Path("apex/app/worker.py").read_text(encoding="utf-8"),
            Path("apex", "compatibility", "legacy_market_runtime.py").read_text(encoding="utf-8"),
            Path("apex", "compatibility", "market_strategy.py").read_text(
                encoding="utf-8"
            ),
        ]
        for source in sources:
            self.assertNotIn("recheck_timing_queue", source)
            self.assertNotIn("save_to_timing_queue", source)
            self.assertNotIn("def get_timing_queue(", source)
            self.assertNotIn("remove_from_timing_queue", source)
            self.assertNotIn("expire_timing_queue", source)

    def test_legacy_research_shadow_and_backtest_are_not_request_reachable(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        with open(os.path.join(root, "apex/ui/dashboard/server.py"), encoding="utf-8") as source:
            dashboard_source = source.read()
        for forbidden in (
            'Command("backtest")', 'Command("think")', 'callback_data="menu_experience"',
            'data == "menu_experience"', 'data == "menu_backtest"',
            'state.get("action") == "backtest"', "_active_experience_rules(",
            "_capture_experience_candidate(", "_record_experience_decision(",
            "_market_relevant_rules(",
        ):
            self.assertNotIn(forbidden, bot_source)
        self.assertNotIn('p.path=="/api/research"', dashboard_source)
        self.assertNotIn("from research", dashboard_source)
        research_dir = os.path.join(root, "research")
        self.assertFalse(
            os.path.isdir(research_dir) and any(name.endswith(".py") for name in os.listdir(research_dir)),
            "research package",
        )
        for removed in (
            "research_worker.py", "core/replay_lab.py",
            "core/shadow_evidence.py", "core/experience_memory.py",
            "core/execution_simulator.py", "core/runtime_observability.py",
            "core/runtime_observability_overrides.py", "core/runtime_observability_fixups.py",
            ".github/workflows/patch-trade-manager-button.yml",
        ):
            self.assertFalse(os.path.exists(os.path.join(root, removed)), removed)
        self.assertFalse(os.path.exists(os.path.join(root, ".github/workflows/btc-research.yml")))
        for forbidden in ("Research", "Shadow", "Replay", "Counterfactual", "/api/research"):
            self.assertNotIn(forbidden, dashboard_source)
        self.assertIn('e["kind"]=="incident_snapshot"', dashboard_source)
        self.assertIn("normalize_incident_snapshot", dashboard_source)

    def test_production_dashboard_is_compact_and_contains_no_virtual_ui(self):
        from apex.ui.dashboard.page import HTML

        self.assertLess(len(HTML.encode("utf-8")), 30_000)
        self.assertIn("APEX V3 · Production", HTML)
        self.assertIn("LIVE_CONTEXT", HTML)
        for tab in (
            "overview", "strategies", "trades", "manager",
            "executionView", "market", "learning", "health",
        ):
            self.assertIn(f'data-view="{tab}"', HTML)
        self.assertIn("Только confirmed fills", HTML)
        self.assertIn("Authority: ADVISORY", HTML)
        for forbidden in ("Research", "Shadow", "Replay", "Counterfactual", "/api/research"):
            self.assertNotIn(forbidden, HTML)

    def test_manager_and_execution_have_no_runtime_replay_dependency(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/execution/ledger.py"), encoding="utf-8") as source:
            self.assertNotIn("from core.replay_lab import", source.read())
        with open(os.path.join(root, "apex/manager/engine.py"), encoding="utf-8") as source:
            manager = source.read()
        runtime_section = manager[manager.index("def manager_cycle("):]
        self.assertNotIn("def replay_closed_candle(", manager)
        self.assertNotIn("trade_manager_replay_tracks", manager)
        self.assertNotIn("trade_manager_replay_events", manager)
        self.assertNotIn("trade_manager_shadow_stats", manager)
        self.assertNotIn("from core.replay_lab import", manager)
        self.assertNotIn("replay_closed_candle(", runtime_section)
        self.assertNotIn("record_management_review", runtime_section)

    def test_fast_scheduler_uses_the_single_v3_strategy_registry(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        self.assertNotIn("asyncio.to_thread(detect_fast_deal, symbol)", bot_source)
        self.assertIn('asyncio.to_thread(_v3_strategy_candidate, "FAST", symbol)', bot_source)
        self.assertIn('candidate["_v3_strategy_trace"] = _v3_strategy_trace_payload(trace)', bot_source)
        self.assertIn("activation = _get_v3_strategy_activation()", bot_source)
        self.assertIn("_get_v3_strategy_snapshot_provider()", bot_source)
        self.assertIn("_v3_publish_strategy_activation_health()", bot_source)
        self.assertIn('"SNAPSHOT_STRATEGY_PROOF_INVALID"', bot_source)
        self.assertIn('"SNAPSHOT_STRATEGY_DATA_NOT_READY"', bot_source)

    def test_every_mtf_entry_path_uses_the_single_v3_registry(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        self.assertNotIn("register_raw_scan_handler(full_scan_raw)", bot_source)
        self.assertNotIn("sig_data = full_scan_raw(symbol, timeframe, auto=True)", bot_source)
        self.assertNotIn('return full_scan_raw(symbol, "1h")', bot_source)
        self.assertIn("register_raw_scan_handler(_canonical_mtf_scan_handler)", bot_source)
        self.assertIn("_v3_snapshot_symbol_detector(detect_fast_deal)", bot_source)
        self.assertIn("_v3_snapshot_symbol_detector(full_scan_raw)", bot_source)
        self.assertIn("_v3_snapshot_symbol_detector(detect_swing_setup)", bot_source)
        self.assertIn("_v3_snapshot_symbol_detector(detect_zone_setup)", bot_source)
        self.assertIn('_v3_strategy_candidate, "MTF", symbol', bot_source)

    def test_swing_and_zone_schedulers_use_the_single_v3_registry(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        self.assertNotIn("asyncio.to_thread(detect_swing_setup, symbol", bot_source)
        self.assertNotIn("asyncio.to_thread(detect_zone_setup, symbol", bot_source)
        self.assertIn('_v3_strategy_candidate, "SWING", symbol', bot_source)
        self.assertGreaterEqual(bot_source.count('_v3_strategy_candidate, "ZONE", symbol'), 2)

    def test_wyckoff_runs_all_subtypes_through_the_registry(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        self.assertIn("_v3_snapshot_symbol_detector(detect_wyckoff_spring)", bot_source)
        self.assertIn("_v3_snapshot_symbol_detector(detect_wyckoff_distribution)", bot_source)
        self.assertIn("_v3_snapshot_symbol_detector(detect_wyckoff_reaccumulation)", bot_source)
        self.assertNotIn("asyncio.to_thread(detect_wyckoff_spring, symbol)", bot_source)
        self.assertNotIn("asyncio.to_thread(detect_wyckoff_distribution, symbol)", bot_source)
        self.assertNotIn("asyncio.to_thread(detect_wyckoff_reaccumulation, symbol)", bot_source)
        self.assertIn('_v3_strategy_candidates, "WYCKOFF", symbol', bot_source)

    def test_v3_source_registry_has_no_shadow_mode(self):
        rows = registry_snapshot()
        self.assertTrue(authorize("gate", "candles"))
        self.assertTrue(authorize("binance", "execution"))
        self.assertTrue(authorize("gate_ws", "context"))
        self.assertNotIn("SHADOW", {row["mode"] for row in rows})
        root = os.path.dirname(os.path.dirname(__file__))
        for relative in ("apex/market/source_registry.py", "external_sources/coinalyze.py",
                         "external_sources/gate_microstructure.py", "external_sources/storage.py"):
            with open(os.path.join(root, relative), encoding="utf-8") as source:
                self.assertNotIn("SHADOW_CONTEXT", source.read(), relative)
        self.assertIn(SourceMode.PROXY.value, {row["mode"] for row in rows})
        with self.assertRaises(SourcePolicyError):
            authorize("binance", "scanner")

    def test_source_registry_has_one_canonical_owner(self):
        from apex.market import source_registry as canonical

        root = os.path.dirname(os.path.dirname(__file__))
        self.assertFalse(os.path.exists(os.path.join(root, "core", "source_registry.py")))
        self.assertEqual(
            canonical.get_source("deribit_options"),
            canonical.get_source("deribit"),
        )
        primary = [item.source for item in canonical.REGISTRY.values() if item.can_influence_entry]
        execution = [item.source for item in canonical.REGISTRY.values() if item.can_execute]
        self.assertEqual(primary, ["gate"])
        self.assertEqual(execution, ["binance"])

    def test_market_uses_explicit_db_bridge_without_global_sqlite_patch(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex", "compatibility", "legacy_market_runtime.py"), encoding="utf-8") as source:
            market_source = source.read()
        self.assertNotIn("sqlite3.connect =", market_source)
        self.assertNotIn("_wal_patched", market_source)
        self.assertNotIn("os.environ", market_source)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS signal_cooldown", market_source)
        self.assertIn("connect_compatibility as _connect_compatibility_db", market_source)

    def test_launcher_has_an_explicit_market_compatibility_contract(self):
        source = Path("apex/app/worker.py").read_text(encoding="utf-8")
        self.assertNotIn("from market import *", source)
        self.assertNotIn("from market import (", source)
        for boundary in (
            "market_transport", "market_user_services", "market_data",
            "market_strategy",
        ):
            self.assertIn(f"from apex.compatibility.{boundary} import (", source)
            adapter = Path("apex", "compatibility", f"{boundary}.py").read_text(
                encoding="utf-8"
            )
            self.assertNotIn("from market import *", adapter)
        facade_source = Path("market.py").read_text(encoding="utf-8")
        self.assertLess(len(facade_source.splitlines()), 30)
        self.assertIn('import_module("apex.compatibility.legacy_market_runtime")', facade_source)
        self.assertNotIn("def detect_fast_deal", facade_source)
        market_source = Path(
            "apex", "compatibility", "legacy_market_runtime.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("_RAW_SCAN_HANDLER", market_source)
        self.assertNotIn("TF_CATEGORIES = {", market_source)
        self.assertNotIn("TF_LABELS = {", market_source)
        self.assertNotIn("FAST_PAIRS = [", market_source)
        self.assertNotIn("SYMBOL_ALIASES = {", market_source)
        self.assertNotIn("COINGECKO_IDS = {", market_source)
        self.assertIn("SYMBOL_ALIASES", Path(
            "apex", "compatibility", "market_constants.py"
        ).read_text(encoding="utf-8"))
        for moved in (
            "def update_global_candles(", "def get_global_candles(",
            "def get_confirmed_candles(", "def smart_price_fmt(",
            "def format_accumulation(", "def format_market(", "def format_news(",
            "class HealthHandler(", "def run_server(",
            "def _db_writer_thread(", "def start_db_writer(",
            "def db_write_async(", "def get_db_conn(",
            "def get_estimated_time(",
            "def calc_smart_levels(", "def select_structural_targets(",
            "def smart_round(",
            "def calc_risk(",
            "def register_raw_scan_handler(",
            "def analyze_trade_type(", "def full_scan(",
            "async def check_alerts(",
            "def live_position_analysis(",
            "def extract_and_save_profile(",
            "def _track_tokens(", "def _tokens_available(",
            "def legacy_strategy_groq_enabled(",
            "def save_signal_db(",
            "def check_pending_signals(",
            "def get_user_memory(", "def update_user_memory(",
            "def save_chat_log(", "def get_chat_history(",
            "def save_news(", "def get_recent_news(",
            "def save_knowledge(", "def get_knowledge(",
            "def parse_rss(", "def get_crypto_news(",
            "def get_market_impact_news(",
            "def get_top_pairs(", "def get_live_prices(",
            "def get_yahoo_finance_prices(", "def get_cryptocompare_prices(",
            "def get_cryptocompare_candles(", "def get_messari_data(",
            "def get_all_market_pairs(",
            "def get_orderbook(",
            "def get_twelvedata_candles(", "def get_mobula_price(",
            "def get_coinalyze_data(", "def get_lunarcrush_data(",
            "def get_price_realtime(",
            "def get_fear_greed(", "def get_funding_rate(",
            "def get_open_interest(",
            "def get_liquidations(", "def get_santiment_data(",
            "def get_whale_alerts(",
            "def get_fg_history(", "def get_dxy_signal(",
            "def get_upcoming_events(",
            "def get_candles(", "async def fetch_candles_batch(",
            "def get_higher_tf_context(", "def get_market_regime(",
            "def find_swings(", "def classify_swings(", "def detect_events(",
            "def get_bos_choch_event(",
            "def detect_bos_choch(", "def find_equal_highs_lows(",
            "def get_historical_context(", "def format_historical_context(",
            "def smc_on_tf(", "def multi_tf_analysis(",
            "def find_ob(", "def find_fvg(",
            "def ema_value(",
            "def average_true_range(",
            "def get_liquidation_ratio(",
            "def detect_accumulation(",
            "def get_btc_1h_change(", "def get_btc_4h_change(",
            "def btc_allows_signal(",
            "def detect_engulfing(",
            "def check_entry_timing(",
            "def get_precomputed_indicators(", "def get_adaptive_params(",
            "def check_session_liquidity(",
            "def get_btc_correlation(",
            "def detect_market_regime_v2(",
        ):
            self.assertNotIn(moved, market_source)
        data_bridge = Path("apex", "compatibility", "market_data.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("from apex.market.runtime_cache import (", data_bridge)
        self.assertIn("from apex.market.time_estimate import get_estimated_time", data_bridge)
        self.assertIn("from apex.market.gate_tickers import ", data_bridge)
        self.assertIn("from apex.market.gate_orderbook import get_orderbook", data_bridge)
        self.assertIn("from apex.market.context_quotes import get_fear_greed, get_funding_rate", data_bridge)
        self.assertIn("from apex.market.macro_context import get_dxy_signal, get_upcoming_events", data_bridge)
        self.assertIn("from apex.market.structure_bridge import find_swings, get_bos_choch_event", data_bridge)
        self.assertIn("from apex.market.legacy_zones import find_fvg, find_ob", data_bridge)
        self.assertIn("from apex.market.indicators import ema_value", data_bridge)
        self.assertIn("from apex.market.engine_bridge import calculate_vwap, get_liquidity_heatmap", data_bridge)
        self.assertIn("from apex.market.adaptive_indicators import LegacyAdaptiveIndicators", market_source)
        self.assertIn("from apex.market.session_liquidity import SessionLiquidityProvider", market_source)
        self.assertIn("from apex.market.btc_correlation import BtcCorrelationProvider", market_source)
        self.assertIn("from apex.market.btc_direction_filter import BtcDirectionFilter", market_source)
        self.assertIn("from apex.market.candle_patterns import detect_engulfing", market_source)
        self.assertIn("from apex.market.entry_timing import check_entry_timing", market_source)
        self.assertIn("from apex.market.regime_v2 import LegacyRegimeV2", market_source)
        self.assertIn("from apex.market.liquidation_context import get_liquidation_ratio", market_source)
        self.assertNotIn("from apex.market.accumulation_analysis import AccumulationAnalysis", market_source)
        self.assertIn("get_all_market_pairs", data_bridge)
        self.assertIn("from apex.ui.price_format import smart_price_fmt", data_bridge)
        user_bridge = Path(
            "apex", "compatibility", "market_user_services.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "from apex.ui.market_format import format_accumulation, format_news",
            user_bridge,
        )
        self.assertIn("from apex.ui.user_memory import ", user_bridge)
        self.assertIn("from apex.ui.context_store import save_news", user_bridge)
        self.assertIn("from apex.market.news_provider import ", user_bridge)
        self.assertIn(
            "from apex.market.accumulation_analysis import AccumulationAnalysis",
            user_bridge,
        )
        self.assertNotIn("ask_ai, ask_groq, detect_accumulation", user_bridge)
        self.assertIn("detect_accumulation = _ACCUMULATION_ANALYSIS.detect", user_bridge)
        self.assertIn("from apex.ui.risk_calculator import calc_risk", user_bridge)
        self.assertIn(
            "from apex.ui.live_position import live_position_analysis",
            user_bridge,
        )
        self.assertIn(
            "from apex.ui.profile_extraction import extract_and_save_profile",
            user_bridge,
        )
        self.assertIn(
            "from apex.strategies.legacy_scan_registry import analyze_trade_type",
            user_bridge,
        )
        self.assertIn("from apex.ui.groq_runtime import (", user_bridge)
        self.assertIn("groq_tokens_used", user_bridge)
        strategy_bridge = Path(
            "apex", "compatibility", "market_strategy.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "from apex.ui.groq_runtime import legacy_strategy_groq_enabled",
            strategy_bridge,
        )
        self.assertIn(
            "from apex.db.legacy_signal_persistence import save_signal_db",
            strategy_bridge,
        )
        self.assertIn(
            "from apex.db.legacy_pending_signals import check_pending_signals",
            strategy_bridge,
        )
        self.assertIn("from apex.ui.price_alerts import check_alerts", strategy_bridge)
        transport_bridge = Path(
            "apex", "compatibility", "market_transport.py"
        ).read_text(encoding="utf-8")
        self.assertIn("from apex.app.health_server import run_server", transport_bridge)
        self.assertIn(
            "from apex.db.compatibility_runtime import start_db_writer",
            transport_bridge,
        )

    def test_runtime_db_access_uses_the_canonical_connection_layer(self):
        root = os.path.dirname(os.path.dirname(__file__))
        runtime_files = [
            os.path.join(root, "apex/app/worker.py"), os.path.join(root, "market.py"),
            os.path.join(root, "apex", "compatibility", "legacy_market_runtime.py"),
        ]
        runtime_files.extend(
            os.path.join(root, "core", name)
            for name in os.listdir(os.path.join(root, "core"))
            if name.endswith(".py") and name != "backup_restore.py"
        )
        for package in ("external_sources", "news_context"):
            runtime_files.extend(
                os.path.join(root, package, name)
                for name in os.listdir(os.path.join(root, package))
                if name.endswith(".py")
            )
        for folder, _directories, names in os.walk(os.path.join(root, "apex")):
            runtime_files.extend(
                os.path.join(folder, name) for name in names if name.endswith(".py")
            )
        offenders = []
        for path in runtime_files:
            with open(path, encoding="utf-8") as source:
                text = source.read()
            if "sqlite3.connect(" in text or "_sq3.connect(" in text:
                offenders.append(os.path.relpath(path, root))
        self.assertEqual(offenders, [])

    def test_runtime_environment_is_parsed_only_at_typed_config_boundaries(self):
        root = os.path.dirname(os.path.dirname(__file__))
        allowed = {
            os.path.join("apex", "config", "settings.py"),
            os.path.join("apex", "ui", "dashboard", "config.py"),
        }
        offenders = []
        packages = ("apex", "core", "external_sources", "news_context")
        for package in packages:
            for folder, _directories, names in os.walk(os.path.join(root, package)):
                for name in names:
                    if not name.endswith(".py"):
                        continue
                    path = os.path.join(folder, name)
                    relative = os.path.relpath(path, root)
                    if relative in allowed or relative == os.path.join("core", "backup_restore.py"):
                        continue
                    with open(path, encoding="utf-8") as source:
                        text = source.read()
                    if "os.environ" in text or "os.getenv" in text:
                        offenders.append(relative)
        self.assertEqual(offenders, [])

    def test_context_adapters_do_not_parse_environment_independently(self):
        root = os.path.dirname(os.path.dirname(__file__))
        offenders = []
        for package in ("external_sources", "news_context"):
            for name in os.listdir(os.path.join(root, package)):
                if not name.endswith(".py"):
                    continue
                relative = os.path.join(package, name)
                with open(os.path.join(root, relative), encoding="utf-8") as source:
                    text = source.read()
                if "os.environ" in text or "os.getenv" in text:
                    offenders.append(relative)
        self.assertEqual(offenders, [])

    def test_runtime_heartbeat_persists_full_sha_for_release_verification(self):
        root = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(root, "apex/app/worker.py"), encoding="utf-8") as source:
            bot_source = source.read()
        self.assertIn('public["release_sha"] = _V3_RUNTIME.snapshot()["release_sha"]', bot_source)

    def test_ready_runtime_still_rejects_entry_after_fencing_expiry(self):
        supervisor = RuntimeSupervisor()
        supervisor.activate(release_sha="a" * 40, instance_id="worker-a")
        for component in supervisor.REQUIRED_COMPONENTS:
            supervisor.mark_component(component, ComponentState.READY)
        future = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()
        supervisor.set_instance_lease(3, future)
        self.assertTrue(supervisor.evaluate_readiness())
        self.assertTrue(supervisor.allows_new_entries)
        past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        supervisor.set_instance_lease(3, past)
        self.assertFalse(supervisor.allows_new_entries)

    def test_all_five_strategies_have_specs_and_data_contracts(self):
        self.assertEqual(set(CONTRACTS), set(Strategy))
        self.assertEqual(set(SPECIFICATIONS), set(Strategy))
        self.assertIs(contract_for("FAST").fields["gate_15m"], Relevance.REQUIRED)
        self.assertIs(contract_for("WYCKOFF").fields["rsi"], Relevance.LOW_RELEVANCE)


if __name__ == "__main__":
    unittest.main()
