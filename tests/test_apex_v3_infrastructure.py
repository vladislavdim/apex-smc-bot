import asyncio
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from apex.app.runtime import runtime_supervisor
from apex.app.scheduler import SchedulerCallbacks, _guarded, build_production_scheduler
from apex.config.settings import ApexConfig, ConfigParseError
from apex.config.validation import ConfigError, validate_config
from apex.db.memory_db import MEMORY_MIGRATIONS, migrate_memory
from apex.db.maintenance import maintain_memory, maintain_state
from apex.db.migrations import Migration, MigrationError, MigrationRunner
from apex.db.state_db import migrate_state
from apex.db.repositories.runtime import RuntimeRepository
from apex.db.repositories.manager import ManagerRepository, ManagerStateError
from apex.db.repositories.executions import ExecutionRepository, ExecutionStateError
from apex.domain.ids import is_id, new_id
from apex.telemetry.incidents import (
    active_incidents, configure_incidents, current_incidents,
    mark_notification_delivered,
    open_incident, pending_notifications, recover_incident, report_incident,
    resolve_incident,
)
from apex.telemetry.dashboard_projection import normalize_incident_snapshot
from apex.telemetry.job_metrics import JobRunRecorder, configure_job_metrics
from apex.ops.release_manifest import (
    ReleaseManifestError,
    build_release_manifest,
    persist_release_manifest,
)


class ApexV3InfrastructureTests(unittest.TestCase):
    def tearDown(self):
        runtime_supervisor.deactivate()
        configure_job_metrics(None)

    def test_one_scheduler_definition_serves_both_transports(self):
        class FakeScheduler:
            def __init__(self, **kwargs):
                self.defaults = kwargs
                self.jobs = []

            def add_job(self, callback, trigger, **kwargs):
                self.jobs.append((callback, trigger, kwargs))

        def noop():
            return None

        callbacks = SchedulerCallbacks(**{
            field: noop for field in SchedulerCallbacks.__dataclass_fields__
        })
        webhook = build_production_scheduler(
            callbacks, execution_reconcile_seconds=30, scheduler_factory=FakeScheduler,
        )
        polling = build_production_scheduler(
            callbacks, execution_reconcile_seconds=30, scheduler_factory=FakeScheduler,
        )
        webhook_ids = [job[2]["id"] for job in webhook.jobs]
        polling_ids = [job[2]["id"] for job in polling.jobs]
        self.assertEqual(webhook_ids, polling_ids)
        self.assertEqual(len(webhook_ids), len(set(webhook_ids)))
        self.assertIn("trade_manager", webhook_ids)
        self.assertNotIn("experience_shadow", webhook_ids)

    def test_scheduler_updates_component_health_and_fails_closed_for_critical_job(self):
        runtime_supervisor.activate()

        async def ok():
            return "done"

        self.assertEqual(asyncio.run(_guarded("trade_manager", ok)()), "done")
        self.assertEqual(
            runtime_supervisor.snapshot()["components"]["manager"]["state"], "READY",
        )

        async def broken():
            raise RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            asyncio.run(_guarded("execution_reconcile", broken)())
        snapshot = runtime_supervisor.snapshot()
        self.assertEqual(snapshot["components"]["binance_reconciliation"]["state"], "DEGRADED")
        self.assertIn("JOB_FAILED:execution_reconcile", snapshot["reason_codes"])

    def test_scheduler_persists_complete_job_lifecycle(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "state.db")
            conn = sqlite3.connect(path)
            migrate_state(conn)
            conn.close()

            def factory():
                connection = sqlite3.connect(path)
                connection.row_factory = sqlite3.Row
                return connection

            configure_job_metrics(factory)

            async def work():
                return {"items_processed": 7}

            result = asyncio.run(_guarded("market_fast", work)())
            self.assertEqual(result["items_processed"], 7)
            check = factory()
            row = check.execute(
                "SELECT status,items_processed,duration_ms,rss_before,rss_after FROM job_runs"
            ).fetchone()
            check.close()
            self.assertEqual(row["status"], "OK")
            self.assertEqual(row["items_processed"], 7)
            self.assertIsNotNone(row["duration_ms"])
            self.assertIsNotNone(row["rss_before"])
            self.assertIsNotNone(row["rss_after"])

    def test_job_metrics_failure_never_blocks_production_job(self):
        def broken_factory():
            raise sqlite3.OperationalError("unavailable")

        configure_job_metrics(broken_factory)

        async def work():
            return "completed"

        self.assertEqual(asyncio.run(_guarded("trade_manager", work)()), "completed")

    def test_config_validation_blocks_unarmed_live_execution(self):
        config = ApexConfig.from_env({
            "AUTO_TRADING_ENABLED": "1",
            "AUTO_TRADING_MODE": "live",
            "AUTO_TRADING_RISK_PCT": "0.5",
        })
        result = validate_config(config)
        self.assertFalse(result.valid)
        self.assertIn("BINANCE_CREDENTIALS_MISSING", result.errors)
        self.assertIn("LIVE_CONFIRMATION_MISSING", result.errors)
        with self.assertRaises(ConfigError):
            validate_config(config, raise_on_error=True)

    def test_config_validation_accepts_safe_paper_defaults(self):
        self.assertTrue(validate_config(ApexConfig.from_env({})).valid)

    def test_snapshot_strategy_activation_is_typed_and_off_by_default(self):
        disabled = ApexConfig.from_env({})
        enabled = ApexConfig.from_env({
            "APEX_SNAPSHOT_STRATEGIES_ENABLED": "true",
            "APEX_STRATEGY_PARITY_CORPUS": "/proof/corpus",
            "APEX_STRATEGY_PARITY_VERDICT": "/proof/verdict.json",
        })
        self.assertFalse(disabled.strategies.snapshot_activation_requested)
        self.assertTrue(enabled.strategies.snapshot_activation_requested)
        self.assertEqual(enabled.strategies.parity_corpus_path, "/proof/corpus")
        self.assertEqual(enabled.strategies.parity_verdict_path, "/proof/verdict.json")
        self.assertTrue(validate_config(enabled).valid)

    def test_invalid_retention_config_fails_closed(self):
        config = ApexConfig.from_env({
            "APEX_STATE_TELEMETRY_RETENTION_DAYS": "1",
            "APEX_MEMORY_CONTEXT_RETENTION_DAYS": "2",
            "APEX_RESOLVED_INCIDENT_RETENTION_DAYS": "3",
        })
        errors = validate_config(config).errors
        self.assertIn("STATE_RETENTION_INVALID", errors)
        self.assertIn("MEMORY_RETENTION_INVALID", errors)
        self.assertIn("INCIDENT_RETENTION_INVALID", errors)

    def test_explicit_invalid_number_is_not_silently_defaulted(self):
        with self.assertRaisesRegex(ConfigParseError, "AUTO_TRADING_LEVERAGE_INVALID"):
            ApexConfig.from_env({"AUTO_TRADING_LEVERAGE": "five"})
        with self.assertRaisesRegex(ConfigParseError, "AUTO_TRADING_ENABLED_INVALID"):
            ApexConfig.from_env({"AUTO_TRADING_ENABLED": "perhaps"})

    def test_operational_and_strategy_config_are_separate_and_versioned(self):
        first = ApexConfig.from_env({})
        second = ApexConfig.from_env({"APEX_EVENT_LOOP_LAG_SLA_MS": "750"})
        deployed = ApexConfig.from_env({
            "RENDER_GIT_COMMIT": "a" * 40,
            "RENDER_INSTANCE_ID": "srv-new-instance",
        })
        restarted = ApexConfig.from_env({
            "RENDER_GIT_COMMIT": "b" * 40,
            "RENDER_INSTANCE_ID": "srv-restarted-instance",
        })
        self.assertEqual(first.strategies.manifest_hash(), second.strategies.manifest_hash())
        self.assertNotEqual(first.safe_config_hash(), second.safe_config_hash())
        self.assertEqual(deployed.safe_config_hash(), restarted.safe_config_hash())
        self.assertEqual(len(first.strategies.manifest_hash()), 64)
        self.assertEqual(first.strategies.minimum_rr, 2.0)
        self.assertEqual(
            {name for name, _version in first.strategies.versions},
            {"FAST", "MTF", "SWING", "ZONE", "WYCKOFF"},
        )

    def test_config_hash_never_contains_or_depends_on_secret_values(self):
        first = ApexConfig.from_env({
            "BINANCE_API_KEY": "first-key", "BINANCE_API_SECRET": "first-secret",
            "GROQ_API_KEY": "first-groq", "TELEGRAM_TOKEN": "first-telegram",
            "GROQ_API_KEY_2": "first-second-groq", "ADMIN_ID": "111,222",
            "COINALYZE_API_KEY": "first-coinalyze",
            "CRYPTO_MONITOR_API_KEY": "first-monitor",
            "OLI_API_KEY": "first-oli",
            "OLI_TRACKED_ADDRESSES_JSON": '{"ETHUSDT":["first"]}',
            "WHALE_TRACKER_API_KEY": "first-whale",
        })
        second = ApexConfig.from_env({
            "BINANCE_API_KEY": "second-key", "BINANCE_API_SECRET": "second-secret",
            "GROQ_API_KEY": "second-groq", "TELEGRAM_TOKEN": "second-telegram",
            "GROQ_API_KEY_2": "second-second-groq", "ADMIN_ID": "333,444",
            "COINALYZE_API_KEY": "second-coinalyze",
            "CRYPTO_MONITOR_API_KEY": "second-monitor",
            "OLI_API_KEY": "second-oli",
            "OLI_TRACKED_ADDRESSES_JSON": '{"ETHUSDT":["second"]}',
            "WHALE_TRACKER_API_KEY": "second-whale",
        })
        self.assertEqual(first.safe_config_hash(), second.safe_config_hash())

    def test_market_integrations_are_parsed_by_central_config(self):
        config = ApexConfig.from_env({
            "ADMIN_ID": "111,222",
            "SIGNAL_CHANNEL_MAIN": "-1001",
            "SIGNAL_CHANNEL_ID": "-1002",
            "SWING_THREAD_ID": "42",
            "FAST_DEAL_THREAD_ID": "43",
            "GROQ_API_KEY": "primary",
            "GROQ_API_KEY_2": "secondary",
            "COINALYZE_API_KEY": "context-key",
            "APEX_GATE_DEPTH_SYMBOLS": "btcusdt,ethusdt",
            "LEGACY_STRATEGY_GROQ": "false",
        })
        self.assertEqual(config.integrations.telegram_admin_ids, (111, 222))
        self.assertEqual(config.integrations.signal_channel_main, -1001)
        self.assertEqual(config.integrations.signal_channel_swing, -1002)
        self.assertEqual(config.integrations.groq_api_keys, ("primary", "secondary"))
        self.assertEqual(config.integrations.coinalyze_api_key, "context-key")
        self.assertEqual(config.integrations.gate_depth_symbols, ("BTCUSDT", "ETHUSDT"))
        self.assertFalse(config.integrations.legacy_strategy_groq)

    def test_invalid_admin_id_fails_configuration_closed(self):
        with self.assertRaisesRegex(ConfigParseError, "ADMIN_ID_INVALID"):
            ApexConfig.from_env({"ADMIN_ID": "111,not-a-number"})

    def test_config_validation_rejects_strategy_or_operational_drift(self):
        invalid_rr = ApexConfig.from_env({"APEX_MINIMUM_RR": "1.9"})
        invalid_ttl = ApexConfig.from_env({"APEX_RUNTIME_LEASE_TTL_SECONDS": "5"})
        self.assertIn("MINIMUM_RR_PARITY_VIOLATION", validate_config(invalid_rr).errors)
        self.assertIn("LEASE_TTL_INVALID", validate_config(invalid_ttl).errors)

    def test_state_and_memory_migrations_are_idempotent(self):
        state = sqlite3.connect(":memory:")
        memory = sqlite3.connect(":memory:")
        self.assertEqual(migrate_state(state), tuple(range(1, 21)))
        self.assertEqual(migrate_state(state), ())
        self.assertEqual(migrate_memory(memory), (1, 2, 3))
        self.assertEqual(migrate_memory(memory), ())
        self.assertIsNotNone(state.execute("SELECT 1 FROM job_runs LIMIT 1").description)
        self.assertIsNotNone(state.execute("SELECT 1 FROM setup_audit_events LIMIT 1").description)
        self.assertIsNotNone(memory.execute("SELECT 1 FROM live_trade_outcomes LIMIT 1").description)

    def test_manager_repository_freezes_geometry_and_deduplicates_events(self):
        state = sqlite3.connect(":memory:")
        state.row_factory = sqlite3.Row
        migrate_state(state)

        def factory():
            return state

        # Keep the shared in-memory connection open while repository methods
        # exercise close ownership through a lightweight proxy.
        class Proxy:
            def __init__(self, connection):
                self.connection = connection
            def __getattr__(self, name):
                return getattr(self.connection, name)
            def close(self):
                return None

        repository = ManagerRepository(lambda: Proxy(state))
        position = {
            "signal_id": 91, "symbol": "BTCUSDT", "strategy": "SWING",
            "direction": "BULLISH", "management_tf": "1h",
            "initial_entry": 100.0, "initial_sl": 95.0,
            "initial_tp1": 110.0, "initial_tp2": 115.0,
            "initial_tp3": 120.0, "initial_rr": 2.0, "manager_version": 3,
            "thesis": {"core": ["4H_STRUCTURE"]},
        }
        self.assertTrue(repository.register(position))
        self.assertFalse(repository.register(position))
        changed = dict(position, initial_sl=96.0)
        with self.assertRaisesRegex(ManagerStateError, "geometry_conflict"):
            repository.register(changed)
        event_id = new_id("manager_event")
        event = {
            "manager_event_id": event_id, "signal_id": 91,
            "event_type": "MANAGEMENT_CANDLE_CLOSE", "action": "HOLD",
            "reason_codes": ("NO_MATERIAL_EVENT",),
        }
        self.assertTrue(repository.append_event(event))
        self.assertFalse(repository.append_event(event))
        self.assertEqual(repository.get(91)["initial_sl"], 95.0)
        self.assertTrue(is_id(repository.get(91)["signal_entity_id"], "signal"))
        self.assertEqual(len(repository.active()), 1)
        self.assertEqual(repository.recent()[0]["signal_id"], 91)
        self.assertEqual(repository.events(91)[0]["manager_event_id"], event_id)
        self.assertEqual(
            repository.events(91)[0]["signal_entity_id"],
            repository.get(91)["signal_entity_id"],
        )
        state.close()

    def test_execution_repository_freezes_plan_and_owns_action_claims(self):
        state = sqlite3.connect(":memory:")
        state.row_factory = sqlite3.Row
        migrate_state(state)

        class Proxy:
            def __init__(self, connection): object.__setattr__(self, "connection", connection)
            def __getattr__(self, name): return getattr(self.connection, name)
            def __setattr__(self, name, value):
                if name == "connection": object.__setattr__(self, name, value)
                else: setattr(self.connection, name, value)
            def close(self): return None

        repository = ExecutionRepository(lambda: Proxy(state))
        plan = {
            "signal_id": 51, "execution_id": new_id("execution"),
            "candidate_id": new_id("candidate"), "mode": "live",
            "symbol": "BTCUSDT", "direction": "LONG", "status": "ENTRY_PENDING",
            "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
            "quantity": 0.01, "risk_usdt": 5, "balance_usdt": 1000, "leverage": 5,
        }
        self.assertTrue(repository.register(plan))
        self.assertFalse(repository.register(plan))
        self.assertTrue(is_id(repository.get(51)["signal_entity_id"], "signal"))
        with self.assertRaisesRegex(ExecutionStateError, "execution_invalid:signal_entity_id"):
            repository.register(dict(plan, signal_id=52, signal_entity_id="legacy-52"))
        with self.assertRaisesRegex(ExecutionStateError, "plan_conflict"):
            repository.register(dict(plan, sl=96))
        with self.assertRaisesRegex(ExecutionStateError, "immutable_field"):
            repository.update_exchange_state(51, sl=96)
        self.assertTrue(repository.update_exchange_state(
            51, status="PROTECTED", entry_order_id="entry-1",
            stop_order_id="stop-1", active_stop_price=95,
        ))
        self.assertTrue(repository.claim_action("protect-1", 51, "PROTECT", 98))
        self.assertFalse(repository.claim_action("protect-1", 51, "PROTECT", 98))
        self.assertTrue(repository.finish_action("protect-1", "EXECUTED", order_id="stop-2"))
        self.assertEqual(repository.get(51)["active_stop_price"], 95)
        manager = ManagerRepository(lambda: Proxy(state))
        self.assertTrue(manager.register({
            "signal_id": 51, "symbol": "BTCUSDT", "strategy": "MTF",
            "direction": "LONG", "management_tf": "1h",
            "initial_entry": 100, "initial_sl": 95, "initial_tp1": 110,
            "manager_version": 3,
        }))
        self.assertEqual(
            manager.get(51)["signal_entity_id"],
            repository.get(51)["signal_entity_id"],
        )
        with self.assertRaisesRegex(
            ManagerStateError, "manager_event_signal_identity_mismatch"
        ):
            manager.append_event({
                "manager_event_id": new_id("manager_event"), "signal_id": 51,
                "signal_entity_id": new_id("signal"),
                "event_type": "MANAGEMENT_CANDLE_CLOSE",
            })
        state.close()

    def test_manager_repository_requires_exchange_confirmation_for_mutations(self):
        state = sqlite3.connect(":memory:")
        state.row_factory = sqlite3.Row
        migrate_state(state)

        class Proxy:
            def __init__(self, connection): object.__setattr__(self, "connection", connection)
            def __getattr__(self, name): return getattr(self.connection, name)
            def __setattr__(self, name, value):
                if name == "connection": object.__setattr__(self, name, value)
                else: setattr(self.connection, name, value)
            def close(self): return None

        repository = ManagerRepository(lambda: Proxy(state))
        repository.register({
            "signal_id": 92, "symbol": "BTCUSDT", "strategy": "FAST",
            "direction": "LONG", "management_tf": "5m", "initial_entry": 100,
            "initial_sl": 95, "initial_tp1": 110, "manager_version": 3,
        })
        event = {
            "manager_event_id": new_id("manager_event"), "signal_id": 92,
            "event_type": "BOS", "action": "PROTECT", "price": 105,
        }
        self.assertTrue(repository.record_review(92, {
            "last_price": 105, "best_price": 106, "current_r": 2,
            "proposed_protect_level": 98, "last_event": "BOS",
            "last_action": "PROTECT",
        }, event))
        self.assertIsNone(repository.get(92)["confirmed_protect_level"])
        self.assertFalse(repository.confirm_action(
            92, action="PROTECT", next_state="MANAGING",
            execution_status="ERROR", confirmed_stop=98,
        ))
        self.assertTrue(repository.confirm_action(
            92, action="PROTECT", next_state="MANAGING",
            execution_status="EXECUTED", confirmed_stop=98,
        ))
        self.assertEqual(repository.get(92)["confirmed_protect_level"], 98)
        self.assertFalse(repository.confirm_action(
            92, action="PARTIAL_EXIT", next_state="TP1_REACHED",
            execution_status="EXECUTED", remaining_fraction=1.0,
        ))
        self.assertTrue(repository.confirm_action(
            92, action="PARTIAL_EXIT", next_state="TP1_REACHED",
            execution_status="EXECUTED", remaining_fraction=0.7,
        ))
        with self.assertRaisesRegex(ManagerStateError, "unconfirmed_accounting"):
            repository.close_from_accounting(92, {"net_r": 2}, result="tp")
        self.assertTrue(repository.mark_exchange_closed(92, {
            "status": "CLOSED", "exit_price": 110, "exit_time": 1_789_000_000_000,
        }, result="tp1"))
        pending = repository.get(92)
        self.assertEqual(pending["last_event"], "CONFIRMED_BINANCE_FILLS_ACCOUNTING_PENDING")
        self.assertIsNone(pending["realized_r"])
        self.assertTrue(repository.close_from_accounting(92, {
            "accounting_basis": "confirmed_fills_after_commissions_and_funding",
            "exit_price": 110, "realized_pct": 9.8, "net_r": 1.9,
        }, result="tp1"))
        self.assertEqual(repository.get(92)["status"], "CLOSED")
        state.close()

    def test_runtime_repository_owns_restart_and_bounded_heartbeat_state(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "state.db")

            def factory():
                conn = sqlite3.connect(path)
                conn.row_factory = sqlite3.Row
                return conn

            conn = factory()
            migrate_state(conn)
            conn.close()
            repository = RuntimeRepository(factory)
            started = repository.record_start("worker-one", "a" * 40)
            self.assertEqual(started["restart_count_1h"], 1)
            self.assertTrue(repository.record_shutdown("worker-one", "SIGTERM"))
            for _index in range(105):
                repository.heartbeat("worker-one", "a" * 40)
            conn = factory()
            try:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM runtime_heartbeats").fetchone()[0], 100)
                row = conn.execute(
                    "SELECT shutdown_reason FROM runtime_instances WHERE instance_id='worker-one'"
                ).fetchone()
                self.assertEqual(row[0], "SIGTERM")
            finally:
                conn.close()

    def test_level_event_migration_preserves_existing_memory(self):
        memory = sqlite3.connect(":memory:")
        MigrationRunner(MEMORY_MIGRATIONS[:2]).run(memory)
        memory.execute(
            """INSERT INTO live_market_events
               (event_id,symbol,timeframe,event_type,event_time,payload_json,source,provenance)
               VALUES('old','BTCUSDT','1h','BOS','2026-09-11T00:00:00+00:00','{}','Gate','PRIMARY_MARKET')"""
        )
        memory.commit()
        self.assertEqual(migrate_memory(memory), (3,))
        self.assertEqual(
            memory.execute("SELECT event_id FROM live_market_events").fetchone()[0],
            "old",
        )
        indexes = {row[1] for row in memory.execute("PRAGMA index_list(live_market_events)")}
        self.assertIn("idx_live_market_event_lookup", indexes)

    def test_release_manifest_is_complete_secret_free_and_immutable(self):
        config = ApexConfig.from_env({
            "RENDER_GIT_COMMIT": "a" * 40,
            "BINANCE_API_KEY": "binance-secret",
            "GROQ_API_KEY": "groq-secret",
        })
        manifest = build_release_manifest(config, deployed_at="2026-09-11T00:00:00+00:00")
        self.assertTrue(manifest.production_valid)
        self.assertEqual(set(manifest.strategy_versions), {"FAST", "MTF", "SWING", "ZONE", "WYCKOFF"})
        self.assertNotIn("binance-secret", manifest.canonical_json())
        self.assertNotIn("groq-secret", manifest.canonical_json())
        conn = sqlite3.connect(":memory:")
        migrate_state(conn)
        persist_release_manifest(conn, manifest)
        persist_release_manifest(conn, manifest)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM release_manifests").fetchone()[0], 1)
        restarted = build_release_manifest(config, deployed_at="2026-09-11T00:01:00+00:00")
        persist_release_manifest(conn, restarted)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM release_manifests").fetchone()[0], 1)
        changed_config = ApexConfig.from_env({
            "RENDER_GIT_COMMIT": "a" * 40,
            "APEX_FAST_CONCURRENCY": "5",
        })
        changed = build_release_manifest(changed_config, deployed_at="2026-09-11T00:02:00+00:00")
        with self.assertRaisesRegex(ReleaseManifestError, "release_manifest_conflict"):
            persist_release_manifest(conn, changed)

    def test_release_manifest_refuses_non_exact_sha(self):
        conn = sqlite3.connect(":memory:")
        migrate_state(conn)
        manifest = build_release_manifest(ApexConfig.from_env({}), release_sha="unknown")
        with self.assertRaisesRegex(ReleaseManifestError, "release_sha_invalid"):
            persist_release_manifest(conn, manifest)

    def test_failed_migration_rolls_back_and_is_not_marked_applied(self):
        conn = sqlite3.connect(":memory:")

        def fail(connection):
            connection.execute("CREATE TABLE must_rollback(id INTEGER)")
            raise RuntimeError("boom")

        with self.assertRaises(MigrationError):
            MigrationRunner((Migration(1, "fails", fail),)).run(conn)
        self.assertIsNone(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='must_rollback'"
        ).fetchone())
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM apex_schema_migrations").fetchone()[0], 0)

    def test_migration_history_drift_fails_closed(self):
        conn = sqlite3.connect(":memory:")
        migrate_state(conn)
        conn.execute(
            "UPDATE apex_schema_migrations SET name='mutated_history' WHERE version=2"
        )
        conn.commit()
        with self.assertRaisesRegex(MigrationError, "migration_history_drift:2"):
            migrate_state(conn)

    def test_database_newer_than_runtime_fails_closed(self):
        conn = sqlite3.connect(":memory:")
        migrate_memory(conn)
        conn.execute(
            "INSERT INTO apex_schema_migrations(version,name) VALUES(99,'future_runtime')"
        )
        conn.commit()
        with self.assertRaisesRegex(MigrationError, "database_schema_newer_than_runtime:99"):
            migrate_memory(conn)

    def test_incident_lifecycle_deduplicates_and_resolves(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        migrate_state(conn)
        first = open_incident(conn, "GATE_STALE", "gate", "WARNING", {"age": 121})
        second = open_incident(conn, "GATE_STALE", "gate", "ERROR", {"age": 300})
        self.assertTrue(first["opened"])
        self.assertFalse(second["opened"])
        rows = active_incidents(conn)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["count"], 2)
        self.assertEqual(rows[0]["severity"], "ERROR")
        self.assertTrue(resolve_incident(conn, "GATE_STALE", "gate"))
        self.assertEqual(active_incidents(conn), [])

    def test_global_incident_service_is_safe_deduplicated_and_recoverable(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "state.db")
            conn = sqlite3.connect(path)
            migrate_state(conn)
            conn.close()

            def factory():
                connection = sqlite3.connect(path)
                connection.row_factory = sqlite3.Row
                return connection

            configure_incidents(factory)
            try:
                first = report_incident("JOB_FAILED", "manager", "warning", {"attempt": 1})
                repeat = report_incident("JOB_FAILED", "manager", "ERROR", {"attempt": 2})
                self.assertTrue(first["notify"])
                self.assertTrue(repeat["severity_changed"])
                self.assertFalse(repeat["opened"])
                check = factory()
                rows = active_incidents(check)
                check.close()
                self.assertEqual(rows[0]["count"], 2)
                self.assertEqual(rows[0]["details"], {"attempt": 2})
                self.assertEqual(current_incidents()[0]["code"], "JOB_FAILED")
                self.assertTrue(recover_incident("JOB_FAILED", "manager"))
                notifications = pending_notifications()
                self.assertEqual(
                    [item["event_type"] for item in notifications],
                    ["OPENED", "SEVERITY_CHANGED", "RESOLVED"],
                )
                self.assertTrue(mark_notification_delivered(notifications[0]["notification_id"]))
                self.assertEqual(len(pending_notifications()), 2)
            finally:
                configure_incidents(None)

    def test_dashboard_incident_projection_is_bounded_and_sanitized(self):
        projected = normalize_incident_snapshot([
            {
                "incident_id": "incident-1", "code": "GATE_STALE",
                "severity": "error", "component": "gate", "count": "2",
                "started_at": "2026-09-11T00:00:00+00:00",
                "details": {"age": 300, "nested": {"not": "forwarded"}},
                "secret_field": "discard me",
            },
            "not-an-incident",
        ])
        self.assertEqual(len(projected), 1)
        self.assertEqual(projected[0]["severity"], "ERROR")
        self.assertEqual(projected[0]["count"], 2)
        self.assertEqual(projected[0]["details"]["age"], "300")
        self.assertNotIn("secret_field", projected[0])

    def test_job_run_records_resource_and_result(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "state.db")

            def factory():
                conn = sqlite3.connect(path)
                conn.row_factory = sqlite3.Row
                return conn

            conn = factory()
            migrate_state(conn)
            conn.close()
            with JobRunRecorder(factory, "market_fast") as run:
                run.finish("OK", items_processed=80)
            conn = factory()
            row = conn.execute("SELECT * FROM job_runs WHERE run_id=?", (run.run_id,)).fetchone()
            conn.close()
        self.assertEqual(row["status"], "OK")
        self.assertEqual(row["items_processed"], 80)
        self.assertIsNotNone(row["duration_ms"])

    def test_database_maintenance_prunes_only_bounded_telemetry(self):
        with tempfile.TemporaryDirectory() as folder:
            state_path = os.path.join(folder, "state.db")
            memory_path = os.path.join(folder, "memory.db")
            state = sqlite3.connect(state_path)
            state.row_factory = sqlite3.Row
            memory = sqlite3.connect(memory_path)
            memory.row_factory = sqlite3.Row
            migrate_state(state)
            migrate_memory(memory)
            now = datetime(2026, 9, 11, tzinfo=timezone.utc)
            old = (now - timedelta(days=400)).isoformat()
            recent = (now - timedelta(days=1)).isoformat()
            state.execute(
                "INSERT INTO job_runs(run_id,job_id,started_at,finished_at,status) VALUES(?,?,?,?,?)",
                ("old", "market_fast", old, old, "OK"),
            )
            state.execute(
                "INSERT INTO job_runs(run_id,job_id,started_at,finished_at,status) VALUES(?,?,?,?,?)",
                ("new", "market_fast", recent, recent, "OK"),
            )
            state.execute(
                "INSERT INTO delivery_claims(cache_key,claimed_at,delivered_at,updated_at) VALUES(?,?,?,?)",
                ("old-delivery", 1.0, 2.0, old),
            )
            state.execute(
                "INSERT INTO delivery_claims(cache_key,claimed_at,delivered_at,updated_at) VALUES(?,?,?,?)",
                ("new-delivery", 3.0, 4.0, recent),
            )
            for event_key, occurred_at, synced in (
                ("old-synced", old, 1),
                ("old-unsynced", old, 0),
                ("new-synced", recent, 1),
            ):
                state.execute(
                    """INSERT INTO setup_audit_events
                       (event_key,kind,occurred_at,payload_json,synced)
                       VALUES(?, 'strategy_attempt', ?, '{}', ?)""",
                    (event_key, occurred_at, synced),
                )
            for symbol, created_at in (("OLDUSDT", old), ("NEWUSDT", recent)):
                state.execute(
                    """INSERT INTO strategy_decisions
                       (symbol,strategy,outcome,stage,created_at)
                       VALUES(?, 'FAST', 'WAIT', 'groq_quality_gate', ?)""",
                    (symbol, created_at),
                )
            memory.execute(
                """INSERT INTO live_trade_outcomes
                   (outcome_id,position_id,strategy,symbol,direction,net_r,closed_at)
                   VALUES('kept','position','FAST','BTCUSDT','LONG',1.0,?)""",
                (old,),
            )
            for event_id, event_time in (("old-event", old), ("new-event", recent)):
                memory.execute(
                    """INSERT INTO live_market_events
                       (event_id,symbol,timeframe,event_type,event_time,payload_json,source,provenance)
                       VALUES(?,?,?,?,?,'{}','Gate','PRIMARY_MARKET')""",
                    (event_id, "BTCUSDT", "1h", event_id, event_time),
                )
            state.commit()
            memory.commit()
            state_report = maintain_state(state, state_path, now=now)
            memory_report = maintain_memory(memory, memory_path, now=now)
            self.assertEqual(state_report["deleted"]["job_runs"], 1)
            self.assertEqual(state_report["deleted"]["delivery_claims"], 1)
            self.assertEqual(state_report["deleted"]["setup_audit_events"], 1)
            self.assertEqual(state_report["deleted"]["strategy_decisions"], 1)
            self.assertEqual(state.execute("SELECT COUNT(*) FROM job_runs").fetchone()[0], 1)
            self.assertEqual(state.execute("SELECT COUNT(*) FROM delivery_claims").fetchone()[0], 1)
            self.assertEqual(
                {row[0] for row in state.execute("SELECT event_key FROM setup_audit_events")},
                {"old-unsynced", "new-synced"},
            )
            self.assertEqual(
                state.execute("SELECT symbol FROM strategy_decisions").fetchone()[0],
                "NEWUSDT",
            )
            self.assertEqual(memory_report["deleted"]["market_events"], 1)
            self.assertEqual(memory.execute("SELECT COUNT(*) FROM live_market_events").fetchone()[0], 1)
            self.assertEqual(memory.execute("SELECT COUNT(*) FROM live_trade_outcomes").fetchone()[0], 1)
            self.assertEqual(state_report["health"]["integrity"], "ok")
            self.assertEqual(memory_report["health"]["integrity"], "ok")
            state.close()
            memory.close()


if __name__ == "__main__":
    unittest.main()
