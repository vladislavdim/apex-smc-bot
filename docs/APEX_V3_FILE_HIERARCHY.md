# APEX repository — full file hierarchy

Current repository structure for `vladislavdim/apex-smc-bot`.

Source commit: `c1150151646ec0d2a9584b28a4aead3669e5f494`

This is the **complete repository tree** at that commit: root files, GitHub workflows, APEX V3 packages, compatibility/core modules, external/news sources, scripts, fixtures and the full test suite.

```text
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── controlled-production-release.yml
├── apex/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── bootstrap.py
│   │   ├── cutover.py
│   │   ├── health_server.py
│   │   ├── job_registry.py
│   │   ├── readiness.py
│   │   ├── runtime.py
│   │   └── scheduler.py
│   ├── compatibility/
│   │   ├── __init__.py
│   │   ├── legacy_market_runtime.py
│   │   ├── market_constants.py
│   │   ├── market_data.py
│   │   ├── market_strategy.py
│   │   ├── market_transport.py
│   │   └── market_user_services.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── settings.py
│   │   ├── validation.py
│   │   └── versions.py
│   ├── db/
│   │   ├── repositories/
│   │   │   ├── __init__.py
│   │   │   ├── correlation.py
│   │   │   ├── deliveries.py
│   │   │   ├── execution_account.py
│   │   │   ├── execution_ledger.py
│   │   │   ├── executions.py
│   │   │   ├── manager_messages.py
│   │   │   ├── manager.py
│   │   │   ├── runtime.py
│   │   │   ├── signal_lifecycle.py
│   │   │   └── strategy_decisions.py
│   │   ├── __init__.py
│   │   ├── compatibility_runtime.py
│   │   ├── connection.py
│   │   ├── execution_ledger_migration.py
│   │   ├── execution_migration.py
│   │   ├── execution_recovery.py
│   │   ├── legacy_pending_signals.py
│   │   ├── legacy_signal_persistence.py
│   │   ├── maintenance.py
│   │   ├── manager_migration.py
│   │   ├── memory_db.py
│   │   ├── migrations.py
│   │   ├── ownership.py
│   │   ├── signal_lifecycle_migration.py
│   │   └── state_db.py
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── enums.py
│   │   ├── ids.py
│   │   ├── models.py
│   │   └── reason_codes.py
│   ├── execution/
│   │   ├── __init__.py
│   │   ├── accounting.py
│   │   └── plan.py
│   ├── learning/
│   │   ├── __init__.py
│   │   ├── advisory.py
│   │   ├── confidence.py
│   │   ├── live_bridge.py
│   │   ├── live_memory.py
│   │   ├── similarity.py
│   │   └── statistics.py
│   ├── manager/
│   │   ├── __init__.py
│   │   ├── eligibility.py
│   │   ├── groq_schema.py
│   │   └── state_machine.py
│   ├── market/
│   │   ├── __init__.py
│   │   ├── accumulation_analysis.py
│   │   ├── adaptive_indicators.py
│   │   ├── breadth.py
│   │   ├── btc_correlation.py
│   │   ├── btc_direction_filter.py
│   │   ├── candle_patterns.py
│   │   ├── candle_router.py
│   │   ├── candles.py
│   │   ├── context_memory.py
│   │   ├── context_quotes.py
│   │   ├── derivatives.py
│   │   ├── derived_context.py
│   │   ├── engine_bridge.py
│   │   ├── entry_timing.py
│   │   ├── freshness.py
│   │   ├── gate_client.py
│   │   ├── gate_orderbook.py
│   │   ├── gate_tickers.py
│   │   ├── health.py
│   │   ├── historical_context.py
│   │   ├── indicators.py
│   │   ├── legacy_zones.py
│   │   ├── level_memory.py
│   │   ├── levels.py
│   │   ├── liquidation_context.py
│   │   ├── live_context.py
│   │   ├── macro_context.py
│   │   ├── microstructure.py
│   │   ├── news_provider.py
│   │   ├── optional_context.py
│   │   ├── optional_prices.py
│   │   ├── optional_signals.py
│   │   ├── orderflow.py
│   │   ├── provider.py
│   │   ├── quote_provider.py
│   │   ├── regime_v2.py
│   │   ├── regime.py
│   │   ├── relative_strength.py
│   │   ├── runtime_cache.py
│   │   ├── session_liquidity.py
│   │   ├── smc_analysis.py
│   │   ├── snapshot_scope.py
│   │   ├── snapshots.py
│   │   ├── source_consensus.py
│   │   ├── source_registry.py
│   │   ├── structural_levels.py
│   │   ├── structure_bridge.py
│   │   ├── structure.py
│   │   ├── time_estimate.py
│   │   ├── universe.py
│   │   ├── volume_profile.py
│   │   └── volume.py
│   ├── ops/
│   │   ├── __init__.py
│   │   ├── instance_fencing.py
│   │   ├── release_manifest.py
│   │   ├── resource_guard.py
│   │   ├── restart_guard.py
│   │   └── watchdog.py
│   ├── quality/
│   │   ├── __init__.py
│   │   ├── context_relevance.py
│   │   ├── groq_schema.py
│   │   ├── integrity.py
│   │   └── setup_evidence.py
│   ├── risk/
│   │   ├── __init__.py
│   │   └── engine.py
│   ├── strategies/
│   │   ├── __init__.py
│   │   ├── activation.py
│   │   ├── base.py
│   │   ├── capture.py
│   │   ├── data_contracts.py
│   │   ├── fast.py
│   │   ├── golden_manifest.py
│   │   ├── legacy_bridge.py
│   │   ├── legacy_scan_registry.py
│   │   ├── mtf.py
│   │   ├── parity_corpus.py
│   │   ├── parity_runner.py
│   │   ├── parity.py
│   │   ├── registry.py
│   │   ├── specifications.py
│   │   ├── swing.py
│   │   ├── wyckoff.py
│   │   └── zone.py
│   ├── telemetry/
│   │   ├── __init__.py
│   │   ├── dashboard_projection.py
│   │   ├── incidents.py
│   │   ├── job_metrics.py
│   │   └── market_context.py
│   ├── ui/
│   │   ├── dashboard/
│   │   │   ├── __init__.py
│   │   │   ├── config.py
│   │   │   └── page.py
│   │   ├── telegram/
│   │   │   ├── callbacks.py
│   │   │   ├── chat.py
│   │   │   ├── commands.py
│   │   │   ├── incidents.py
│   │   │   ├── learning.py
│   │   │   ├── market_callbacks.py
│   │   │   ├── router.py
│   │   │   └── system.py
│   │   ├── __init__.py
│   │   ├── context_store.py
│   │   ├── groq_runtime.py
│   │   ├── live_position.py
│   │   ├── market_format.py
│   │   ├── price_alerts.py
│   │   ├── price_format.py
│   │   ├── profile_extraction.py
│   │   ├── risk_calculator.py
│   │   └── user_memory.py
│   └── __init__.py
├── core/
│   ├── __init__.py
│   ├── apex_v2.py
│   ├── brain_persistence.py
│   ├── coingecko_guard.py
│   ├── control_loop.py
│   ├── data_policy.py
│   ├── execution_ledger.py
│   ├── external_market_context.py
│   ├── groq_calibration.py
│   ├── groq_models.py
│   ├── historical_zones.py
│   ├── htf_close_context.py
│   ├── market_data_health.py
│   ├── market_intelligence.py
│   ├── market_structure.py
│   ├── pair_universe.py
│   ├── portfolio_dependency.py
│   ├── release_guard.py
│   ├── scan_batching.py
│   ├── session_clock.py
│   ├── setup_audit.py
│   ├── setup_evidence.py
│   ├── signal_delivery.py
│   ├── signal_integrity.py
│   ├── signal_lifecycle.py
│   ├── signal_quality_gate.py
│   ├── smc_engine.py
│   ├── source_registry.py
│   ├── strategy_catalog.py
│   ├── strategy_decisions.py
│   ├── strategy_validation.py
│   ├── telegram_dashboard.py
│   ├── trade_execution.py
│   ├── trade_manager_telegram.py
│   ├── trade_manager.py
│   └── trade_views.py
├── docs/
│   ├── APEX_V2_COVERAGE.md
│   ├── APEX_V2.md
│   ├── APEX_V3_COMPLETION_MATRIX.md
│   ├── APEX_V3_FILE_HIERARCHY.md
│   └── REVIEW_FIXES_STATUS.md
├── external_sources/
│   ├── __init__.py
│   ├── aggregator.py
│   ├── btc_mempool.py
│   ├── budget.py
│   ├── cache.py
│   ├── coinalyze.py
│   ├── coinmetrics.py
│   ├── crypto_monitor.py
│   ├── defillama.py
│   ├── deribit_options.py
│   ├── dex_liquidity.py
│   ├── exchange_fallback.py
│   ├── gate_microstructure.py
│   ├── http_client.py
│   ├── hyperliquid.py
│   ├── live_tape.py
│   ├── models.py
│   ├── oli.py
│   ├── pair_registry.py
│   ├── smart_money.py
│   ├── storage.py
│   └── whale_tracker.py
├── news_context/
│   ├── __init__.py
│   ├── aggregator.py
│   ├── official_macro.py
│   ├── sources.py
│   └── storage.py
├── scripts/
│   ├── capture_strategy_corpus.py
│   ├── capture_strategy_fixture.py
│   ├── render_controlled_release.py
│   ├── run_strategy_parity_corpus.py
│   └── verify_v3_completion.py
├── signals/
│   └── signals/
│       └── __init__.py
├── tests/
│   ├── fixtures/
│   │   ├── apex_v3_strategy_parity/
│   │   │   ├── corpus.json
│   │   │   ├── fast.json
│   │   │   ├── mtf.json
│   │   │   ├── swing.json
│   │   │   ├── wyckoff.json
│   │   │   └── zone.json
│   │   └── apex_v3_strategy_parity_verdict.json
│   ├── test_apex_v2.py
│   ├── test_apex_v3_accumulation_analysis.py
│   ├── test_apex_v3_adaptive_indicators.py
│   ├── test_apex_v3_bootstrap.py
│   ├── test_apex_v3_btc_correlation.py
│   ├── test_apex_v3_btc_direction_filter.py
│   ├── test_apex_v3_candle_patterns.py
│   ├── test_apex_v3_candle_router.py
│   ├── test_apex_v3_change_authority.py
│   ├── test_apex_v3_completion_gate.py
│   ├── test_apex_v3_context_memory.py
│   ├── test_apex_v3_context_quotes.py
│   ├── test_apex_v3_correlation.py
│   ├── test_apex_v3_cutover.py
│   ├── test_apex_v3_dashboard_config.py
│   ├── test_apex_v3_db_connection.py
│   ├── test_apex_v3_derived_context.py
│   ├── test_apex_v3_domain_models.py
│   ├── test_apex_v3_entry_timing.py
│   ├── test_apex_v3_execution_ledger_migration.py
│   ├── test_apex_v3_execution_ledger_state.py
│   ├── test_apex_v3_execution_migration.py
│   ├── test_apex_v3_execution_recovery.py
│   ├── test_apex_v3_execution.py
│   ├── test_apex_v3_gate_client.py
│   ├── test_apex_v3_gate_orderbook.py
│   ├── test_apex_v3_gate_tickers.py
│   ├── test_apex_v3_groq_calibration.py
│   ├── test_apex_v3_groq_runtime.py
│   ├── test_apex_v3_historical_context.py
│   ├── test_apex_v3_indicator_bridge.py
│   ├── test_apex_v3_infrastructure.py
│   ├── test_apex_v3_instance_fencing.py
│   ├── test_apex_v3_learning_authority.py
│   ├── test_apex_v3_learning.py
│   ├── test_apex_v3_legacy_scan_registry.py
│   ├── test_apex_v3_legacy_zones.py
│   ├── test_apex_v3_liquidation_context.py
│   ├── test_apex_v3_live_bridge.py
│   ├── test_apex_v3_live_position.py
│   ├── test_apex_v3_macro_context.py
│   ├── test_apex_v3_manager_migration.py
│   ├── test_apex_v3_manager.py
│   ├── test_apex_v3_market_context_projection.py
│   ├── test_apex_v3_market.py
│   ├── test_apex_v3_news_provider.py
│   ├── test_apex_v3_optional_context.py
│   ├── test_apex_v3_optional_prices.py
│   ├── test_apex_v3_optional_signals.py
│   ├── test_apex_v3_pending_signals.py
│   ├── test_apex_v3_price_alerts.py
│   ├── test_apex_v3_profile_extraction.py
│   ├── test_apex_v3_quality.py
│   ├── test_apex_v3_quote_provider.py
│   ├── test_apex_v3_regime_v2.py
│   ├── test_apex_v3_risk_calculator.py
│   ├── test_apex_v3_risk.py
│   ├── test_apex_v3_runtime_boundaries.py
│   ├── test_apex_v3_runtime.py
│   ├── test_apex_v3_schema_ownership.py
│   ├── test_apex_v3_session_liquidity.py
│   ├── test_apex_v3_signal_lifecycle_migration.py
│   ├── test_apex_v3_signal_persistence.py
│   ├── test_apex_v3_smc_analysis.py
│   ├── test_apex_v3_snapshot_cache_isolation.py
│   ├── test_apex_v3_source_consensus.py
│   ├── test_apex_v3_strategies.py
│   ├── test_apex_v3_strategy_activation.py
│   ├── test_apex_v3_strategy_capture.py
│   ├── test_apex_v3_strategy_golden_manifest.py
│   ├── test_apex_v3_strategy_parity_corpus.py
│   ├── test_apex_v3_strategy_parity_runner.py
│   ├── test_apex_v3_strategy_parity.py
│   ├── test_apex_v3_structure_bridge.py
│   ├── test_apex_v3_telegram_callbacks.py
│   ├── test_apex_v3_telegram_chat.py
│   ├── test_apex_v3_telegram_commands.py
│   ├── test_apex_v3_telegram_incidents.py
│   ├── test_apex_v3_telegram_learning.py
│   ├── test_apex_v3_telegram_market_callbacks.py
│   ├── test_apex_v3_telegram_router.py
│   ├── test_apex_v3_telegram_system.py
│   ├── test_apex_v3_typed_state_schema.py
│   ├── test_apex_v3_user_memory.py
│   ├── test_apex_v3_watchdogs.py
│   ├── test_brain_persistence.py
│   ├── test_candle_cache_request_limit.py
│   ├── test_closed_candle_structure.py
│   ├── test_coingecko_guard.py
│   ├── test_control_loop.py
│   ├── test_controlled_render_release.py
│   ├── test_critical_context.py
│   ├── test_dashboard_singleflight.py
│   ├── test_data_policy.py
│   ├── test_execution_ledger.py
│   ├── test_execution_only_binance.py
│   ├── test_external_sources.py
│   ├── test_fast_structural_target_rr.py
│   ├── test_groq_gpt_oss_output.py
│   ├── test_groq_models.py
│   ├── test_htf_close_context.py
│   ├── test_knowledge_pipeline.py
│   ├── test_market_data_diagnostics.py
│   ├── test_market_intelligence.py
│   ├── test_market_structure.py
│   ├── test_mtf_structural_levels.py
│   ├── test_news_context.py
│   ├── test_numeric_diagnostics_and_mtf_throughput.py
│   ├── test_observability_telemetry_only.py
│   ├── test_pair_universe.py
│   ├── test_quality_gate_fail_closed.py
│   ├── test_rr_floor_fast_balanced.py
│   ├── test_scan_batching.py
│   ├── test_scan_round_radar.py
│   ├── test_session_clock.py
│   ├── test_session_liquidity.py
│   ├── test_setup_audit.py
│   ├── test_setup_evidence.py
│   ├── test_signal_delivery.py
│   ├── test_signal_integrity.py
│   ├── test_signal_lifecycle.py
│   ├── test_signal_quality_gate.py
│   ├── test_source_budget.py
│   ├── test_strategy_catalog.py
│   ├── test_strategy_funnel_cleanup.py
│   ├── test_strategy_observability.py
│   ├── test_strategy_stats_final.py
│   ├── test_strategy_stats_post95.py
│   ├── test_strategy_tuning_trade_stats.py
│   ├── test_swing_ltf_entry_refinement.py
│   ├── test_telegram_dashboard.py
│   ├── test_trade_execution.py
│   ├── test_trade_manager_runtime.py
│   ├── test_trade_manager_v2.py
│   ├── test_trade_manager.py
│   └── test_trade_views.py
├── .env.external.example
├── .gitignore
├── bot.py
├── market.py
├── render.yaml
├── requirements-test.txt
├── requirements.txt
├── runtime.txt
└── stats_server.py
```

## Notes

- `apex/` is the canonical V3 package hierarchy.
- `apex/compatibility/`, `core/`, `market.py` and parts of `bot.py` remain compatibility boundaries while migration is still being completed.
- `external_sources/` and `news_context/` provide optional/read-only context and do not own trade execution.
- `scripts/` contains capture, parity, verification and controlled-release utilities.
- `tests/` includes the complete unit/integration/parity/regression suite.
- This file documents structure only; it does not change runtime ownership or deployment behavior.
