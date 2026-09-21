# APEX V3 file hierarchy

This document reflects the production hierarchy after the V3 runtime and market-context cleanup.

```
apex/
  app/
    bootstrap.py
    cutover.py
    health_server.py
    job_registry.py
    readiness.py
    runtime.py
    scheduler.py

  config/
    settings.py
    validation.py
    versions.py

  domain/
    enums.py
    ids.py
    models.py
    reason_codes.py

  db/
    connection.py
    state_db.py
    memory_db.py
    migrations.py
    maintenance.py
    ownership.py
    repositories/
    *_migration.py
    *_recovery.py

  market/
    gate_client.py
    candles.py
    snapshots.py
    provider.py
    freshness.py
    health.py
    source_registry.py
    source_consensus.py
    structure.py
    levels.py
    level_memory.py
    regime.py
    volume.py
    volume_profile.py
    derivatives.py
    orderflow.py
    microstructure.py
    live_context.py
    context_memory.py
    universe.py
    relative_strength.py
    breadth.py

  strategies/
    base.py
    registry.py
    specifications.py
    data_contracts.py
    activation.py
    fast.py
    mtf.py
    zone.py
    swing.py
    wyckoff.py
    parity*.py
    legacy_bridge.py

  quality/
    integrity.py
    setup_evidence.py
    context_relevance.py
    groq_schema.py

  risk/
    engine.py

  execution/
    plan.py
    accounting.py

  manager/
    state_machine.py
    eligibility.py
    groq_schema.py

  learning/
    live_memory.py
    live_bridge.py
    similarity.py
    statistics.py
    confidence.py
    advisory.py

  telemetry/
    incidents.py
    job_metrics.py
    dashboard_projection.py
    market_context.py

  ops/
    instance_fencing.py
    release_manifest.py
    resource_guard.py
    restart_guard.py
    watchdog.py

  ui/
    dashboard/
    telegram/
    market_format.py
    ...

  compatibility/
    legacy_market_runtime.py
    market_constants.py
    market_data.py
    market_strategy.py
    market_transport.py
    market_user_services.py

external_sources/
  Gate and optional LIVE_CONTEXT adapters only.
  These modules do not own strategy geometry or Binance execution.

core/
  Compatibility and legacy implementations that are still imported by the
  production launcher or tests. New V3 ownership must live under `apex/`.

scripts/
  Capture, parity, verification and controlled-release helpers.

tests/
  Unit, integration, parity, migration and production-boundary tests.

bot.py
  Production composition/launcher compatibility boundary.

stats_server.py
  Read-only telemetry ingest and Dashboard API.
```

## Ownership rules

- Gate is the only primary scanning market source.
- Binance is execution-only.
- `apex/market` owns canonical MarketSnapshot and LIVE_CONTEXT representation.
- `apex/telemetry` owns read-only production projections.
- `apex/strategies` owns strategy adapters, data contracts and proof-gated snapshot activation.
- `apex/db` owns State/Memory persistence.
- `apex/compatibility` and remaining `core/` files are transitional. Do not move or delete them until runtime imports and tests no longer reference them.
- Dashboard and Telegram may display state but must not mutate Entry/SL/TP/RR, risk, Manager state or exchange orders.
