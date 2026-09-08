# APEX V2

APEX V2 is the control plane around the existing production strategies and
Trade Manager 2.0. It does not replace or loosen FAST, MTF, SWING, ZONE or
WYCKOFF. Entry, SL, TP, RR >= 2, CORE/TRIGGER and risk rules remain owned by
their existing deterministic layers.

## Production path

1. Gate supplies candles, price, structure and strategy market data.
2. A strategy creates an immutable candidate.
3. Signal Integrity and Setup Evidence validate geometry and causal evidence.
4. Groq performs the final bounded quality review.
5. The APEX V2 thesis freezes the original levels, evidence and version set.
6. Trade Manager 2.0 receives Gate facts, the frozen thesis, similar completed
   scenarios and a cached execution snapshot.
7. Manager validates one Groq action against its transition matrix and safety
   rules.
8. The execution layer alone may send an idempotent approved command to Binance.

Groq never calls Binance. Binance is not used for candles or scanner data.
Timeouts, malformed AI output and uncertain exchange state produce HOLD and do
not remove existing protective orders.

## Control-plane records

- `apex_v2_theses`: immutable trade passports.
- `apex_v2_market_states`: normalized Gate market-state snapshots.
- `apex_v2_portfolio_snapshots`: cached aggregate exposure and risk.
- `apex_v2_decisions`: versioned, idempotent decision audit.
- `apex_v2_opportunities`: executable/missed-opportunity outcomes.
- `apex_v2_incidents`: open and resolved operational incidents.
- Existing `trade_manager_replay_tracks`: isolated ACTUAL, NO_MANAGER and
  PLAYBOOK_ONLY results.
- `apex_v2_replay_*`: immutable Gate-candle/action capture and idempotent
  replay bundles with gross/net R, MFE/MAE, giveback, fees/slippage, duration,
  exit reason and the three counterfactual edges.
- `apex_v2_groq_calibration`: confidence/outcome calibration memory. It is
  diagnostic and cannot authorize a Manager action.
- `apex_v2_dependency_snapshots`: Gate closed-return correlation/BTC-beta
  clusters for portfolio diagnostics; existing risk limits remain unchanged.
- `apex_v2_shadow_evaluations`: old/new rule A/B statistics and a manual-only
  promotion proposal under the 30-trade evidence gate.
- `gate_microstructure_shadow`: sequence-checked Gate WS depth/taker-flow
  features. Sequence gaps require a resync; no feature claims to reveal a
  particular market maker or stop hunt.

All migrations are additive and restart-safe. No table in this control plane
is allowed to rewrite a strategy's initial levels.

## Dashboard V2

Dashboard V2 is read-only and defaults to the current release cohort. It shows:

- APEX Overview and exact version manifest;
- Strategy Lab funnel and terminal STOP truth;
- Market Data / Gate health and freshness;
- non-expired PENDING LTF lifecycle;
- Opportunity Review, including `TARGET_ALREADY_PASSED`;
- Manager actions, states and execution confirmations;
- Portfolio exposure and risk blockers;
- execution mode without secrets;
- ACTUAL/NO_MANAGER/PLAYBOOK_ONLY edge and shadow-rule evidence;
- operational incidents.
- source ownership/provenance, rolling API budget and optional daily load plan;
- Gate-only microstructure observations, Groq calibration, dependency clusters
  and replay bundles in the Learning diagnostics block.

Old release data remains stored but is never mixed into the default production
view. Book-derived rules remain shadow-only and can only become eligible for
manual review after their evidence gate; they are never auto-activated.

## Source and API budget contract

`core/source_registry.py` is the adapter contract. Gate is the only source
accepted for candles, indicators, MTF, structure and scanner rows. Coinalyze,
Hyperliquid, Coin Metrics, DefiLlama, Deribit, on-chain, DEX and news feeds are
context/shadow and are omitted on stale or failed reads. Binance has an
execution-only contract and is rejected by the market-data request guard.

Every optional HTTP attempt, including a retry, is admitted by the persistent
rolling minute/hour/day ledger. Rate-limit responses open a circuit with
`Retry-After`; cached/stale context is never promoted into a live gate. Set
`APEX_EXTERNAL_SOURCE_PLAN_JSON` to display a secret-free whole-day load
projection before enabling a new adapter.

## Replay and deployment safety

Closed Gate candles are captured once per trade and can be replayed offline
through three isolated states. Intrabar SL/TP ambiguity uses an explicit
conservative policy. The execution simulator is marked `REPLAY_ONLY` and is
never imported by the live Binance executor. `release_guard` and
`backup_restore` provide read-only canary/restart checks; they do not deploy,
rollback or enable live trading automatically.

## Later evidence-gated extensions

The interfaces intentionally allow confidence calibration and an independent
model critic. The critic must not be enabled on routine cycles and cannot gain
execution authority. It is appropriate only after sufficient closed trades
show that the added review improves net R without degrading drawdown or safety.
