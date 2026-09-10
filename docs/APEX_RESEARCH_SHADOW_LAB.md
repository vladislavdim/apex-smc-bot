# APEX Research / Shadow Lab

## Safety boundary

Research is isolated from live execution. It reads Gate history, writes only
`market_*` and `research_*` tables, and cannot call Groq, Binance, Telegram or
the production Manager. Production rules, levels, RR >= 2, gates and risk are
locked. A research result can only become a human-review proposal.

The separate database is configured with `APEX_MARKET_DATABASE_URL`. Without
that explicit variable the live scanner keeps its existing Gate path and the
Dashboard reports Research as unavailable. The research worker must never be
given Binance or Telegram credentials.

## Data flow

1. `GateHistoryClient` downloads only closed Gate futures candles.
2. `ResearchStore` upserts `(source, symbol, timeframe, open_time)` and stores
   listing metadata, quality issues, coverage and durable job checkpoints.
3. Feature snapshots are calculated **as of** each candle with shared
   deterministic structure/SMC helpers used by live APEX where those helpers
   are pure. The research profile remains explicitly versioned where the full
   live detector cannot be safely imported.
4. Point-in-time causal shadow replay advances chronologically. Candidates can
   wait for entry, expire, hit TP/SL, or remain open. Same-candle TP/SL ambiguity
   is conservative `SL_FIRST` when a lower timeframe is unavailable.
5. Fees and slippage are estimated on entry and exit. Gate funding, OI,
   liquidation aggregates and long/short ratios are joined strictly as-of.
   Recent signed public trades produce real taker CVD. Missing values are
   unavailable, never zero.
6. Segment analytics use chronological 60/20/20 splits. Promotion stays a
   proposal and requires at least 30 eligible observations plus all safety,
   drawdown, tail and outlier gates.
7. Dashboard `Research / Shadow` reads the research DB and is labelled
   `NO REAL EXECUTION`.

## Default historical target and API budget

| Timeframe | Coverage target | Universe |
|---|---:|---:|
| 1h, 4h, 1d | 365 days | up to 80 Gate pairs |
| 15m | latest 9,990 candles (~104 days) | up to 80 Gate pairs |
| 5m | latest 9,990 candles (~34 days) | configured FAST subset, default 5 pairs |

Gate's canonical futures endpoint rejects `limit` together with `from` and
`to`, and rejects data older than the latest 10,000 candles for an interval.
APEX therefore uses bounded 999-point time pages without `limit`; it never
labels unavailable older 15m/5m history as a gap and never substitutes another
venue. The initial default backfill is approximately 1,900 successful REST
requests. The worker admits at most 1 request/second, 50 requests/minute and
12,000 requests/day, retries with exponential backoff, and resumes from the
last persisted candle/checkpoint. A steady-state refresh is expected to remain
below roughly 8,000 requests/day for this default universe. These are APEX
budgets, not claims about an exchange-wide shared-IP allowance.

The one-year default is roughly half the former two-year row count before indexes and
feature snapshots. A dedicated durable PostgreSQL plan must therefore be sized
and retained explicitly. Do not point this workload at an expiring/free
database and call it production-ready.

## Coverage matrix

| Requirement | Status | Notes |
|---|---|---|
| Separate Market History DB | Implemented, configuration required | Dedicated URL; additive schema; SQLite only for local/test |
| Gate OHLCV and incremental refresh | Implemented with source retention | full requested year on 1h/4h/1d; latest ~104d on 15m and ~34d on 5m; closed candles only |
| Gaps, duplicates, OHLC, listing metadata | Implemented | Issues and coverage exposed in Dashboard |
| Feature Store | Implemented foundation | Structure, OB/FVG/breaker, volume, volatility, regime, VWAP/profile, RSI/MACD, CVD proxy |
| Level lifecycle | Implemented foundation | Persisted objects; advanced reaction/sweep transitions accumulate with later workers |
| Point-in-time replay | Implemented causal shadow | Honest profile label; not falsely called an exact import of stateful live detectors |
| Filtered-candidate outcomes | Implemented | Valid geometry continues as `FILTERED_SHADOW` |
| Entry waiting, expiry, TP/SL ambiguity | Implemented | Conservative ordering |
| Costs | Partial, explicit | fees/slippage estimated; Gate funding history is stored as shadow context, while historical basis remains unavailable |
| Feature attribution / chronological OOS | Implemented foundation | predefined causal segments; no random split or combinatorial mining |
| Ablations/interactions | Requires comparable profile runs | schema/versioning ready; no invented result |
| Strategy profiles and audit | Implemented foundation | production reference and research-v2 remain separate |
| Promotion engine | Implemented proposal gate | never automatic |
| Dashboard Research/Shadow | Implemented | coverage, jobs, runs, funnels, outcomes, features, quality, virtual positions |
| Restart/checkpoints/idempotency | Implemented | stable stream job IDs and continuous replay run |
| Gate read-through for live scanner | Implemented opt-in | only fresh, complete history; otherwise existing Gate path |
| OI/funding/long-short/liquidations | Implemented Gate shadow context | official public Gate contract statistics/funding; provider-retained history only |
| Real trade-based CVD | Implemented, recent-only | signed Gate taker trades; bounded recent tape is never presented as one-year coverage |
| Order-book/liquidity heatmap | Implemented, forward-only | bounded Gate ladder snapshots; historical depth is not fabricated and no market-maker intent is inferred |
| Two-year research results | Requires completed backfill/replay | Dashboard must show progress, not fabricated metrics |
| New strategy discovery | Research-only future profile | never auto-created or promoted live |

## Operational contract

- Start command: `python research_worker.py`.
- Processing is deliberately pair-sequential. One symbol completes Gate
  history, quality validation, features, and all five strategy replays before
  the worker releases its in-memory streams and advances to the next symbol.
- Dashboard progress is weighted from 0 to 100 for each pair: Gate history
  0–30, quality 30–35, features 35–70, five strategy replays 70–95, and
  checkpoint/manifest finalisation 95–100. Overall progress is the completed
  pair fraction plus the current pair fraction.
- Gate research admission is stored in UTC day/minute buckets in the Research
  DB, so a restart cannot reset its allowance. `APEX_RESEARCH_GATE_DAILY` and
  `APEX_RESEARCH_GATE_MINUTE` are APEX-local ceilings, not claims about the
  exchange's published limits.
- `APEX_RESEARCH_MAX_RSS_MB` pauses at a durable checkpoint before the chosen
  memory ceiling; `APEX_RESEARCH_CPU_DUTY_PERCENT` and the batch yield prevent
  continuous feature/replay calculation from monopolising a small instance.
- With 80 pairs the initial OHLCV download is approximately 1,900 successful
  999-point time pages (including the optional five-symbol 5m subset).
  A fully caught-up 30-minute refresh projects at most about 6,600 successful
  requests/day. Defaults therefore admit only 1 request/second, 50/minute and
  12,000/day, leaving retry headroom while enforcing an absolute local ceiling.
  Dashboard exposes used, denied, error and rate-limit counters.
- SIGTERM never restarts a historical job from zero: candle, feature, and replay
  checkpoints are idempotent and resume from the last committed timestamp.
- Required: dedicated `APEX_MARKET_DATABASE_URL`.
- Optional: `APEX_RESEARCH_PAIRS`, `APEX_RESEARCH_PAIR_LIMIT`, `APEX_RESEARCH_HISTORY_DAYS`,
  `APEX_RESEARCH_FAST_PAIRS`, `APEX_RESEARCH_5M_DAYS`,
  `APEX_RESEARCH_GATE_RPS`, `APEX_RESEARCH_GATE_DAILY`,
  `APEX_RESEARCH_CONTEXT_PAIRS`, `APEX_RESEARCH_MARKET_CONTEXT_ENABLED`,
  `APEX_RESEARCH_TRADE_CVD_HOURS`, `APEX_RESEARCH_TRADE_CVD_PAGES`,
  `APEX_RESEARCH_ORDERBOOK_LEVELS`.
- The web service needs only the same read URL to expose `/api/research`.
- Do not run Research on `apex-smc-bot-1`. A separate worker and a durable,
  adequately sized PostgreSQL database are required before enabling the
  historical workload. Until then the production scanner remains unchanged.
- Research failures must not affect the production worker.
- Database rollback is never automatic. Restore drills use an isolated target
  without live credentials.
- `brain.db` snapshots remain on the dedicated `brain-backups` branch and the
  Research SQLite snapshot remains a verified GitHub Release asset. Neither
  path updates `main`, so neither is a production deployment trigger.
- Production Render services use `.github/workflows/controlled-production-release.yml`.
  The workflow accepts only an exact full SHA already reachable from `main`,
  runs the complete verification suite, disables commit-triggered deployment,
  deploys the web service, verifies `/health`, and only then deploys the sole
  active worker. The suspended legacy worker is not present in the release
  allow-list.

## Replay integrity and current limits

- Each virtual track waits for its own entry fill; pre-entry prices never count
  toward MFE/MAE or TP/SL. Entry expiry, entry time and duration are persisted.
- ACTUAL comes from confirmed bot-owned fills. Gate excursions are descriptive
  context only; candle touches are distinct from confirmed target executions.
  Unknown fee assets keep net results unavailable.
- Research setup identities distinguish repeated checks from unique structures.
  Ambiguous historical STOP ownership remains explicitly unresolved.
- Previous day/week highs/lows are calculated point-in-time and materialized
  as levels. Missing external features remain unavailable.
- All six market-context features have role `SHADOW_CONTEXT` and
  `execution_authority=false`. They may be segmented by outcome only after the
  observation existed. They cannot change Entry, SL, TP, RR, direction, a
  production gate, position size or Manager action.
- Research run boundaries are aligned to an immutable UTC-day cohort, so a
  retry/restart resumes the same run instead of creating a second-level
  duplicate. Gate derivatives history is explicitly limited to the provider's
  last 180 days; trade CVD is recent-only and order-book depth forward-only.
- Strategy Lab's public dashboard uses a fixed cohort baseline
  (`APEX_STATS_BASELINE_UTC`, currently `2026-09-10T07:55:47Z`). Deploys do not
  reset counters. Change this baseline only with a validated formula/settings
  promotion; then a new cohort deliberately starts at zero.
- Dashboard aggregation is single-flight and cached for 45 seconds. Concurrent
  refreshes receive the last completed result while one request rebuilds it;
  failed rebuilds retain the last good value. Audit retry backlogs are sent in
  batches of at most 20 events to remain below the 2 MB ingest boundary and to
  avoid the request bursts that previously coincided with Render restarts.
- Full production detector parity, paired ablation/walk-forward evaluation and
  a provisioned isolated Research database/worker remain release prerequisites
  for the complete specification. Surrogate profiles cannot establish live edge.
