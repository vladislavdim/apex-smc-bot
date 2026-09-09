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
5. Fees and slippage are estimated on entry and exit. Missing funding/basis are
   stored as unavailable, never zero.
6. Segment analytics use chronological 60/20/20 splits. Promotion stays a
   proposal and requires at least 30 eligible observations plus all safety,
   drawdown, tail and outlier gates.
7. Dashboard `Research / Shadow` reads the research DB and is labelled
   `NO REAL EXECUTION`.

## Default historical target and API budget

| Timeframe | Coverage target | Universe |
|---|---:|---:|
| 15m, 1h, 4h, 1d | 365 days | up to 80 Gate pairs |
| 5m | 365 days | configured FAST subset, default 5 pairs |

At Gate's 2,000-candle page size the initial default backfill is approximately
2,100 successful REST requests. The worker admits at most 1 request/second, 50 requests/minute and
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
| Gate 1-year OHLCV and incremental refresh | Implemented | configurable up to 730 days; 5m bounded separately; closed candles only |
| Gaps, duplicates, OHLC, listing metadata | Implemented | Issues and coverage exposed in Dashboard |
| Feature Store | Implemented foundation | Structure, OB/FVG/breaker, volume, volatility, regime, VWAP/profile, RSI/MACD, CVD proxy |
| Level lifecycle | Implemented foundation | Persisted objects; advanced reaction/sweep transitions accumulate with later workers |
| Point-in-time replay | Implemented causal shadow | Honest profile label; not falsely called an exact import of stateful live detectors |
| Filtered-candidate outcomes | Implemented | Valid geometry continues as `FILTERED_SHADOW` |
| Entry waiting, expiry, TP/SL ambiguity | Implemented | Conservative ordering |
| Costs | Partial, explicit | fees/slippage estimated; historical funding/basis unavailable until sourced |
| Feature attribution / chronological OOS | Implemented foundation | predefined causal segments; no random split or combinatorial mining |
| Ablations/interactions | Requires comparable profile runs | schema/versioning ready; no invented result |
| Strategy profiles and audit | Implemented foundation | production reference and research-v1 remain separate |
| Promotion engine | Implemented proposal gate | never automatic |
| Dashboard Research/Shadow | Implemented | coverage, jobs, runs, funnels, outcomes, features, quality, virtual positions |
| Restart/checkpoints/idempotency | Implemented | stable stream job IDs and continuous replay run |
| Gate read-through for live scanner | Implemented opt-in | only fresh, complete history; otherwise existing Gate path |
| OI/funding/long-short/liquidations/real CVD | Requires licensed historical source/data | schema accepts point-in-time context; no present-day backfill |
| Historical heatmap/order book | Requires source and storage | live Gate microstructure remains shadow; no inferred market-maker intent |
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
- With 80 pairs the initial one-year OHLCV download is approximately 2,100
  successful 2,000-row pages (including the optional five-symbol 5m subset).
  A fully caught-up 30-minute refresh projects at most about 6,600 successful
  requests/day. Defaults therefore admit only 1 request/second, 50/minute and
  12,000/day, leaving retry headroom while enforcing an absolute local ceiling.
  Dashboard exposes used, denied, error and rate-limit counters.
- SIGTERM never restarts a historical job from zero: candle, feature, and replay
  checkpoints are idempotent and resume from the last committed timestamp.
- Required: dedicated `APEX_MARKET_DATABASE_URL`.
- Optional: `APEX_RESEARCH_PAIRS`, `APEX_RESEARCH_PAIR_LIMIT`, `APEX_RESEARCH_HISTORY_DAYS`,
  `APEX_RESEARCH_FAST_PAIRS`, `APEX_RESEARCH_5M_DAYS`,
  `APEX_RESEARCH_GATE_RPS`, `APEX_RESEARCH_GATE_DAILY`.
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
- Full production detector parity, paired ablation/walk-forward evaluation and
  a provisioned isolated Research database/worker remain release prerequisites
  for the complete specification. Surrogate profiles cannot establish live edge.
