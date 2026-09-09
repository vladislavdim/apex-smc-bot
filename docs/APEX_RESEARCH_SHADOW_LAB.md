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
| 15m, 1h, 4h, 1d | 730 days | up to 80 Gate pairs |
| 5m | 365 days | configured FAST subset, default 5 pairs |

At Gate's 2,000-candle page size the initial default backfill is approximately
4,100 successful REST requests. The worker admits at most 2 requests/second and
12,000 requests/day, retries with exponential backoff, and resumes from the
last persisted candle/checkpoint. A steady-state refresh is expected to remain
below roughly 8,000 requests/day for this default universe. These are APEX
budgets, not claims about an exchange-wide shared-IP allowance.

The two-year default is roughly 7.8 million OHLCV rows before indexes and
feature snapshots. A dedicated durable PostgreSQL plan must therefore be sized
and retained explicitly. Do not point this workload at an expiring/free
database and call it production-ready.

## Coverage matrix

| Requirement | Status | Notes |
|---|---|---|
| Separate Market History DB | Implemented, configuration required | Dedicated URL; additive schema; SQLite only for local/test |
| Gate 2-year OHLCV and incremental refresh | Implemented | 5m bounded separately; closed candles only |
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
- Required: dedicated `APEX_MARKET_DATABASE_URL`.
- Optional: `APEX_RESEARCH_PAIRS`, `APEX_RESEARCH_PAIR_LIMIT`,
  `APEX_RESEARCH_FAST_PAIRS`, `APEX_RESEARCH_5M_DAYS`,
  `APEX_RESEARCH_GATE_RPS`, `APEX_RESEARCH_GATE_DAILY`.
- The web service needs only the same read URL to expose `/api/research`.
- Research failures must not affect the production worker.
- Database rollback is never automatic. Restore drills use an isolated target
  without live credentials.
