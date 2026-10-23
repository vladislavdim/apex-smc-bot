# APEX V3: complete runtime audit follow-up, 23 October 2026

This batch includes the previously merged coverage database correction (PR210) and the additional defects found by cross-checking production logs, restored database snapshots and actual worker entry points. It is prepared and tested as one combined follow-up release. Auto-trading remains disabled.

## Reproduced defects and repairs

1. **FAST starvation introduced in PR209.** The wrapper allocated 90 seconds, but every pair required a 100-second remaining budget. Three production session runs reported OK with zero attempted pairs. Use the same 210-second inner budget as other scanners, below the 240-second scheduler guard. A behavioral test invokes the actual wrapper and actual implementation with the real budget context; it failed before the repair because no detector ran.
2. **Coverage health used the wrong database.** Read completed rounds from the Brain/control DB, where scanners write them, rather than the separate diagnostic Audit DB. Included from PR210.
3. **Cached approval bypass.** Fresh and cached reviews now share a final validation before claiming delivery: APPROVE, no degradation, finite numeric confidence in [current minimum, 1]. Current strategy and global approval thresholds both apply. Booleans are rejected by the strict Groq schema.
4. **Nonfinite live-execution approval.** NaN, infinity and confidence greater than one previously passed the less-than comparison before Binance admission. The execution boundary now uses the same fail-closed approval validator. Injected cases verify zero exchange calls.
5. **Candidate identity collided across strategies.** Queue keys now include strategy. Pre-existing keys migrate transactionally, retaining payload and original creation time without extending TTL. Retiring one strategy cannot remove another's work.
6. **Malformed candle boundary.** Explicit close times must agree with the candle timeframe; historical epoch zero stays historical; future closes cannot be FRESH. Strict parsing rejects malformed prices before silent row removal can hide corrupt recent data. The provider uses strict Gate normalization for both candidate and BTC reference candles.
7. **Incomplete scan reported as full in logs.** The worker now logs the actual persisted scan status, including PARTIAL. The MTF price-validation loop checks its remaining budget before starting additional calls.
8. **Old Binance rate-limit incident never recovered.** A confirmed successful response resolves the restored incident only when the current circuit is no longer blocked. Telemetry failure does not change execution behavior.
9. **Optional context with unknown age was usable.** Unknown, negative and nonfinite ages are excluded from Groq context. Nonfinite external numeric values and booleans normalize to UNKNOWN rather than NaN/infinity or false precision.
10. **News absence labelled LOW risk.** Missing calendar/headline observations are UNKNOWN. They do not create an automatic technical strategy veto.
11. **Unbounded public payloads and XML declarations.** Gate and external JSON readers cap responses at 4 MiB; news at 1 MiB. RSS/calendar parsing rejects DTD/entity declarations before ElementTree. Excess/invalid optional data becomes unavailable through the existing provider error boundary.
12. **Release-input shell interpolation.** The workflow receives the requested SHA through an environment variable and validates 40 lowercase hex digits, instead of inserting input into shell source.
13. **Telegram/profile defects.** First risk save preserves the requested value. Deposit, alerts and manual journal reject invalid/nonfinite financial values; alerts require an actual non-stale quote. Names and generated journal analysis are HTML-escaped. Alert price retrieval and journal model analysis run off the event loop.

## Verification and evidence

- Full suites passed: **1117 pytest, 841 unittest**, zero failures. Additional behavioral and fault-injection regressions cover the above boundaries. Coverage was measured separately; passing tests do not mean every line or branch was exercised.
- Strict undefined-name lint, full compilation, production-authority and release-readiness checks, dependency vulnerability audit.
- Rebuilt 33-case frozen real-market parity proof using the actual production strategy registry, with live requests forbidden. This protects existing strategy behavior; it is not an independent profitability backtest.
- Latest inspected Brain backup: 2026-10-23 18:24 UTC; State: 18:34 UTC. Both pass SQLite quick_check and foreign_key_check.
- Since first release readiness at 15:32 UTC, inspected State snapshot records: MTF 6 successful runs (max 81.4s), SWING 6 (max114.5s), ZONE 9 (max110.7s), WYCKOFF 3 (max80.3s). FAST 3 zero-pair runs exposed item1 above. Brain snapshot confirms completed 80-pair ZONE and SWING rounds. All five must have actual completed round evidence before operational acceptance passes.
- Production Telegram connection reset at18:03:30 UTC recovered at18:03:41 (one reconnect attempt).

## Practical limits

The follow-up release must be checked on Render after rollout. Startup READY alone does not prove full strategy coverage. FAST deliberately waits outside London/New York sessions; an offline wrapper regression is not a live session run. Groq is on-demand; no new market candidate means no new model verdict to observe. The available real-market corpus does not provide independent positive golden trades for every strategy/subtype/direction. No live-money order drill or auto-trading enablement is included. Interactive Render shell is not exposed by the connected tools; local shell tests and read-only restored production backups supplement Render logs/metrics.

A 48–72-hour same-release acceptance window is a separate measured requirement and cannot be declared elapsed immediately after deployment. These tests do not establish a future win rate, profitability, or the absence of every possible bug.
