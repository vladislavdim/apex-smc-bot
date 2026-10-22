# Final APEX V3 cutover work record

Baseline production/main: 0f8c9edd60ff6b66f1c3953b7ef6d7b79fd9470f.
User requires all code changes together, then one controlled deployment. Never
trigger an intermediate deploy or mark long-running acceptance complete early.

## Required work before release

- [x] Snapshot-native implementations of FAST, MTF, SWING, ZONE and three
  WYCKOFF subtypes, independent of legacy detector calls and live global reads.
- [x] Equivalent-input golden tests: successful LONG/SHORT candidates, ordered
  gates, geometry, rejection boundaries and missing/stale input cases.
- [x] Expand real frozen corpus across symbols, sessions and regimes; do not
  label synthetic data as real. Bind proof to source and data digests.
- [x] Nonzero actual cluster exposure, conservative UNKNOWN handling and a
  separately configurable budget; no invented empirical correlation.
- [x] Global shutdown deadline, mutation fence that survives stopping, safe
  lease retention with outstanding work, SIGTERM regression.
- [ ] Accurate completion/release gate, final complete CI, exact-SHA release,
  no duplicate deployment and runtime verification.
- [x] Explicit 48–72h acceptance status and verifiable live checkpoints.

The web Dashboard was intentionally retired with user confirmation on
2026-10-14 (docs/telegram-only-operation.md). Telegram remains the supported UI;
recreating a paid web service is not a completion requirement. Autotrading stays
disabled. Profitability is not a software acceptance claim.

## In progress

Baseline source verified against all 503 GitHub blobs. No production mutation
has been made in this phase. Branch: completion/final-cutover.

## Evidence and limits

- Native arithmetic/gate equivalence: eleven migrated bodies compared by AST;
  execution binds every market dependency to the input snapshot. Nested market
  data is detached and immutable. Decision clocks use `as_of`, not wall clock.
- Expanded corpus: 33 Gate cases, four symbols, eleven timestamps. Actual MTF
  LONG/SHORT and SWING LONG results match legacy geometry and ordered checks.
  FAST/ZONE/Wyckoff currently have real filtered examples; this is not a claim
  that all strategies/subtypes have a successful real-market candidate fixture.
- Corpus/version/code hashes are validated; production requires both real
  candidate directions and all three Wyckoff subtypes. An invalid proof never
  silently falls back to a legacy detector.
- Cluster risk uses 168 hourly log returns, at least 96 aligned observations,
  absolute Pearson correlation >=0.75 and connected components. Unknown or
  stale correlations take no diversification credit; opposing sides take no
  hedge credit. Actual active State exposures replace the old zero placeholder.
  Base risk/leverage are not increased. The optional cluster budget defaults
  to the existing portfolio budget.
- Shutdown has one 25-second budget, stops signed mutations and Telegram
  sends, retains the shared lease while work remains, and forces process exit
  on Render if Python executor threads cannot drain. A failed State checkpoint
  cannot release the lease. The real SIGTERM regression exercises this path.
- Final Render configuration must set `APEX_SNAPSHOT_STRATEGIES_ENABLED=true`
  by merging only that key before the single standard deploy. Render's
  environment API saves values without deploying; other settings remain intact.

## Production acceptance (pending real elapsed time)

The release log emits exact SHA, UTC timestamp, instance/fencing generation,
snapshot proof, readiness, disabled autotrading and working memory every five
minutes and on health changes. Collect actual `[APEX Release Ready]` records
as JSONL and run:

```
python scripts/verify_production_acceptance.py observations.jsonl --sha FULL_SHA --hours 48
```

A same-SHA 48–72h history is required. Missing observations, >15-minute gaps,
failed readiness, changed proof, conflicting fencing owners, unexpected
execution mode or memory >=90% of the limit fail acceptance. A new deployment
or a short READY sample does not complete it. Acceptance remains pending after
initial release verification; profitability is outside software acceptance.
