# Runtime and accounting recovery — 2026-10-09

Signals 468 (ZRO SHORT), 469 (PIEVERSE LONG) and 470 (PENGU LONG) were blocked by runtime instance fencing before Binance entry submission. Their later SL records were analytical candle outcomes, not confirmed exchange losses.

Changes:

- Separate confirmed execution accounting from analytical `trade_event`. Only canonical ledger entries and fully accounted exits enter live financial statistics. Net win/loss uses net R after costs. Pending funding is not final net PnL. Event timestamps follow actual fills, retries deduplicate, and financial events have an independent bounded fetch window. Old persisted dashboard caches are invalidated by a projection version.
- Reconcile both ACTIVE and CLOSING Manager rows. Proven skipped/blocked executions with no entry close as NOT_OPENED, without invented PnL. Ambiguous and actual submitted entries remain fenced for exchange reconciliation.
- Convert database failures on worker readiness into controlled 503 responses; bound connection, statement and lock waits. Require fresh occurrence as well as fresh receipt for worker heartbeats. Seed and lock the shared lease atomically on first acquisition. Backend HTTP failures no longer masquerade as lease-owner conflicts; worker clears obsolete conflicting reason codes.
- Retain the exact pending CAS upload receipt. If GitHub committed but acknowledgement was lost, recover only the matching Git blob SHA and parent. Snapshot again on a later cycle before retiring newly staged Audit rows. A foreign blob still blocks writes.
- Correct Manager list filtering and net R display. Show inhibited entries and their runtime reasons in the overview.
- Owner-scoped read-only production diagnostics in CI for this repair branch fetch bounded, redacted logs for the two verified services. No order, deploy, service mutation or credential export is performed by diagnostics.

Validation before publication: 971 pytest tests passed; 820 unittest tests passed; five strategy parity fixtures unchanged; production authority and release-ready verification passed; workflow YAML and dashboard JavaScript checked. Regression cases include analytical SL without any fill, funding pending, net-negative TP, duplicate events, backend timeout, lost acknowledgement followed by additional local rows, foreign backup writes, and CLOSING with skipped/submitted/protected executions.

Deployment must be verified separately. The original remote brain/Audit snapshots are stale; obtain production diagnostics and preserve unsaved local data before restarting a disposable worker. Do not bypass instance fencing or place real exchange orders for validation.

## Additional production evidence and repair

CI read-only Render diagnostics on 2026-10-09 confirmed brain backup `create_blob HTTP 422` with GitHub's `input was too large` error, and repeated Audit `stale_remote`. These are distinct faults. The backup now falls back to bounded-memory native Git transport for oversized API blobs, preserves the gzip format, checks the exact previous file SHA, and never forces the branch ref. A real local bare-repository regression verifies preservation of unrelated files and rejection of a stale writer. Snapshots above GitHub's 100 MiB compressed-file limit fail explicitly.

Canonical realized percentage now includes confirmed funding, consistent with net R; financial telemetry consumes that value without adding funding twice. Production database diagnostics inspect the exact configured database using read-only connections and emit no credentials. Worker replacement remains unsafe until unsaved local brain/Audit data is preserved or persistence of its disk is verified.

## Verified infrastructure blocker

Read-only production diagnostics, GitHub run 37940440274, 2026-10-09 13:56 UTC: configured PostgreSQL `dpg-dacbivh5efls73dr0750-a` has `status=suspended`, `suspended=suspended`, and `expiresAt=2026-10-03T00:04:14.737408Z`. External read-only connection failed with SSL connection closed unexpectedly. The infrastructure suspension explains unavailable dashboard telemetry and instance lease service; exact beginning of signal blocking still needs historical endpoint logs. This is not evidence of another live worker holding the lease. The release CLI now rejects worker replacement while the configured database is suspended. Restore/upgrade of this database requires provider access and, if paid, explicit cost authorization; do not bypass instance fencing.
