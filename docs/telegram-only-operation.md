# Telegram-only operation

The Telegram menu contains General Statistics with calendar-day filters (UTC)
and FAST, MTF, SWING, ZONE, WYCKOFF strategy filters. Scanner daily totals are
transactionally maintained in the existing backed-up compatibility database.
They survive retention of individual scanner runs. Initial totals cover the
surviving local history; inaccessible records from an expired PostgreSQL are
not invented or represented as migrated.

Actual trade statistics come only from confirmed live Memory outcomes. Missing
database sources are reported as unavailable, rather than fabricated zeros.
Learning, scanners, trades, Manager, execution and incidents retain their own
Telegram views. The statistics button and `/stats` use the same read-only view.

Set `APEX_TELEGRAM_ONLY=true` and `APEX_RUNTIME_LEASE_BACKEND=github` to use
Telegram without the external statistics PostgreSQL. This requires the existing
`GITHUB_TOKEN` with Contents write on `GITHUB_BACKUP_REPO` and the normal State,
Memory and Brain backups. The `apex-runtime-lease` branch stores only an
ownership record. Every update compares the previous blob SHA and uses a unique
nonce. Separate process nonces prevent duplicate owners even if host names are
equal. Unknown responses, corruption and excessive clock skew fail closed.
Local mutation authority expires forty seconds before shared ownership, covering
bounded clock skew and in-flight exchange request timeouts.

Use the Controlled Production Release workflow with the tested full main commit SHA. The Telegram release verifies the existing
worker identity and backup configuration, suspends the old worker for a cold
backend cutover, temporarily disables trading, configures the new backend and
verifies fresh ready evidence for the exact release. It then restores the
original trading-enabled setting, if it was enabled, and verifies the final
deployment. Failure leaves trading disabled until repaired. No database plan,
payment or exchange-account setting is changed. After worker readiness is proven,
the retired dashboard service is suspended and its commit deploys are disabled.
Its database is retained; permanent deletion is a separate action.

Telegram-only mode persists Audit events locally and does not attempt external
dashboard ingestion. Existing diagnostic backups remain in place; no expired
database is deleted. SQLite snapshots and the ownership branch are independent
files and branches, so a diagnostic backup cannot overwrite a runtime lease.


The HTTP dashboard, its HTML and PostgreSQL dependency have been removed.
Telegram reads local State/Memory/Brain directly. Pure formatting projections
live in `apex.ui.readmodels`; they have no server, HTML or database connection.

Memory controls bound Gate responses and same-key locks, prune expired candles
and cap the router, shared and SMC candle stores at 64 entries. Candle history
returned to strategies is not shortened. Git snapshot uploads fetch trees only
and avoid downloading old database blobs. The resource guard accounts for child
processes using cgroup working set as well as Python RSS. Job start/end resource
records identify residual spikes. Readiness logs emit on transitions and once
per five minutes rather than as warnings every fifteen seconds.

Detailed technical Audit events use a bounded local queue (128 detached,
size-limited payloads). Local diagnostics can be dropped under overload and do
not survive instance replacement. The old GitHub Audit archive is retained but
is no longer restored or uploaded every ten minutes; it is not a source for
Telegram statistics or trading decisions. State, confirmed Memory outcomes and
Brain (including daily scanner totals) retain their durable backups.
