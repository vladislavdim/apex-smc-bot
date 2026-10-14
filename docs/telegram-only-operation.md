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

Use the Controlled Production Release workflow with the tested full main commit
SHA. It checks worker identity, backup credentials and fresh exact-release ready
evidence. An already Telegram-only worker with the shared GitHub lease uses one
pinned rolling deployment. The previous owner retains exclusive management until
shutdown; the replacement waits for shared ownership. A first lease-backend
migration still needs a cold stop. New entries are temporarily disabled, and an
originally enabled setting is restored only after verification. Failure keeps
new entries disabled. Existing authenticated live executions remain eligible for
reconciliation and protective management, subject to the same ownership fence.
No paid plan or exchange-account setting is changed.

The exact `apex-strategy-stats-web` service was permanently deleted with user
confirmation on 2026-10-14. Its expired PostgreSQL is retained and is not needed
for Telegram operation. The release tolerates only a 404 for that exact retired
service; it never recreates it or treats authentication failures as deletion.
Python is pinned to the same `.python-version` patch in CI and Render; the release
sets `PYTHON_VERSION` to prevent an older environment override.

Telegram-only mode persists Audit events locally and does not attempt external
dashboard ingestion. Existing diagnostic backups remain in place; no expired
database is deleted. SQLite snapshots and the ownership branch are independent
files and branches, so a diagnostic backup cannot overwrite a runtime lease.


The HTTP dashboard, its HTML and PostgreSQL dependency have been removed.
Telegram reads local State/Memory/Brain directly. Unused web formatting projections and the obsolete dashboard health component
were also removed; Telegram owns its views.

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

External async context caches retain at most 128 items and 128 cooldowns with 64
fixed single-flight locks. Separate local Audit retention keeps up to 1,000
technical events for three days, including unsynced rows, and reclaims WAL/pages;
this policy cannot run on State, Memory or Brain. Payloads are bounded to 20,000
characters with valid JSON and preserved correlation IDs.

Backup snapshot, integrity/hash, compression and Git stages release consumed file
pages with best-effort Linux advice. Stage resource logs include RSS, container
working set, anon/file bytes and sampled peaks, including Git child processes.
These OS hints preserve file contents, SQLite integrity and Git compare-and-swap;
they do not guarantee that all future workloads fit 512 MiB. Confirm the peak on
a full production Brain backup before declaring the memory issue resolved.

Manual chat scans use a dedicated two-thread executor. Cancelling an async wait
cannot create additional scanner threads while its synchronous call is still
running. Scans stop scheduling pairs after 200 seconds and report checked,
failed and planned counts; a partial scan is not presented as proof that the
entire market has no setups.
