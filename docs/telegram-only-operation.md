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
Memory, Brain and Audit backups. The `apex-runtime-lease` branch stores only an
ownership record. Every update compares the previous blob SHA and uses a unique
nonce. Separate process nonces prevent duplicate owners even if host names are
equal. Unknown responses, corruption and excessive clock skew fail closed.
Local mutation authority expires forty seconds before shared ownership, covering
bounded clock skew and in-flight exchange request timeouts.

Use the Controlled Production Release workflow with `telegram_only=true` and
the tested full main commit SHA. The Telegram release verifies the existing
worker identity and backup configuration, suspends the old worker for a cold
backend cutover, temporarily disables trading, configures the new backend and
verifies fresh ready evidence for the exact release. It then restores the
original trading-enabled setting, if it was enabled, and verifies the final
deployment. Failure leaves trading disabled until repaired. No database plan,
payment, exchange-account setting or external dashboard deployment is changed.

Telegram-only mode persists Audit events locally and does not attempt external
dashboard ingestion. Existing diagnostic backups remain in place; no expired
database is deleted. SQLite snapshots and the ownership branch are independent
files and branches, so a diagnostic backup cannot overwrite a runtime lease.
