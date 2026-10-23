# Cold-start repair after the combined runtime audit release

The Render deployment of `68f45d98fab5fe63683670085ba41f42964287da` was triggered once and reached platform `live`, but the worker did not reach application READY. The queue introduced in PR209 created `scan_candidate_queue` at runtime without registering it in the State ownership contract or normal migration chain. The next deployment restored a State backup containing that table and rejected it as `state_unexpected:scan_candidate_queue`.

This was reproduced directly against the inspected production State/Memory backups. SQLite quick_check and foreign_key_check both passed: structural integrity alone did not cover the ownership/startup contract.

## Repair

- Register the candidate queue as State-owned.
- Add idempotent State migration27 with CREATE TABLE IF NOT EXISTS and a strategy/creation index. Existing candidate payloads, identities and creation times remain intact.
- Log the original polling/bootstrap traceback before final shutdown can hard-exit on its checkpoint deadline. The earlier State shutdown deadline error masked the initiating ownership error.
- Add a regression that creates a version26 State, persists a runtime candidate, reopens/migrates it, verifies ownership and checks unchanged candidate contents and TTL. It failed before the repair and passes afterwards.

## Validation

- 1124 pytest and 841 unittest, zero failures; strict undefined-name lint, compilation, authority and release-readiness checks.
- Copies of all three production backups were migrated and checked. Actual signal lifecycle, execution, execution-ledger and Manager import/parity chains passed.
- The actual full worker initializer reached READY on these copies with external network operations forbidden. Remote restore/checkpoint acknowledgements, the shared lease, the Gate freshness probe and Telegram transport were replaced by controlled test boundaries. This establishes local startup/storage compatibility; it does not certify live credentials, market access or a production ownership handoff.
- The 33-case real-market strategy proof is unchanged (`1af9128617b0fd241348b8f9b1a645fb71c10473be1a69fa5dc0080bfc23140e`).

No second deployment is included in these code changes. The user's explicit one-deployment limit was consumed by the failed-start release; an additional corrective rollout requires renewed authorization. The application must not be reported as healthy or fully fixed on Render until that rollout and its actual readiness checks succeed.
