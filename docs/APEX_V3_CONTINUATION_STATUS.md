# APEX V3 continuation — 2026-09-27

Base: `942f59c7188f33cbd23250bff8197889fdba7ee3` on
`feature/apex-v3-final-architecture`.

## Completed in this continuation

- Moved production shutdown composition into `apex/app/shutdown.py` with
  injected dependencies. Worker retains only dependency assembly.
- Lease-release, marker and backup failures do not skip remaining cleanup.
  Polling initialization failures now invoke shutdown after partial startup.
- Restored the Execution-owned protection state machine from commit `797d838`,
  retaining the newer protection-request API. Manager's compatibility imports
  resolve again.
- Restored Telegram incident and live-learning projections from `f941db4`,
  retaining newer line-format helpers and exposing the router entry alias.
- Removed pytest module-name collision by renaming the unit primitive test file.
  Updated stale tests to keyword-only APIs and explicit confirmed-position checks.

## Verification

- `python -m pytest -q`: 855 passed.
- `python -m unittest discover -s tests -q`: 747 passed.
- Compile and `git diff --check`: passed.
- No production process, real orders, merge or deployment invoked.

## Still open

This is not a completed V3 release. `apex/app/worker.py` still contains substantial
scanner, lifecycle and infrastructure composition. Continue transferring real
implementations with behavior tests, audit compatibility call paths and Dashboard
ownership, then verify strategy parity and restart/reconciliation behavior.
A green local suite does not establish live acceptance or snapshot parity proof.
