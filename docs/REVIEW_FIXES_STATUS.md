# Review fixes and accounting limitations

Fixed locally:

- Replay checks existing barriers before close-boundary actions and freezes closed tracks.
- Replay percentages include weighted partial fills; moving to breakeven cannot loosen protection.
- Replay bundle identity includes results and engine version; action slot writes are serialized.
- Textual FAIL is not treated as a successful funnel check.
- Dictionary order-book levels produce depth features; deltas require a snapshot.
- Shadow proposals require unique closed trade IDs, finite paired results, timestamps and one strategy/rule cohort; minimum remains 30. Reported delta is recomputed.
- API planning checks synchronized minute/hour bursts as well as daily allocation, including retries.
- Dashboard response forwards the portfolio dependency snapshot.
- Unconfirmed Groq proposals are no longer captured as executions. ACTUAL uses an idempotent exchange fill ledger for bot-owned order IDs. Partial exits are quantity-weighted; USDT commissions are included. Missing fills, incomplete pages or unsupported fee assets keep net R unavailable.

The previously missing runtime dependencies have been installed. Full pytest regression now covers 352 tests plus 9 subtests. Compilation and diff whitespace checks are also required before publishing.

Accounting and rollout constraints:

1. Fill reconciliation starts after the signal closes, reads only known execution orders, and adds at most one read per minute globally with a persistent admission timestamp. Account trade reads use one attempt and weight 5: at most 7,200 additional weight/day. Algo resolution consumes a separate cycle. Existing execution requests are additional. No candle queries are added. Fees in other assets and funding are not silently estimated: the basis is after commissions, excluding funding.
2. Verify chronological replay ingestion and continuation after actual closure, legacy replay metrics, and strategy-specific shadow rules before using edge estimates for decisions.
3. Validate adapter schedules and shared-provider quotas against actual runtime configuration. Local allocations alone are not provider guarantees.
4. Green GitHub Actions are required before merge. Helpers such as the execution simulator and correlation graph builder are diagnostics, not new live trading rules.
5. Only then merge and deploy the active worker and web service; verify health and persistence.

Never describe this branch as all catalogue ideas fully integrated. New book rules remain shadow. Missing observations must remain explicitly unavailable.
