# October production audit repairs

Baseline: `aa18b9e176abaa346ffdf6fdf6fba00a7d067cb0`.

This release addresses the October audit findings as one deployment. Automatic
trading settings are unchanged. No manual exchange position is adopted or closed
by the recovery inventory.

| Finding | Change and validation boundary |
| --- | --- |
| F01 memory pressure | Limit each SQLite connection to a 512 KiB page cache, disable mmap, close WS storage connections, cap depth history, defer optional snapshots under pressure and release allocator arenas after backups. Memory admission thresholds remain unchanged. Sustained production stability requires observation after deployment. |
| F02 stale signal | Mandatory uncached Gate quote after review and durable checkpoint; quote age <=15 seconds, candidate lifetime <=180 seconds; check again before sending. Unavailable or invalid quote yields WAIT. |
| F03 stale legacy candles | Actual legacy router validates timestamps, ordered contiguous bars, OHLC geometry and last closed-bar age on cache, shared and venue paths. Explicit historical snapshot scope stays deterministic. |
| F04 cancelled protection | Query and verify the real stop's status, venue symbol, side, STOP_MARKET type, close-all/reduce-only coverage and trigger grid. Repair only confirmed missing/cancelled stops with deterministic IDs, without recreating TPs. Unknown responses quarantine admission. |
| F05 disposable intent | Remote State receipt precedes entry mutations and Manager action submission. Deterministic IDs recover entry, emergency exit and stop handover acknowledgements. Startup inventory identifies unknown APEX orders and unowned positions, including alongside known positions. |
| F06 missing liquidations | Missing/unsubscribed liquidation observations remain null; they no longer overwrite other providers with fabricated zero totals. |
| F07 missing multiplier | No USD trade/OI/depth conversion without a positive verified Gate contract multiplier; rebuild the book if its multiplier changes. |
| F08 backup contention | One resource slot, bounded waiting and priority for critical State receipts. Ordinary contention returns WAIT/skip without installing an unrelated global permanent fence. |
| F09 late jobs | A timed-out shielded task retains job ownership, preventing overlapping retries. Actual late success/failure updates telemetry and component health. |
| F10 uncertain claims | Admin-only `/delivery` lists and inspects unresolved payloads/receipts. Confirm/release requires evidence and stores actor/action. Legacy unknown claims have an audited release path. Unknown outcomes never expire automatically. |
| F11 event loop blocking | Price retrieval and delivery SQLite work run in threads. WS parsers/persistence run outside the loop, with serialized reads/writes to shared tape data. |
| F12 depth retention | One day / 10,000 rows maximum, timestamp index and explicit connection close. |
| F13 manual recommendations | Manual market screen displays structure/zones only; removed imperative entry advice and fabricated SL/TP levels. |
| F14 live R | Store each valid observed price/current R independently of management events or candle availability; keep price observation time separate from decision update time. |
| F15 delivery crash window | Remote checkpoint contains immutable signal geometry, formatted recipients/payloads and stable outbox ID before Telegram. Persist per-recipient status/message IDs and checkpoint delivered State before execution. Failure after possible acceptance remains unresolved, never automatically resent. |

Shutdown prioritizes the small State store; optional Memory/Brain stores retain
scheduled checkpoints rather than extending final shutdown with large uploads.

## Verification

The local full run passed 1,045 pytest tests and 833 unittest tests. Added
regressions cover restored outboxes, evidence-based resolution, all three candle
sources, stop cancellation/unknown responses, empty-State remote exposure, an
orphan alongside a known position, missing multiplier/liquidation data, depth
retention, shielded job ownership and fresh quote checks after review. Existing
schema ownership and migration tests include State migration 26.

Compile, undefined-name checks, production authority, release-ready verification
and the unchanged five-case real-market parity verdict pass. The five corpus
cases remain a limited historical sample; this release does not assert live
profitability or complete snapshot-native strategy migration. Long-duration
memory stability cannot be established from a short post-deployment window.

## Operator recovery

- `/delivery list`: unresolved delivery IDs.
- `/delivery show ID`: immutable payload and known receipts.
- `/delivery confirm ID evidence`: record verified delivery and its analytical
  lifecycle, without an automatic exchange entry.
- `/delivery release ID evidence`: release a verified absent delivery; no
  immediate resend is triggered.
- `/delivery claims`: historical unknown claims.
- `/delivery release-claim KEY TIMESTAMP evidence`: audited release of a legacy
  claim only when it has no unresolved full outbox.

Inspect actual Telegram history before resolving an unknown response. Existing
protective orders and manually placed positions must not be blindly recreated,
closed, or adopted to clear an alert.
