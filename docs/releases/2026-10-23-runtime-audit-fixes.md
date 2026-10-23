# Post-release audit corrections, 23 October 2026

Production baseline: da614e72e0cab3d7d16857c3415c58673ab5bb46.
Observed 28 scanner timeout events (ZONE 16, SWING 12) in the reviewed overnight window. The inner limit is 210 seconds; the scheduler had misleadingly reported its own 240-second limit.

## Root cause and correction

The snapshot builder performed each symbol's candle, derivative, trade and order-book requests serially; the scanner then processed a multi-symbol batch before reviewing and delivering any candidates. A separate liquidity request repeated candle I/O. There was no budget reserved for review/delivery. Thus network latency accumulated across a batch and the inner wait_for interrupted it.

A fresh public Gate ZONE/GRAMUSDT sample in the audit environment performed 10 calls with a summed request duration of 53.595 seconds. Bounded parallel collection completed in 15.974 seconds. These are audit-host measurements, not a production latency trace or a promised speedup on Render.

The new path bounds public HTTP concurrency to four across a shared client, joins all collection tasks, chooses the live decision timestamp after collection, and uses the same closed snapshot for the liquidity check. Explicit historical as_of still excludes later observations. Pair scans reserve time for review; unvisited coverage remains pending for the next scheduled batch. Deferred candidates persist in State before another pair is scanned and expire after 15 minutes; every candidate is rechecked for current entry validity before delivery. This is progress through a complete universe round, not a claim that each bounded cycle covers the entire universe.

## Other corrections

- Inner timeouts/shutdown retain scan ownership until the actual task has ended. A shared cancellable budget prevents new pair/review work while draining.
- Logs distinguish the 210-second scan deadline from the scheduler deadline. Repeated scanner timeout incidents escalate once to ERROR rather than disappearing as silent warnings.
- HIGH incident severity callsites now use supported severity values. Pair-specific missing data is scoped to the pair and does not toggle global activation/inhibit state.
- Canonical candles reject invalid OHLC/negative volume; snapshot construction uses strict validation and rejects internal timeframe gaps. Insufficient liquidity is UNKNOWN, not OK; fully closed tails use the latest closed candle.
- Analytical monitoring preserves uncertainty when the activation bar could have touched a barrier; history outages and unobserved entry paths produce incidents, not silent certainty. Such ambiguous outcomes do not become positive Groq calibration labels.
- Snapshot identity and strategy trace survive formatters and durable delivery storage. Telegram adds an approval label only after the final quality gate passes.
- Groq quality prompts retain evidence and the strict response schema rather than being blindly truncated. Format recovery preserves relevant evidence/context, and VALID is explicitly not an instruction to approve. Missing/invalid responses remain WAIT. Review confidence is not a win probability.
- Polling respects Telegram retry_after before ordinary transport backoff.
- Acceptance rejects degraded components and requires a recent completed universe round for all five strategies; startup READY is not operational acceptance.
- Root logging exposes informational review/scan diagnostics. No credentials are added to logs.

## Validation and release constraints

Regression checks cover native-thread drain ownership, queued-candidate restart recovery, market-data corruption/gaps, missing liquidity and closed-tail selection, activation-bar ambiguity, retry_after, trace preservation, incident escalation and false acceptance. The frozen real-market parity corpus is regenerated against the changed implementation; no fixture expected result or strategy predicate is changed to force parity.

The bounded-store test fixture previously had close prices above its declared high. Its OHLC range is corrected so it continues testing the store bound with valid candles. The old insufficient-history liquidity test is corrected to require UNKNOWN.

Production release must use one final merged SHA, after CI and duplicate-deploy checks. Execution remains unarmed/Telegram-only. Post-deploy verification must show actual scheduled work and report any partial coverage or unhealthy components. This change does not claim 48–72 hours of production acceptance, a successful real-money order drill, or positive golden coverage for every Wyckoff subtype.
