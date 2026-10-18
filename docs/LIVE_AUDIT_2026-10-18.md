# APEX production audit — 18 October 2026

Scope: Render worker `srv-da9f0d6gekts7381reo0`, live commit
`e9346b47f2be2983007fa43bf8dd382e9d8af790`, plus the corresponding
`main` checkout. The retired HTTP dashboard is outside the production path.

## Observed

- The worker deploy is `live` since 15 October. Resource logs show FAST, MTF,
  ZONE, SWING and WYCKOFF scheduled jobs starting and finishing. The sampled
  period does not prove that a strategy found a valid setup or delivered a
  Telegram signal; neither candidate nor Groq decision logs were present in
  the queried last 24 hours.
- Release health repeatedly reports `DEGRADED`, `ready=false`,
  `auto_trading_enabled=false`, `telegram_only=true`. New live Binance entries
  are disabled. The exact reason codes were absent from this release log;
  this patch adds public, bounded reason codes and component states.
- The 512 MiB worker rose from about 209 MiB after its 15 October replacement
  to about 478–500 MiB by 18 October, with one instance throughout. At
  14:19 UTC the worker reported RSS 479576064 and working set 494059520 bytes
  against a 536870912-byte limit. With the default 85% stop threshold,
  either measurement is sufficient to inhibit new entries. Cache relief had
  already fired on 15 October. The cause of sustained growth is not yet proven;
  changing the threshold would hide this condition.
- Repeated FAST scans requested `MATICUSDT` 1h and 15m candles without data.
  The reviewed FAST list was static despite Gate's migration to `POLUSDT`.
  Telegram polling had brief retryable network/429 errors on 18 October.

## Corrected in this branch

- FAST uses `POLUSDT`, and Polygon chat aliases resolve to that contract.
- At each FAST scan, only reviewed contracts with a live Gate quote are sent to
  strategy evaluation. A completely unavailable Gate quote set fails the job
  closed, retaining its scheduler/incident telemetry. Strategy geometry,
  thresholds and Entry/SL/TP/RR remain unchanged.
- Release heartbeat logs bounded readiness reason codes and unhealthy
  component names so the next production check can identify the exact gate.

## Verification and remaining release blockers

- Local: 990 pytest and 817 unittest pass with test-environment proxies unset;
  five-strategy replay exactly matches the pinned verdict; production authority,
  matrix gate, compile and diff checks pass.
- This audit cannot establish continuous market availability or real-order
  success from unit tests. Continue production monitoring, diagnose the
  sustained memory growth on the worker, and verify readiness plus Binance
  account/rules before restoring `AUTO_TRADING_ENABLED`. Do not infer a live
  trade from an analytical OHLC outcome or a delivered Telegram signal.
