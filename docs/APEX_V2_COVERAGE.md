# APEX V2 implementation coverage

This matrix distinguishes code presence from an end-to-end production link. It
is intentionally conservative: an empty store or helper module is not marked
as a completed feature.

| Area | Status | Production link / remaining condition |
|---|---|---|
| Sole Manager V2, transition/risk validation, Gate market evaluation, Binance validated execution | Integrated | Existing production path; live flags remain explicit and unchanged. |
| Compact Telegram cards and Manager history | Integrated | Existing two-channel persistent message IDs and final-card path. |
| Attempt STOP ownership | Integrated in this release | Exactly one failed check can own a terminal STOP; passed checks cannot be counted as blocking. New cohorts are required because old malformed telemetry is immutable. |
| Unique LTF lifecycle | Integrated in this release | Dashboard keys events by `setup_id`; legacy events fall back to strategy/symbol/direction/TF. |
| Gate request status versus TF freshness | Integrated in this release | Dashboard exposes request result, last success, age and a diagnostic three-candle SLA separately. It does not alter strategy gates. |
| ACTUAL fill accounting | Integrated, data-dependent | Only bot-owned confirmed Binance fills count. Missing pages/fees remain unavailable, never zero. Funding is explicitly excluded. |
| NO_MANAGER / PLAYBOOK_ONLY isolation | Integrated, data-dependent | Frozen levels and quantity are independent. Post-ACTUAL-close Gate candles now create updated immutable replay bundles until virtual tracks close. |
| Replay chronology / OHLC ambiguity | Integrated in this release | Closed candles are ordered by event time; terminal-candle MFE/MAE uncertainty is explicit. |
| Execution simulator | Helper / replay-only | Tested deterministic fill model exists, but it is not yet calibrated on enough real fills and is not used to change production decisions. |
| Groq calibration | Shadow, partially integrated | Predictions now have versioned targets. Manager actions are no longer labelled with the whole-trade outcome. Per-action action-vs-HOLD outcome windows still require samples and evaluator completion. |
| Similar scenarios / walk-forward | Shadow / requires data | Time-safe retrieval scaffolding exists; reliable held-out evidence cannot be produced before closed samples accumulate. |
| Villahermosa / Holmes rules | PLAYBOOK_ONLY shadow | Never entry criteria and never auto-promoted; requires at least 30 unique eligible closed trades plus safety/tail checks. |
| Gate trades + BBO microstructure | Integrated shadow | One bounded Gate WS path stores venue-normalized USD flow/BBO and labels it `BBO_TRADE_ONLY`. |
| Gate sequence-verified depth | Integrated shadow, opt-in | Official `futures.obu` full-first stream feeds a strict snapshot/delta reducer; a sequence gap reconnects/resubscribes for a new snapshot. It is capped at three explicitly configured symbols and remains off when `APEX_GATE_DEPTH_SYMBOLS` is empty. |
| Portfolio dependency graph | Helper / producer pending | Computation now aligns closed Gate candles by event-time intersection. A scheduled producer is intentionally absent until source candle retention and API budget are explicit. |
| Source Registry / provenance | Partially integrated | Policy is enforced for registered adapters and Dashboard context. Optional sources remain disabled/context-only. |
| External API budget | Partially integrated | Persistent rolling admission exists for external adapters. If the daily plan is absent, Dashboard reports `UNCONFIGURED`; legacy scanner Gate REST is not falsely counted as covered. Binance execution has separate priority controls. |
| Backup/restore drill | Integrated helper + production evidence | Backup creation is atomic and source read-only; missing source cannot create/clobber a generation. Production logs showed successful generations after the isolated HTTP 403, whose historical root cause remains unknown. |
| Release/current-cohort Dashboard | Integrated | Main UI stays current-release-only; old malformed cohorts are not rewritten. Historical comparison remains read-only and retention-limited. |
| Strategy tuning | Not performed | Entry/SL/TP, RR>=2, CORE/TRIGGER, gates and risk remain unchanged. WYCKOFF has observations but no eligible closed-trade evidence. |

## Safety interpretation

`Integrated shadow` means data can be collected and displayed but cannot alter
entry, direction, levels, risk, Manager transitions or Binance execution.
`Requires data` is not a failed implementation: it means the statistical claim
cannot yet be made safely. `Helper` means tested code exists without a complete
runtime producer/consumer path and must not be presented as production-active.
