# Verified corrections integrated on 26 October 2026

Base: edf0aa6e7b6cb2c8597f9edadac165277d256241.
This change combines the previously tested corrections below. It does not add a
new trading algorithm, change live risk parameters, enable execution, or deploy.

| Finding | Applied correction |
| --- | --- |
| R3-01 | MTF LTF-watch uses the canonical proof-gated snapshot admission path. |
| R3-02 | Manual deep MTF scan uses the same canonical admission path. |
| R3-03 | Requested but inactive snapshot activation blocks legacy fallback. |
| R3-04 | Manual summary keeps current grade labels and says Groq review was not performed. |
| R5-01 | Delivery intents bind to canonical signal identity; recovery reuses the existing signal, including terminal signals, instead of reopening it. |
| R10-01 | REST orderbook age is measured at the decision clock, not the receipt clock. |
| R13-01 | ZONE counts interval overlaps after zone formation, preserving the existing forty-bar window, final-three-bar exclusion and threshold. |
| R19-01, implemented boundaries | Historical critical-State restore and missing remote State cannot mark startup ready or silently initialize an empty replacement. |

## State migration and recovery implications

Migration 28 adds a nullable unique delivery identity to canonical signal rows and
an identity version to delivery outbox rows. Existing outbox rows remain version0,
unlinked, and require reconciliation before confirmation. The migration does not
invent links from old candidate geometry. Existing canonical terminal signals stay
terminal when a delivery identity is confirmed again.

State history fallback retains the recovered local file for inspection but blocks
startup and checkpoint overwrite. HTTP404 does not authorize automatic bootstrap.
First provisioning or recovery requires an explicit operator procedure and review;
this change does not provide an automatic unblock command. Existing valid current
State still restores normally. A stale but valid current head has no independent
trusted high-water mark: its recency remains unproven. These guards do not recover
lost records or prove exchange/Telegram state.

## Validation and compatibility

New portable recovery regression tests construct synthetic temporary databases
using the current schema; no production databases or credentials are used. New
ZONE tests independently check OB/FVG birth and overlap rules. The existing strict
AST legacy-equivalence guard now permits only the exact reviewed ZONE counter
replacement and still compares every other statement against the frozen legacy
implementation.

The packaged activation verdict was regenerated after the market/native changes.
All 33 frozen cases remain compatible. This self-parity corpus has no independent
profitability oracle and is not evidence of a profitable strategy.

## Explicit exclusions

R4-01 durable LTF handoff/typed-sender integration and R11-01 WYCKOFF chronology
remain unfinished prototypes and are excluded. No R15/R17/R18 queue/reconciliation
prototype is installed. Existing production operational memory/timeouts findings
are not repaired by these code changes. No promise is made that all bugs are fixed
or that trading accuracy/profitability has improved.

Local validation of the combined change: 1198 pytest tests passed (87.37s),
841 unittest tests passed (23.31s; overlapping coverage), plus 77 additional
synthetic ZONE/point-in-time-context research contracts passed (27.94s).
Compilation, Ruff F821/F822/F823, production-authority verification and V3
release-ready verification passed. The 33-case activation verdict is ready and
bound to implementation eacacccbaa0ae015c2fb752a19b8d31e55546f7057e919ca7da540c1ed0fcc00.
Dependency audit and GitHub CI status are reported separately in the PR.

## Follow-up: all delivered signals in trade history

At the user's request, active/closed/take/stop tabs now include delivered
analytical signals even when Binance auto-entry is disabled. Rows explicitly
identify analytical results versus confirmed Binance position results. Waiting
entry and cancelled/ambiguous outcomes are not turned into fabricated stop/take
results. The dedicated analytical-signals tab remains available.

Reads use one SQLite snapshot and apply each outcome filter before the result
limit. A confirmed managed position appears once and its Manager outcome takes
priority over a conflicting analytical result. A position reference without a
Manager row no longer hides the delivered signal; its outcome remains labelled
analytical. This changes display only, not lifecycle transitions or trade history.

The change does not establish that a specific historical notification was saved,
activated or closed correctly. That requires the relevant State rows. It also
does not repair the separately observed FAST timeout or incomplete LTF recovery.
