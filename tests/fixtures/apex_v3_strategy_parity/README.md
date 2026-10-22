# Frozen Gate parity corpus

The original five BTC cases remain byte-for-byte unchanged. The expanded v2
manifest includes historical candles fetched from the public Gate USD-M
`/api/v4/futures/usdt/candlesticks` endpoint on 2026-10-22. Candle fields are
venue observations; none were edited to manufacture a setup. Historical
funding, sentiment and orderbook are UNKNOWN rather than current observations
backfilled into an earlier decision. BTC reference candles are frozen at each
case's `as_of`. Every file is pinned by a canonical-content SHA-256 digest.

Coverage spans BTC, ETH, ADA and DOGE, London/New York and off-session decisions,
and dates from September through October 2026. Actual candidate cases include
MTF LONG on BTC/ETH (1h), MTF SHORT on DOGE (4h), and SWING LONG on ADA (4h).
FAST, ZONE and all three Wyckoff subtypes are exercised as filtered cases;
this corpus does **not** claim successful examples for every strategy/subtype.
The independent AST regression additionally compares all eleven migrated
calculation bodies with their previous arithmetic, thresholds and ordered gates.

Public Gate history rejects requests older than the latest 10,000 intervals;
failed older requests were not used as fixtures. The stored closed candles and
inputs are sufficient to replay without network or private credentials:

```
TELEGRAM_TOKEN=123456:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA \
  python scripts/run_strategy_parity_corpus.py --output /tmp/parity-verdict.json
cmp /tmp/parity-verdict.json tests/fixtures/apex_v3_strategy_parity_verdict.json
```

The schema-v2 verdict binds fixture digests and outcomes to the calculation,
market, domain, adapter and audit source hashes. Production admission requires
matching parity, multiple dates/symbols/cases, both real candidate directions
and all Wyckoff subtypes. This proves preservation of the migrated calculations;
it does not estimate a win rate, profitability, or probability of reaching TP.
