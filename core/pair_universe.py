"""Select a liquid APEX universe tradable on both Gate and Binance USD-M."""

from __future__ import annotations

from typing import Any


DEFAULT_UNIVERSE_SIZE = 120
MIN_GATE_QUOTE_VOLUME = 250_000.0
MAX_GATE_SPREAD_PCT = 0.25


# Used only while either public instrument endpoint is unavailable.  The live
# refresh replaces this list hourly and never admits a symbol unless the exact
# same USDT perpetual name is trading on both exchanges.
FALLBACK_COMMON_PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "ZECUSDT", "XRPUSDT", "PROMUSDT",
    "TRUMPUSDT", "BTRUSDT", "HYPEUSDT", "UNIUSDT", "DOGEUSDT", "ZKCUSDT",
    "BNBUSDT", "ENAUSDT", "SUIUSDT", "PUMPUSDT", "WLDUSDT", "SKRUSDT",
    "ADAUSDT", "AKEUSDT", "BEATUSDT", "ZKPUSDT", "LINKUSDT", "GIGGLEUSDT",
    "BTWUSDT", "TUTUSDT", "CYSUSDT", "UAIUSDT", "NEARUSDT", "TAOUSDT",
    "AAVEUSDT", "MAGMAUSDT", "DOSUSDT", "LTCUSDT", "AVAXUSDT", "BCHUSDT",
    "NILUSDT", "ONGUSDT", "XMRUSDT", "FARTCOINUSDT", "LABUSDT", "ONDOUSDT",
    "DEXEUSDT", "PENGUUSDT", "HEMIUSDT", "XLMUSDT", "FETUSDT", "FILUSDT",
    "AUCTIONUSDT", "DOTUSDT", "INJUSDT", "LITUSDT", "TRXUSDT", "BICOUSDT",
    "COTIUSDT", "DASHUSDT", "XAUTUSDT", "ICPUSDT", "POLUSDT", "MOVRUSDT",
    "COLLECTUSDT", "ZKUSDT", "VELVETUSDT", "APTUSDT", "WLFIUSDT",
    "ESPORTSUSDT", "XPLUSDT", "WIFUSDT", "ACEUSDT", "CLOUSDT", "HOMEUSDT",
    "ASTERUSDT", "TNSRUSDT", "ARBUSDT", "VIRTUALUSDT", "BANKUSDT", "ETCUSDT",
    "BROCCOLIF3BUSDT", "ETHFIUSDT", "BOMEUSDT", "TIAUSDT", "OPGUSDT",
    "VVVUSDT", "CHIPUSDT", "BLESSUSDT", "XANUSDT", "KAITOUSDT", "SKYAIUSDT",
    "PIEVERSEUSDT", "REUSDT", "PAXGUSDT", "UBUSDT", "LIGHTUSDT", "EGLDUSDT",
    "TACUSDT", "HBARUSDT", "AIOUSDT", "STXUSDT", "GRVTUSDT", "MAGICUSDT",
    "ZORAUSDT", "CHILLGUYUSDT", "ZBTUSDT", "ZROUSDT", "ORDIUSDT", "GRAMUSDT",
    "CRVUSDT", "HUMAUSDT", "ROBOUSDT", "ALLOUSDT", "ERAUSDT", "HEIUSDT",
    "SLXUSDT", "MUBARAKUSDT", "BMTUSDT", "USUSDT", "CAPUSDT", "PEOPLEUSDT",
    "MONUSDT", "GALAUSDT",
]


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def select_common_pairs(
    gate_tickers: Any,
    binance_exchange_info: Any,
    *,
    limit: int = DEFAULT_UNIVERSE_SIZE,
) -> list[str]:
    """Return liquid exact-name USDT perpetuals shared by Gate and Binance.

    Exact-name matching is deliberate: it excludes contracts such as Gate
    ``PEPE_USDT`` versus Binance ``1000PEPEUSDT`` so live execution cannot send
    levels calculated for a differently scaled instrument.
    """
    if not isinstance(gate_tickers, list) or not isinstance(binance_exchange_info, dict):
        return []
    binance_symbols = {
        str(row.get("symbol", "")).upper()
        for row in binance_exchange_info.get("symbols", [])
        if isinstance(row, dict)
        and row.get("contractType") == "PERPETUAL"
        and row.get("status") == "TRADING"
        and row.get("quoteAsset") == "USDT"
        and row.get("marginAsset") == "USDT"
    }
    ranked: list[tuple[float, str]] = []
    for row in gate_tickers:
        if not isinstance(row, dict):
            continue
        contract = str(row.get("contract", "")).upper()
        if not contract.endswith("_USDT"):
            continue
        symbol = contract.replace("_", "")
        base = symbol[:-4]
        if symbol not in binance_symbols or len(base) < 2 or not base.isascii() or not base.isalnum():
            continue
        volume = _number(row.get("volume_24h_quote") or row.get("volume_24h_settle"))
        bid, ask = _number(row.get("highest_bid")), _number(row.get("lowest_ask"))
        midpoint = (bid + ask) / 2
        spread_pct = ((ask - bid) / midpoint * 100) if midpoint > 0 and ask >= bid else 999.0
        if volume < MIN_GATE_QUOTE_VOLUME or spread_pct > MAX_GATE_SPREAD_PCT:
            continue
        ranked.append((volume, symbol))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [symbol for _, symbol in ranked[: max(1, int(limit))]]
