"""BTC mempool risk context from public mempool.space data."""
from __future__ import annotations
import asyncio
from .cache import cache
from .http_client import http_client
from .models import number

SOURCE = "btc_mempool"

async def collect(symbol: str) -> dict:
    if symbol.upper().replace("/", "") != "BTCUSDT":
        return {"source": SOURCE, "status": "unsupported_pair", "symbol": symbol}
    async def fetch():
        recent, ticker = await asyncio.gather(
            http_client.get_json("https://mempool.space/api/mempool/recent"),
            http_client.get_json("https://api.gateio.ws/api/v4/spot/tickers", {"currency_pair": "BTC_USDT"}))
        return {"recent": recent, "ticker": ticker}
    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:BTC", 90, 900, fetch)
        ticker = payload.get("ticker", [])
        price = number(ticker[0].get("last")) if isinstance(ticker, list) and ticker and isinstance(ticker[0], dict) else None
        transfers = []
        for row in payload.get("recent", []) if isinstance(payload.get("recent"), list) else []:
            btc = (number(row.get("value")) or 0) / 100_000_000 if isinstance(row, dict) else 0
            usd = btc * price if price is not None else 0
            if usd >= 100_000:
                transfers.append(usd)
        return {"source": SOURCE, "status": status, "age_seconds": age, "symbol": symbol,
                "normalized": {"large_transfer_usd": round(sum(transfers), 2) or None,
                "large_transfer_count": len(transfers), "method": "last_10_mempool_transactions_no_direction"}}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}
