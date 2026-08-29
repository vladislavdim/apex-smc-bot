"""Lifecycle orchestration for coverage, live tape and zone memory."""
from __future__ import annotations
import asyncio,logging
from typing import Any,Callable
from external_sources.live_tape import start as start_live_tape,stop as stop_live_tape
from external_sources.pair_registry import coverage_summary,refresh_pair_registry
try:from .historical_zones import refresh_zones
except ImportError:from historical_zones import refresh_zones
_cursor=0;_started=False

async def refresh_historical_zone_batch(symbols:list[str],candle_fetcher:Callable[[str,str,int],list[dict[str,Any]]],batch_size=12):
    global _cursor
    if not symbols:return {"status":"empty","updated":0}
    start_index=_cursor%len(symbols);batch=(symbols+symbols)[start_index:start_index+min(batch_size,len(symbols))];_cursor=(start_index+len(batch))%len(symbols);semaphore=asyncio.Semaphore(4)
    async def one(symbol,timeframe):
        async with semaphore:
            try:
                candles=await asyncio.wait_for(asyncio.to_thread(candle_fetcher,symbol,timeframe,220),timeout=15)
                if candles:return (await asyncio.to_thread(refresh_zones,symbol,timeframe,candles)).get("status")=="updated"
            except Exception as exc:logging.debug("[ZoneMap] %s %s: %s",symbol,timeframe,type(exc).__name__)
            return False
    outcomes=await asyncio.gather(*(one(symbol,tf) for symbol in batch for tf in ("15m","1h","4h","1d")))
    return {"status":"updated","pairs":len(batch),"updated":sum(outcomes),"next_cursor":_cursor}

async def start_market_intelligence(symbols,candle_fetcher):
    global _started
    registry=await refresh_pair_registry(symbols);tape=await start_live_tape(symbols);zones=await refresh_historical_zone_batch(symbols,candle_fetcher);_started=True;summary=coverage_summary(symbols);logging.info("[MarketIntelligence] coverage=%s tape=%s zones=%s",summary,tape,zones);return {"registry":len(registry),"coverage":summary,"tape":tape,"zones":zones}
async def refresh_market_intelligence(symbols,candle_fetcher):
    if not _started:return await start_market_intelligence(symbols,candle_fetcher)
    await refresh_pair_registry(symbols);await start_live_tape(symbols);return {"coverage":coverage_summary(symbols),"zones":await refresh_historical_zone_batch(symbols,candle_fetcher)}
async def stop_market_intelligence():
    global _started
    await stop_live_tape();_started=False
