"""Optional read-through Market History cache for the live Gate scanner.

The adapter is enabled only when ``APEX_MARKET_DATABASE_URL`` is configured.
It never provides stale or incomplete history and never changes a strategy
definition; on any error the existing Gate path continues unchanged.
"""
from __future__ import annotations

import os
import time
from typing import Any, Iterable, Mapping

from .features import TIMEFRAME_SECONDS
from .store import ResearchStore


_store: ResearchStore | None = None


def configured() -> bool:
    return bool(os.environ.get("APEX_MARKET_DATABASE_URL", "").strip())


def store() -> ResearchStore | None:
    global _store
    if not configured(): return None
    if _store is None: _store=ResearchStore(os.environ["APEX_MARKET_DATABASE_URL"])
    return _store


def read(symbol: str,timeframe: str,limit: int) -> list[dict[str,Any]]:
    target=store()
    if target is None or timeframe not in TIMEFRAME_SECONDS: return []
    try:
        rows=target.candles(symbol,timeframe,limit=limit)
        if len(rows)<limit: return []
        last=int(rows[-1]["close_time"]); max_age=TIMEFRAME_SECONDS[timeframe]*2
        if time.time()-last>max_age: return []
        return [{"timestamp":int(x["open_time"]),"open":float(x["open"]),"high":float(x["high"]),
                 "low":float(x["low"]),"close":float(x["close"]),"volume":float(x["volume"]),
                 "_market_history":True} for x in rows]
    except Exception: return []


def write(symbol: str,timeframe: str,candles: Iterable[Mapping[str,Any]]) -> int:
    target=store()
    if target is None or timeframe not in TIMEFRAME_SECONDS: return 0
    now=int(time.time()); period=TIMEFRAME_SECONDS[timeframe]; rows=[]
    try:
        for item in candles:
            opened=int(item.get("open_time",item.get("timestamp",0)) or 0); closed=opened+period
            if not opened or closed>now: continue
            rows.append({"symbol":symbol,"timeframe":timeframe,"open_time":opened,"close_time":closed,
                         "open":item["open"],"high":item["high"],"low":item["low"],"close":item["close"],
                         "volume":item.get("volume",0),"is_closed":True,"data_quality":"VALID",
                         "payload":{"path":"live_gate_cache"}})
        return target.upsert_candles(rows)
    except Exception: return 0


__all__=["configured","read","write"]
