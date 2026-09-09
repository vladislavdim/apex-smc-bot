"""Bounded Gate historical-candle backfill with durable checkpoints."""
from __future__ import annotations

import os
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests

from core.pair_universe import FALLBACK_COMMON_PAIRS
from external_sources.pair_registry import get_pair
from .features import TIMEFRAME_SECONDS, validate_candles
from .store import ResearchStore, stable_id


GATE_CANDLES_URL = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
GATE_CONTRACTS_URL = "https://api.gateio.ws/api/v4/futures/usdt/contracts"


class ResearchBudget:
    """Conservative local admission control for optional research requests."""
    def __init__(self, per_second: float = 1.0, daily: int = 12000, *,
                 minute: int = 50, store: ResearchStore | None = None):
        self.minimum_interval = 1.0 / max(0.1, float(per_second))
        self.daily = max(100, int(daily)); self._last = 0.0
        self.minute = max(1, int(minute)); self.store = store
        self._day = datetime.now(timezone.utc).date(); self._used = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            today = datetime.now(timezone.utc).date()
            if today != self._day:
                self._day, self._used = today, 0
            if self.store:
                if not self.store.admit_api_request("GATE_RESEARCH", daily_limit=self.daily,
                                                    minute_limit=self.minute):
                    raise RuntimeError("research Gate persisted request budget exhausted")
            elif self._used >= self.daily:
                raise RuntimeError("research Gate daily request budget exhausted")
            wait = self.minimum_interval - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic(); self._used += 1

    @property
    def used_today(self) -> int:
        if self.store:
            return int(self.store.api_usage("GATE_RESEARCH")["day"].get("used") or 0)
        return self._used


class GateHistoryClient:
    def __init__(self, *, session: requests.Session | None = None, budget: ResearchBudget | None = None,
                 store: ResearchStore | None = None):
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "APEX-Research/1.0"})
        self.budget = budget or ResearchBudget(
            float(os.environ.get("APEX_RESEARCH_GATE_RPS", "1")),
            int(os.environ.get("APEX_RESEARCH_GATE_DAILY", "12000")),
            minute=int(os.environ.get("APEX_RESEARCH_GATE_MINUTE", "50")), store=store,
        )

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        last: Exception | None = None
        for attempt in range(5):
            self.budget.acquire()
            try:
                response = self.session.get(url, params=params, timeout=20)
                if response.status_code in {418, 429} or response.status_code >= 500:
                    if self.budget.store:
                        self.budget.store.record_api_result("GATE_RESEARCH",
                            rate_limited=response.status_code in {418,429}, error=response.status_code>=500)
                    retry_after = float(response.headers.get("Retry-After") or 0)
                    time.sleep(min(60.0, max(retry_after, 2 ** attempt + random.random())))
                    last = RuntimeError(f"Gate HTTP {response.status_code}")
                    continue
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                if self.budget.store:
                    self.budget.store.record_api_result("GATE_RESEARCH",error=True)
                last = exc
                if attempt < 4:
                    time.sleep(min(30.0, 2 ** attempt + random.random()))
        raise RuntimeError(f"Gate history unavailable: {last}")

    def contract_metadata(self) -> dict[str, dict[str, Any]]:
        data = self._get(GATE_CONTRACTS_URL)
        return {str(row.get("name") or ""): row for row in data if isinstance(row, dict) and row.get("name")}

    def candles(self, symbol: str, timeframe: str, start: int, end: int) -> list[dict[str, Any]]:
        if timeframe not in TIMEFRAME_SECONDS:
            raise ValueError(f"unsupported timeframe {timeframe}")
        pair = get_pair(symbol)
        contract = str(pair.get("gate_symbol") or symbol.replace("USDT", "_USDT"))
        data = self._get(GATE_CANDLES_URL, {
            "contract": contract, "interval": timeframe, "from": int(start), "to": int(end), "limit": 2000,
        })
        now = int(time.time()); period = TIMEFRAME_SECONDS[timeframe]; rows=[]
        for raw in data if isinstance(data, list) else []:
            if not isinstance(raw, dict):
                continue
            opened = int(float(raw.get("t") or 0)); closed = opened + period
            try:
                row = {"symbol": symbol.upper(), "timeframe": timeframe,
                       "open_time": opened, "close_time": closed,
                       "open": float(raw.get("o")), "high": float(raw.get("h")),
                       "low": float(raw.get("l")), "close": float(raw.get("c")),
                       "volume": float(raw.get("v") or 0),
                       "quote_volume": float(raw.get("sum")) if raw.get("sum") is not None else None,
                       "is_closed": closed <= now, "data_quality": "VALID", "payload": {"contract": contract}}
            except (TypeError, ValueError):
                continue
            if row["is_closed"]:
                rows.append(row)
        rows.sort(key=lambda x: x["open_time"])
        return rows


def configured_universe() -> list[str]:
    raw = [x.strip().upper().replace("_", "") for x in os.environ.get("APEX_RESEARCH_PAIRS", "").split(",") if x.strip()]
    limit = max(1, min(int(os.environ.get("APEX_RESEARCH_PAIR_LIMIT", "80")), 120))
    values = raw or FALLBACK_COMMON_PAIRS
    return list(dict.fromkeys(values))[:limit]


def target_ranges(now: int | None = None) -> dict[str, tuple[int, int]]:
    end = int(now or time.time())
    two_years = 730 * 86400; fast_days = max(180, min(int(os.environ.get("APEX_RESEARCH_5M_DAYS", "365")), 730))
    return {"15m": (end-two_years,end), "1h": (end-two_years,end),
            "4h": (end-two_years,end), "1d": (end-two_years,end), "5m": (end-fast_days*86400,end)}


def backfill_pair(store: ResearchStore, client: GateHistoryClient, symbol: str, timeframe: str,
                  start: int, end: int, *, should_stop=None, on_progress=None) -> dict[str, Any]:
    period = TIMEFRAME_SECONDS[timeframe]
    # Stable across rolling refresh windows: a new cycle resumes this stream.
    job_id = stable_id("backfill", symbol, timeframe)
    existing = store.job(job_id)
    cursor = max(start, int(existing.get("last_timestamp") or 0) + period,
                 (store.max_open_time(symbol, timeframe) or 0) + period)
    total = max(1, (end-start)//period); completed = max(0, (cursor-start)//period)
    store.checkpoint(job_id, job_type="BACKFILL", symbol=symbol, timeframe=timeframe,
                     range_start=start, range_end=end, last_timestamp=cursor-period,
                     completed_units=completed, total_units=total, status="RUNNING")
    pages=0; inserted=0
    try:
        while cursor <= end-period:
            if should_stop and should_stop():
                store.checkpoint(job_id,job_type="BACKFILL",symbol=symbol,timeframe=timeframe,
                    range_start=start,range_end=end,last_timestamp=max(start,cursor-period),
                    completed_units=completed,total_units=total,status="PAUSED")
                return {"job_id":job_id,"status":"PAUSED","pages":pages,"upserts":inserted}
            page_end = min(end, cursor + period * 1999)
            candles = client.candles(symbol,timeframe,cursor,page_end)
            if candles:
                if int(candles[0]["open_time"]) > cursor:
                    store.save_quality_issue(symbol,timeframe,"MISSING_CANDLES",open_time=cursor,
                        severity="WARNING",detail={"expected":cursor,"observed":candles[0]["open_time"],"page_boundary":True})
                inserted += store.upsert_candles(candles)
                for issue in validate_candles(candles,timeframe):
                    store.save_quality_issue(symbol,timeframe,issue["type"],open_time=issue.get("open_time"),
                                             severity=issue.get("severity","WARNING"),detail=issue)
                last=max(x["open_time"] for x in candles)
            else:
                last=page_end-period
            cursor=max(cursor+period,last+period); completed=min(total,max(0,(cursor-start)//period)); pages+=1
            store.checkpoint(job_id,job_type="BACKFILL",symbol=symbol,timeframe=timeframe,
                             range_start=start,range_end=end,last_timestamp=last,
                             completed_units=completed,total_units=total,status="RUNNING")
            if on_progress:
                on_progress(completed,total)
        count=store.candle_count(symbol,timeframe)
        store.update_coverage("OHLCV",source="GATE",symbol=symbol,timeframe=timeframe,start=start,end=end,
                              quality="VALID",availability="HISTORICAL",samples=count,
                              metadata={"idempotent":True,"closed_only":True})
        store.checkpoint(job_id,job_type="BACKFILL",symbol=symbol,timeframe=timeframe,
                         range_start=start,range_end=end,last_timestamp=end-period,
                         completed_units=total,total_units=total,status="COMPLETED")
        return {"job_id":job_id,"status":"COMPLETED","pages":pages,"upserts":inserted,"candles":count}
    except Exception as exc:
        store.checkpoint(job_id,job_type="BACKFILL",symbol=symbol,timeframe=timeframe,
                         range_start=start,range_end=end,last_timestamp=max(start,cursor-period),
                         completed_units=completed,total_units=total,status="FAILED",error=str(exc))
        raise


__all__ = ["GateHistoryClient", "ResearchBudget", "backfill_pair", "configured_universe", "target_ranges"]
