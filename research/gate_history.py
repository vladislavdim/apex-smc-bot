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
    history_days = max(90, min(int(os.environ.get("APEX_RESEARCH_HISTORY_DAYS", "365")), 730))
    history = history_days * 86400
    fast_days = max(90, min(int(os.environ.get("APEX_RESEARCH_5M_DAYS", "365")), history_days))
    return {"15m": (end-history,end), "1h": (end-history,end),
            "4h": (end-history,end), "1d": (end-history,end), "5m": (end-fast_days*86400,end)}


def backfill_pair(store: ResearchStore, client: GateHistoryClient, symbol: str, timeframe: str,
                  start: int, end: int, *, should_stop=None, on_progress=None) -> dict[str, Any]:
    period = TIMEFRAME_SECONDS[timeframe]
    # Gate candles are interval-aligned.  Never checkpoint an arbitrary wall
    # clock boundary as if it were an exchange candle boundary.
    start = (int(start) // period) * period
    end = (int(end) // period) * period
    # Stable across rolling refresh windows: a new cycle resumes this stream.
    job_id = stable_id("backfill", symbol, timeframe)
    existing = store.job(job_id)
    checkpoint = existing.get("last_timestamp")
    cursor = max(start, int(checkpoint) + period) if checkpoint is not None else start
    # A prior empty page or detected gap is a repairable checkpoint.  Rewind
    # only this stream (and only inside the requested rolling window), leaving
    # other completed timeframes untouched.  Upserts are keyed by candle time,
    # so replaying the page is idempotent after a restart.
    repair_from = store.earliest_open_quality_issue(
        symbol, timeframe,
        issue_types=("EMPTY_HISTORY_PAGE", "MISSING_CANDLES", "TIMESTAMP_ORDER", "DUPLICATE"),
        start=start, end=end,
    )
    if repair_from is not None:
        # Validation reports a gap on the *later* candle.  Rewind one full
        # interval so the missing predecessor can actually be fetched.
        cursor = min(cursor, max(start, (int(repair_from) // period) * period - period))
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
            candles = sorted((row for row in candles
                if row.get("is_closed") and cursor <= int(row["open_time"])
                and int(row["open_time"]) + period <= end), key=lambda row: row["open_time"])
            if candles:
                if int(candles[0]["open_time"]) > cursor:
                    store.save_quality_issue(symbol,timeframe,"MISSING_CANDLES",open_time=cursor,
                        severity="WARNING",detail={"expected":cursor,"observed":candles[0]["open_time"],"page_boundary":True})
                inserted += store.upsert_candles(candles)
                page_issues = validate_candles(candles,timeframe)
                for issue in page_issues:
                    store.save_quality_issue(symbol,timeframe,issue["type"],open_time=issue.get("open_time"),
                                             severity=issue.get("severity","WARNING"),detail=issue)
                # A successful retry proves only the exact page was returned;
                # resolve an earlier page marker at this boundary, never a
                # different defect or an arbitrary issue for the stream.
                if int(candles[0]["open_time"]) == cursor and not any(
                    issue.get("type") in {"MISSING_CANDLES", "TIMESTAMP_ORDER", "DUPLICATE"}
                    for issue in page_issues
                ):
                    for issue_type in ("EMPTY_HISTORY_PAGE", "MISSING_CANDLES"):
                        store.resolve_quality_issue(symbol, timeframe, issue_type, open_time=cursor)
                last=max(x["open_time"] for x in candles)
            else:
                # Empty transport results do not establish historical absence.
                # Keep the cursor before this page so a retry cannot skip it.
                store.save_quality_issue(symbol,timeframe,"EMPTY_HISTORY_PAGE",open_time=cursor,
                    severity="WARNING",detail={"start":cursor,"end":page_end})
                store.checkpoint(job_id,job_type="BACKFILL",symbol=symbol,timeframe=timeframe,
                    range_start=start,range_end=end,last_timestamp=cursor-period,
                    completed_units=completed,total_units=total,status="PAUSED")
                return {"job_id":job_id,"status":"PAUSED","pages":pages,"upserts":inserted}
            cursor=max(cursor+period,last+period); completed=min(total,max(0,(cursor-start)//period)); pages+=1
            store.checkpoint(job_id,job_type="BACKFILL",symbol=symbol,timeframe=timeframe,
                             range_start=start,range_end=end,last_timestamp=last,
                             completed_units=completed,total_units=total,status="RUNNING")
            if on_progress:
                on_progress(completed,total)
        # Validate the complete requested stream before declaring the job
        # complete.  This catches a gap spanning two transport pages and lets
        # a subsequent rolling run rewind to the exact unresolved candle.
        stream = store.candles_between(symbol, timeframe, max(0, start-period), end)
        stream = [row for row in stream if start <= int(row["open_time"]) < end]
        complete_issues = validate_candles(stream, timeframe)
        for issue in complete_issues:
            store.save_quality_issue(symbol, timeframe, issue["type"], open_time=issue.get("open_time"),
                                     severity=issue.get("severity", "WARNING"), detail=issue)
        issue_keys = {(str(issue.get("type")), issue.get("open_time")) for issue in complete_issues}
        repair_types = {"EMPTY_HISTORY_PAGE", "MISSING_CANDLES", "TIMESTAMP_ORDER", "DUPLICATE"}
        for issue in store.quality_issues(symbol, timeframe, issue_types=tuple(repair_types), start=start, end=end):
            key = (str(issue.get("issue_type")), issue.get("open_time"))
            if issue.get("open_time") is not None and key not in issue_keys:
                store.resolve_quality_issue(symbol, timeframe, str(issue["issue_type"]),
                                            open_time=int(issue["open_time"]))
        if complete_issues:
            # Do not publish a range which still has a known missing/ordering
            # defect.  The checkpoint remains resumable at the first issue.
            blocking = [x for x in complete_issues if x.get("type") in repair_types]
            if blocking:
                first_issue = min(int(x.get("open_time") or start) for x in blocking)
                store.checkpoint(job_id, job_type="BACKFILL", symbol=symbol, timeframe=timeframe,
                                 range_start=start, range_end=end,
                                 last_timestamp=max(start, first_issue-period), completed_units=max(0, (first_issue-start)//period),
                                 total_units=total, status="PAUSED",
                                 error="historical quality gap requires retry")
                return {"job_id":job_id,"status":"PAUSED","pages":pages,"upserts":inserted,
                        "quality_issues":len(complete_issues)}
        count=store.candle_count(symbol,timeframe)
        store.update_coverage("OHLCV",source="GATE",symbol=symbol,timeframe=timeframe,start=start,end=end,
                              quality="VALID",availability="HISTORICAL",samples=count,
                              metadata={"idempotent":True,"closed_only":True})
        store.checkpoint(job_id,job_type="BACKFILL",symbol=symbol,timeframe=timeframe,
                         range_start=start,range_end=end,last_timestamp=cursor-period,
                         completed_units=total,total_units=total,status="COMPLETED")
        return {"job_id":job_id,"status":"COMPLETED","pages":pages,"upserts":inserted,"candles":count}
    except Exception as exc:
        store.checkpoint(job_id,job_type="BACKFILL",symbol=symbol,timeframe=timeframe,
                         range_start=start,range_end=end,last_timestamp=max(start,cursor-period),
                         completed_units=completed,total_units=total,status="FAILED",error=str(exc))
        raise


__all__ = ["GateHistoryClient", "ResearchBudget", "backfill_pair", "configured_universe", "target_ranges"]
