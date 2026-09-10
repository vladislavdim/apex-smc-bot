"""Gate derivatives and microstructure context for Research/Shadow only.

The module stores only observations that Gate can identify point-in-time.  It
never fabricates missing history and never exposes an execution callback.
Historical contract statistics provide OI, long/short ratios and liquidation
aggregates. Funding is fetched from its dedicated history endpoint. Public
trades and order-book depth are explicitly recent/forward-only because Gate
does not promise a one-year replayable tape or historical depth snapshots.
"""
from __future__ import annotations

import os
import time
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from math import isfinite
from typing import Any, Iterable, Mapping

from external_sources.gate_microstructure import summarize_order_book
from external_sources.pair_registry import get_pair
from .store import ResearchStore, utc_now


GATE_FUTURES_BASE = "https://api.gateio.ws/api/v4/futures/usdt"
FEATURES = (
    "TRADE_CVD_REAL", "OPEN_INTEREST", "FUNDING_RATE", "LIQUIDATIONS",
    "ORDER_BOOK_LIQUIDITY", "LONG_SHORT_RATIO",
)
MAX_AGE_SECONDS = {
    "TRADE_CVD_REAL": 30 * 60,
    "OPEN_INTEREST": 2 * 60 * 60,
    "FUNDING_RATE": 9 * 60 * 60,
    "LIQUIDATIONS": 2 * 60 * 60,
    "ORDER_BOOK_LIQUIDITY": 15,
    "LONG_SHORT_RATIO": 2 * 60 * 60,
}
GATE_DERIVATIVES_HISTORY_SECONDS = 180 * 86400


def _gate_history_start(start: int, end: int) -> int:
    """Respect Gate's public 180-day retention without inventing history."""
    return max(int(start), int(end) - GATE_DERIVATIVES_HISTORY_SECONDS + 1)


def _number(value: Any) -> float | None:
    try:
        result=float(value)
    except (TypeError,ValueError):
        return None
    return result if isfinite(result) else None


def _contract(symbol: str) -> str:
    pair=get_pair(symbol)
    return str(pair.get("gate_symbol") or symbol.upper().replace("USDT","_USDT"))


def _observation(source: str, symbol: str, feature: str, event_time: int,
                 value: Mapping[str, Any], *, quality: str = "VALID",
                 availability: str = "HISTORICAL") -> dict[str, Any]:
    return {"source":source,"symbol":symbol.upper(),"feature":feature,
            "event_time":int(event_time),"received_at":utc_now(),"quality":quality,
            "availability":availability,"point_in_time":True,"value":dict(value)}


class PointInTimeContextIndex:
    """Efficient AS-OF join between sparse context and candle features."""
    def __init__(self, rows: Iterable[Mapping[str, Any]]):
        grouped: dict[str,list[tuple[int,dict[str,Any]]]]=defaultdict(list)
        for row in rows:
            grouped[str(row.get("feature") or "").upper()].append(
                (int(row.get("event_time") or 0),dict(row)))
        self.rows={key:sorted(values,key=lambda x:x[0]) for key,values in grouped.items()}
        self.times={key:[x[0] for x in values] for key,values in self.rows.items()}

    def as_of(self, timestamp: int) -> dict[str, Any]:
        result={}
        for feature, rows in self.rows.items():
            pos=bisect_right(self.times[feature],int(timestamp))-1
            if pos < 0: continue
            row=rows[pos][1]
            if int(timestamp)-int(row.get("event_time") or 0) > MAX_AGE_SECONDS.get(feature,0):
                continue
            result[feature.lower()]={"source":row.get("source"),
                "event_time":row.get("event_time"),"quality":row.get("quality"),
                "availability":row.get("availability"),**dict(row.get("value") or {})}
        return result


@dataclass
class GateMarketContextClient:
    """Uses the same persisted budget and retry path as Gate candle history."""
    history_client: Any

    def _get(self, path: str, params: Mapping[str, Any]) -> Any:
        return self.history_client._get(f"{GATE_FUTURES_BASE}/{path}",dict(params))

    def funding(self, symbol: str, start: int, end: int) -> list[dict[str, Any]]:
        contract=_contract(symbol); cursor=_gate_history_start(start,end); output=[]
        while cursor <= end:
            data=self._get("funding_rate",{"contract":contract,"from":cursor,"to":int(end),"limit":1000})
            batch=[]
            for row in data if isinstance(data,list) else []:
                ts=int(row.get("t") or 0); rate=_number(row.get("r"))
                if cursor <= ts <= end and rate is not None:
                    batch.append(_observation("GATE_FUNDING",symbol,"FUNDING_RATE",ts,
                        {"rate":rate,"contract":contract,"unit":"fraction"},
                        availability="HISTORICAL_LIMITED_180D"))
            batch.sort(key=lambda x:x["event_time"]); output.extend(batch)
            if not batch or len(batch)<1000: break
            next_cursor=batch[-1]["event_time"]+1
            if next_cursor<=cursor: break
            cursor=next_cursor
        return output

    def contract_stats(self, symbol: str, start: int, end: int,
                       interval: str = "1h") -> list[dict[str, Any]]:
        contract=_contract(symbol); cursor=_gate_history_start(start,end); output=[]; previous_oi=None
        page_limit=max(30,min(int(os.environ.get("APEX_RESEARCH_STATS_PAGE_LIMIT","100")),1000))
        while cursor <= end:
            data=self._get("contract_stats",{"contract":contract,"from":cursor,
                "interval":interval,"limit":page_limit})
            batch=sorted((row for row in (data if isinstance(data,list) else [])
                          if isinstance(row,dict) and cursor<=int(row.get("time") or 0)<=end),
                         key=lambda x:int(x.get("time") or 0))
            for row in batch:
                ts=int(row["time"]); oi=_number(row.get("open_interest")); oi_usd=_number(row.get("open_interest_usd"))
                change=(oi/previous_oi-1)*100 if oi is not None and previous_oi not in (None,0) else None
                output.append(_observation("GATE_CONTRACT_STATS",symbol,"OPEN_INTEREST",ts,
                    {"contracts":oi,"usd":oi_usd,"change_1h_pct":change,"contract":contract},
                    availability="HISTORICAL_LIMITED_180D"))
                output.append(_observation("GATE_CONTRACT_STATS",symbol,"LONG_SHORT_RATIO",ts,
                    {"accounts":_number(row.get("lsr_account")),"takers":_number(row.get("lsr_taker")),
                     "top_accounts":_number(row.get("top_lsr_account")),
                     "top_size":_number(row.get("top_lsr_size")),"contract":contract},
                    availability="HISTORICAL_LIMITED_180D"))
                output.append(_observation("GATE_CONTRACT_STATS",symbol,"LIQUIDATIONS",ts,
                    {"long_contracts":_number(row.get("long_liq_size")),
                     "short_contracts":_number(row.get("short_liq_size")),
                     "long_usd":_number(row.get("long_liq_usd")),
                     "short_usd":_number(row.get("short_liq_usd")),"interval":interval,
                     "contract":contract},availability="HISTORICAL_LIMITED_180D"))
                if oi is not None: previous_oi=oi
            if not batch or len(batch)<page_limit: break
            next_cursor=int(batch[-1].get("time") or 0)+1
            if next_cursor<=cursor: break
            cursor=next_cursor
        return output

    def recent_trade_cvd(self, symbol: str, start: int, end: int) -> list[dict[str, Any]]:
        """Fetch a bounded recent trade tape; exact only when the page is not truncated."""
        contract=_contract(symbol); hours=max(1,min(int(os.environ.get(
            "APEX_RESEARCH_TRADE_CVD_HOURS","6")),24))
        window_start=max(int(start),int(end)-hours*3600)
        page_size=1000; max_pages=max(1,min(int(os.environ.get(
            "APEX_RESEARCH_TRADE_CVD_PAGES","10")),10)); rows=[]; seen=set(); complete=False
        for page in range(max_pages):
            data=self._get("trades",{"contract":contract,"from":window_start,
                "to":int(end),"limit":page_size,"offset":page*page_size})
            batch=[x for x in (data if isinstance(data,list) else []) if isinstance(x,dict)]
            for row in batch:
                identity=row.get("id") or (row.get("create_time"),row.get("size"),row.get("price"))
                if identity not in seen: seen.add(identity); rows.append(row)
            if len(batch)<page_size:
                complete=True; break
        truncated=not complete
        buckets: dict[int,dict[str,float]]=defaultdict(lambda:{"buy":0.0,"sell":0.0,"count":0.0})
        for row in rows:
            if row.get("is_internal"): continue
            ts=int(_number(row.get("create_time")) or 0); size=_number(row.get("size")); price=_number(row.get("price"))
            if not (window_start<=ts<=end) or size is None: continue
            bucket=(ts//900)*900; notional=abs(size)*(price or 0)
            buckets[bucket]["buy" if size>0 else "sell"]+=notional
            buckets[bucket]["count"]+=1
        output=[]
        for ts,value in sorted(buckets.items()):
            buy,sell=value["buy"],value["sell"]; total=buy+sell
            output.append(_observation("GATE_TRADES",symbol,"TRADE_CVD_REAL",ts,
                {"buy_notional":buy,"sell_notional":sell,"delta_notional":buy-sell,
                 "taker_imbalance":(buy-sell)/total if total else None,"trades":int(value["count"]),
                 "window_seconds":900,"contract":contract},
                quality="PARTIAL_TRUNCATED" if truncated else "VALID",availability="RECENT_ONLY"))
        return output

    def order_book(self, symbol: str) -> list[dict[str, Any]]:
        contract=_contract(symbol); depth=max(10,min(int(os.environ.get(
            "APEX_RESEARCH_ORDERBOOK_LEVELS","20")),100))
        data=self._get("order_book",{"contract":contract,"interval":"0","limit":depth,"with_id":"true"})
        if not isinstance(data,dict): return []
        features=summarize_order_book(data.get("bids") or [],data.get("asks") or [],depth,
            "FRESH",data.get("id"))
        # Keep a bounded ladder so successive forward-only observations can be
        # rendered as a real price/time liquidity heatmap. This is visible
        # resting depth, not a claim about stops or market-maker intent.
        ladder=[]
        for side,key in (("BID","bids"),("ASK","asks")):
            for item in (data.get(key) or [])[:depth]:
                if not isinstance(item,(list,tuple)) or len(item)<2: continue
                price,size=_number(item[0]),_number(item[1])
                if price is not None and size is not None and price>0 and size>=0:
                    ladder.append({"side":side,"price":price,"size":size})
        current=_number(data.get("current")) or time.time()
        ts=int(current/1000 if current>1_000_000_000_000 else current)
        features.update({"contract":contract,"historical_replay":False,
                         "observation_kind":"POINT_IN_TIME_SNAPSHOT",
                         "heatmap_levels":ladder})
        return [_observation("GATE_ORDER_BOOK",symbol,"ORDER_BOOK_LIQUIDITY",ts,features,
            availability="FORWARD_ONLY")]


def collect_market_context(store: ResearchStore, history_client: Any, symbol: str,
                           start: int, end: int) -> dict[str, Any]:
    """Collect bounded context without making optional data a run blocker."""
    client=GateMarketContextClient(history_client); observations=[]; errors={}
    collectors={"FUNDING_RATE":lambda:client.funding(symbol,start,end),
                "CONTRACT_STATS":lambda:client.contract_stats(symbol,start,end),
                "TRADE_CVD_REAL":lambda:client.recent_trade_cvd(symbol,start,end),
                "ORDER_BOOK_LIQUIDITY":lambda:client.order_book(symbol)}
    for name,collector in collectors.items():
        try: observations.extend(collector())
        except Exception as exc: errors[name]=f"{type(exc).__name__}: {exc}"[:300]
    inserted=store.upsert_context_observations(observations)
    by_feature: dict[str,list[dict[str,Any]]]=defaultdict(list)
    for row in observations: by_feature[row["feature"]].append(row)
    availability={"TRADE_CVD_REAL":"RECENT_ONLY","OPEN_INTEREST":"HISTORICAL_LIMITED_180D",
        "FUNDING_RATE":"HISTORICAL_LIMITED_180D","LIQUIDATIONS":"HISTORICAL_LIMITED_180D",
        "ORDER_BOOK_LIQUIDITY":"FORWARD_ONLY","LONG_SHORT_RATIO":"HISTORICAL_LIMITED_180D"}
    sources={"TRADE_CVD_REAL":"GATE_TRADES","OPEN_INTEREST":"GATE_CONTRACT_STATS",
        "FUNDING_RATE":"GATE_FUNDING","LIQUIDATIONS":"GATE_CONTRACT_STATS",
        "ORDER_BOOK_LIQUIDITY":"GATE_ORDER_BOOK","LONG_SHORT_RATIO":"GATE_CONTRACT_STATS"}
    collector_error={"TRADE_CVD_REAL":"TRADE_CVD_REAL","OPEN_INTEREST":"CONTRACT_STATS",
        "FUNDING_RATE":"FUNDING_RATE","LIQUIDATIONS":"CONTRACT_STATS",
        "ORDER_BOOK_LIQUIDITY":"ORDER_BOOK_LIQUIDITY","LONG_SHORT_RATIO":"CONTRACT_STATS"}
    for feature in FEATURES:
        rows=by_feature.get(feature,[]); quality=("VALID" if rows and all(x["quality"]=="VALID" for x in rows)
            else "PARTIAL" if rows else "UNAVAILABLE")
        store.update_coverage(feature,source=sources[feature],symbol=symbol,timeframe="*",
            start=min((x["event_time"] for x in rows),default=None),
            end=max((x["event_time"] for x in rows),default=None),quality=quality,
            availability=availability[feature],samples=len(rows),metadata={
                "execution_authority":False,"shadow_only":True,"missing_is_not_zero":True,
                "point_in_time":True,"provider_history_limit_days":180,
                "error":errors.get(collector_error[feature])})
    return {"inserted":inserted,"features":{k:len(v) for k,v in by_feature.items()},"errors":errors}


__all__=["FEATURES","GATE_DERIVATIVES_HISTORY_SECONDS","GateMarketContextClient",
         "PointInTimeContextIndex","collect_market_context"]
