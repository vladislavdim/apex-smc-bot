"""Point-in-time strategy profiles, shadow outcomes and research statistics."""
from __future__ import annotations

import json
import math
import os
import statistics
from bisect import bisect_right
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from core.execution_simulator import ExecutionModel, simulate_fill
from core.setup_evidence import assess_candidate
from .features import FEATURE_VERSION
from .store import ResearchStore, stable_id


STRATEGIES=("FAST","MTF","SWING","ZONE","WYCKOFF")
WORKING_TF={"FAST":"15m","MTF":"15m","SWING":"1h","ZONE":"1h","WYCKOFF":"4h"}


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        result=float(value)
        return result if math.isfinite(result) else default
    except (TypeError,ValueError): return default


def _direction(snapshot: Mapping[str,Any]) -> str:
    return str((snapshot.get("structure") or {}).get("direction") or "").upper()


def _event(snapshot: Mapping[str,Any]) -> bool:
    structure=snapshot.get("structure") or {}
    return str(structure.get("event") or "").upper() in {"BOS","CHOCH"} and str(structure.get("event_direction") or "").upper()==_direction(snapshot)


def _location(snapshot: Mapping[str,Any]) -> bool:
    location=snapshot.get("location") or {}
    return bool(location.get("ob") or location.get("fvg"))


def _geometry(snapshot: Mapping[str,Any], direction: str) -> dict[str,float] | None:
    price=_num(snapshot.get("price")); atr=_num((snapshot.get("volatility") or {}).get("atr"))
    swings=(snapshot.get("structure") or {}).get("swings") or []
    highs=[_num(x.get("price")) for x in swings if str(x.get("side"))=="HIGH"]
    lows=[_num(x.get("price")) for x in swings if str(x.get("side"))=="LOW"]
    highs=[x for x in highs if x is not None]; lows=[x for x in lows if x is not None]
    if not price or not atr or direction not in {"BULLISH","BEARISH"}: return None
    if direction=="BULLISH":
        structural_lows=[x for x in lows if x<price]; structural_highs=[x for x in highs if x>price]
        sl=(max(structural_lows) if structural_lows else price-atr)-atr*.15
        candidates=sorted(structural_highs)
        tp=next((x for x in candidates if (x-price)/(price-sl)>=2),price+2*(price-sl))
    else:
        structural_highs=[x for x in highs if x>price]; structural_lows=[x for x in lows if x<price]
        sl=(min(structural_highs) if structural_highs else price+atr)+atr*.15
        candidates=sorted(structural_lows,reverse=True)
        tp=next((x for x in candidates if (price-x)/(sl-price)>=2),price-2*(sl-price))
    risk=abs(price-sl)
    if risk<=0 or min(price,sl,tp)<=0: return None
    rr=abs(tp-price)/risk
    if rr<2: return None
    return {"entry":price,"sl":sl,"tp1":price+(tp-price)*.6,"tp2":tp,"terminal_tp":tp,"rr":rr}


def _candidate(strategy: str, snapshots: Mapping[str,Mapping[str,Any]], symbol: str) -> tuple[dict[str,Any],str]:
    working=snapshots.get(WORKING_TF[strategy]) or {}; direction=_direction(working)
    geometry=_geometry(working,direction)
    technical: dict[str,Any]={}; stop=""
    if not geometry:
        return {"scan_type":strategy,"symbol":symbol,"direction":direction,"technical_evidence":technical},"NO_STRUCTURAL_GEOMETRY"
    if strategy=="FAST":
        session=str((working.get("session") or {}).get("name") or "")
        technical={"zone":session in {"LONDON","OVERLAP","NEW_YORK"},"ob":(working.get("location") or {}).get("ob"),
                   "fvg":(working.get("location") or {}).get("fvg"),"structure_event":_event(working),
                   "volume_confirmed":(_num((working.get("participation") or {}).get("relative_volume"),0) or 0)>=2.0}
        if not technical["zone"]: stop="FAST_SESSION"
        elif not _location(working): stop="FAST_LOCATION"
        elif not _event(working): stop="FAST_STRUCTURE"
        elif not technical["volume_confirmed"]: stop="FAST_VOLUME"
    elif strategy=="MTF":
        one=snapshots.get("1h") or {}; four=snapshots.get("4h") or {}
        technical={"timeframe_alignment":{"1h":_direction(one),"4h":_direction(four)},
                   "ob":(four.get("location") or {}).get("ob") or (one.get("location") or {}).get("ob"),
                   "fvg":(four.get("location") or {}).get("fvg") or (one.get("location") or {}).get("fvg"),
                   "structure_event":_event(working),"volume_confirmed":True}
        if _direction(one)!=direction or _direction(four)!=direction: stop="MTF_ALIGNMENT"
        elif not (technical["ob"] or technical["fvg"]): stop="MTF_LOCATION"
        elif not _event(working): stop="MTF_15M_STRUCTURE"
    elif strategy=="SWING":
        four=snapshots.get("4h") or {}; technical={"htf_dir":_direction(four),
          "ob":(four.get("location") or {}).get("ob"),"fvg":(four.get("location") or {}).get("fvg"),
          "confirms":_location(four),"structure_event":_event(four),"structure_event_1h":_event(working),
          "volume_confirmed":True}
        if not _location(four): stop="SWING_LOCATION"
        elif not _event(working): stop="SWING_1H_STRUCTURE"
    elif strategy=="ZONE":
        recent=snapshots.get("4h") or working; swings=(recent.get("structure") or {}).get("swings") or []
        prices=[_num(x.get("price")) for x in swings if _num(x.get("price"))]
        price=_num(working.get("price"),0) or 0
        position=(price-min(prices))/(max(prices)-min(prices)) if len(prices)>=2 and max(prices)>min(prices) else .5
        zone="discount" if position<=.25 else "premium" if position>=.75 else "middle"
        expected="discount" if direction=="BULLISH" else "premium"
        technical={"zone":zone,"zone_type":"RANGE_EXTREME" if zone!="middle" else "",
                   "structure_event":_event(working),"volume_confirmed":True}
        if zone!=expected: stop="ZONE_EXTREME"
        elif not _event(working): stop="ZONE_STRUCTURE"
    else:
        four=snapshots.get("4h") or working; regime=four.get("regime") or {}; loc=four.get("location") or {}
        event=str((four.get("structure") or {}).get("event") or "").upper()
        bullish=direction=="BULLISH"; range_ready=regime.get("primary")=="RANGE" or regime.get("phase")=="COMPRESSION"
        technical={"phases":"RE-ACCUMULATION" if bullish else "DISTRIBUTION",
                   "spring":bullish and range_ready,"sos":bullish and event in {"BOS","CHOCH"},
                   "utad":not bullish and range_ready,"sow":not bullish and event in {"BOS","CHOCH"},
                   "reacc_trigger_validated":bool(event),"ob":loc.get("ob"),"fvg":loc.get("fvg")}
        if not range_ready: stop="WYCKOFF_RANGE"
        elif not event: stop="WYCKOFF_PHASE_TRIGGER"
    candidate={"scan_type":strategy,"grade":strategy,"symbol":symbol,"timeframe":WORKING_TF[strategy],
               "direction":direction,**geometry,"technical_evidence":technical,"feature_snapshot":working}
    assessment=assess_candidate(candidate)
    candidate["setup_assessment"]=assessment
    if assessment.get("blocking") and not stop: stop="CAUSAL_MATRIX"
    return candidate,stop


@dataclass(frozen=True)
class ReplayConfig:
    entry_expiry_bars: int=12
    fee_bps: float=4.0
    slippage_bps: float=1.0
    ambiguity_policy: str="SL_FIRST"


class ReplayEngine:
    def __init__(self,store: ResearchStore,config: ReplayConfig|None=None):
        self.store=store; self.config=config or ReplayConfig()

    def snapshots(self,symbol: str,as_of: int) -> dict[str,dict[str,Any]]:
        return {tf:self.store.feature_snapshot(symbol,tf,as_of=as_of,feature_version=FEATURE_VERSION)
                for tf in ("15m","1h","4h","1d")}

    def evaluate(self,strategy: str,symbol: str,as_of: int) -> tuple[dict[str,Any],str]:
        if strategy not in STRATEGIES: raise ValueError("unsupported strategy")
        return _candidate(strategy,self.snapshots(symbol,as_of),symbol)

    def replay_profile(self,research_run_id: str,profile_id: str,strategy: str,symbol: str,
                       start: int,end: int,*,on_progress=None) -> dict[str,Any]:
        timestamps=self.store.feature_timestamps(symbol,WORKING_TF[strategy],start,end,FEATURE_VERSION)
        future=self.store.candles_between(symbol,WORKING_TF[strategy],start,end)
        future_close_times=[int(row["close_time"]) for row in future]
        attempts=trades=0; batch_size=max(25,min(int(os.environ.get("APEX_RESEARCH_REPLAY_BATCH","100")),500))
        timeframes=("15m","1h","4h","1d")
        for offset in range(0,len(timestamps),batch_size):
            batch=timestamps[offset:offset+batch_size]
            series={tf:self.store.feature_series(symbol,tf,batch[0],batch[-1],FEATURE_VERSION)
                    for tf in timeframes}
            series_times={tf:[item[0] for item in rows] for tf,rows in series.items()}
            attempt_rows=[]; trade_rows=[]
            for as_of in batch:
                snapshots={}
                for tf in timeframes:
                    position=bisect_right(series_times[tf],as_of)-1
                    snapshots[tf]=series[tf][position][1] if position>=0 else {}
                candidate,stop=_candidate(strategy,snapshots,symbol)
                geometry=all(_num(candidate.get(k)) for k in ("entry","sl","tp1"))
                outcome="CANDIDATE" if not stop else "FILTERED"
                attempt_id=stable_id(research_run_id,profile_id,symbol,as_of)
                attempt_rows.append({"attempt_id":attempt_id,"research_run_id":research_run_id,
                    "profile_id":profile_id,"parent_strategy":strategy,"symbol":symbol,
                    "direction":candidate.get("direction"),"decision_time":as_of,
                    "stage":"GEOMETRY" if geometry else "STRUCTURE","outcome":outcome,"stop_code":stop,
                    "entry":candidate.get("entry"),"sl":candidate.get("sl"),"tp1":candidate.get("tp1"),
                    "tp2":candidate.get("tp2"),"terminal_tp":candidate.get("terminal_tp"),"rr":candidate.get("rr"),
                    "snapshot":{"candidate":{k:v for k,v in candidate.items() if k!="feature_snapshot"},
                                "feature_ref":{"symbol":symbol,"as_of":as_of,
                                    "feature_version":FEATURE_VERSION},
                                "point_in_time":True,"filtered_shadow":bool(stop)}})
                if geometry:
                    track="FILTERED_SHADOW" if stop else "PROFILE_SHADOW"
                    trade_rows.append(self.simulate_trade(attempt_id,track,candidate,as_of,end,
                        future=future,future_close_times=future_close_times))
            self.store.save_attempts(attempt_rows); self.store.save_trades(trade_rows)
            attempts+=len(attempt_rows); trades+=len(trade_rows)
            if on_progress: on_progress(offset+len(batch),len(timestamps))
        return {"attempts":attempts,"trades":trades,"timestamps":len(timestamps),
                "last_timestamp":timestamps[-1] if timestamps else None}

    def refresh_open_tracks(self, research_run_id: str, end: int) -> int:
        updated=0
        for row in self.store.open_trade_rows(research_run_id):
            try:
                snapshot=json.loads(row.get("snapshot_json") or "{}")
            except (TypeError,ValueError,json.JSONDecodeError):
                continue
            candidate=(snapshot.get("candidate") or {})
            if not candidate:
                continue
            trade=self.simulate_trade(str(row["attempt_id"]),str(row["track"]),candidate,
                                      int(row["decision_time"]),end)
            self.store.save_trade(trade); updated+=1
        return updated

    def simulate_trade(self,attempt_id: str,track: str,candidate: Mapping[str,Any],decision_time: int,end: int,
                       *,future: Sequence[Mapping[str,Any]]|None=None,
                       future_close_times: Sequence[int]|None=None) -> dict[str,Any]:
        side=str(candidate.get("direction") or ""); entry=float(candidate["entry"]); sl=float(candidate["sl"])
        tp1=float(candidate["tp1"]); tp2=float(candidate.get("tp2") or candidate.get("terminal_tp") or tp1)
        terminal=float(candidate.get("terminal_tp") or tp2); risk=abs(entry-sl); timeframe=str(candidate.get("timeframe") or "1h")
        first=0
        if future is None:
            future=self.store.candles_between(str(candidate["symbol"]),timeframe,decision_time,end)
        elif future_close_times is not None:
            first=bisect_right(future_close_times,int(decision_time))
        entered=False; entry_time=None; mfe=0.0; mae=0.0; targets=[]; ambiguity=None; exit_price=None; exit_time=None; reason=None
        for absolute_idx in range(first,len(future)):
            idx=absolute_idx-first; candle=future[absolute_idx]
            high,low=float(candle["high"]),float(candle["low"]); ts=int(candle["close_time"])
            if not entered:
                if low<=entry<=high:
                    entered=True; entry_time=ts
                elif idx>=self.config.entry_expiry_bars:
                    return self._trade(attempt_id,track,side,entry,sl,tp1,tp2,terminal,"EXPIRED","EXPIRED",None,None,None,0,0,[],None)
                else: continue
            favorable=(high-entry)/risk if side=="BULLISH" else (entry-low)/risk
            adverse=(entry-low)/risk if side=="BULLISH" else (high-entry)/risk
            mfe=max(mfe,favorable); mae=max(mae,adverse)
            sl_hit=low<=sl if side=="BULLISH" else high>=sl
            terminal_hit=high>=terminal if side=="BULLISH" else low<=terminal
            tp1_hit=high>=tp1 if side=="BULLISH" else low<=tp1
            if tp1_hit and "TP1" not in targets: targets.append("TP1")
            if sl_hit and terminal_hit:
                ambiguity="TP_AND_SL_SAME_CANDLE_SL_FIRST"; exit_price=sl; exit_time=ts; reason="SL"; break
            if sl_hit: exit_price=sl; exit_time=ts; reason="SL"; break
            if terminal_hit: targets.append("TERMINAL_TP"); exit_price=terminal; exit_time=ts; reason="TP"; break
        if not entered:
            return self._trade(attempt_id,track,side,entry,sl,tp1,tp2,terminal,"EXPIRED","EXPIRED",None,None,None,0,0,[],None)
        if exit_price is None:
            return self._trade(attempt_id,track,side,entry,sl,tp1,tp2,terminal,"OPEN","FILLED",entry_time,None,None,mfe,mae,targets,ambiguity)
        gross=((exit_price-entry)/risk if side=="BULLISH" else (entry-exit_price)/risk)
        fill=simulate_fill("BUY" if side=="BULLISH" else "SELL",1,entry,
                           model=ExecutionModel(self.config.fee_bps,self.config.slippage_bps,250,5))
        # Entry and exit both incur fees/slippage. Funding/basis are deliberately
        # unavailable until point-in-time source data exists; never treat missing as zero.
        entry_fee=float(fill.get("fee_quote",0) or 0)
        exit_fee=abs(float(exit_price))*self.config.fee_bps/10000.0
        entry_slippage=abs(float(fill.get("fill_price",entry))-entry)
        exit_slippage=abs(float(exit_price))*self.config.slippage_bps/10000.0
        fee_r=(entry_fee+exit_fee)/risk if risk else 0
        slippage_r=(entry_slippage+exit_slippage)/risk if risk else 0
        cost_r=fee_r+slippage_r
        net=gross-cost_r
        trade=self._trade(attempt_id,track,side,entry,sl,tp1,tp2,terminal,"CLOSED","FILLED",entry_time,exit_price,exit_time,mfe,mae,targets,ambiguity)
        trade.update({"exit_reason":reason,"gross_r":gross,"net_r":net,"fees_r":fee_r,
                      "slippage_r":slippage_r,
                      "pnl_pct":((exit_price-entry)/entry*100 if side=="BULLISH" else (entry-exit_price)/entry*100)-cost_r*risk/entry*100,
                      "giveback_r":max(0,mfe-gross),
                      "state":{"independent_track":True,"manager_actions_inherited":False,
                               "funding_r":None,"basis_r":None,"cost_completeness":"FEES_SLIPPAGE_ESTIMATED"}})
        return trade

    @staticmethod
    def _trade(attempt_id,track,side,entry,sl,tp1,tp2,terminal,status,entry_state,entry_time,exit_price,exit_time,mfe,mae,targets,ambiguity):
        return {"attempt_id":attempt_id,"track":track,"status":status,"entry_state":entry_state,"side":side,
                "entry":entry,"entry_time":entry_time,"initial_sl":sl,"current_sl":sl,"tp1":tp1,"tp2":tp2,
                "terminal_tp":terminal,"exit_price":exit_price,"exit_time":exit_time,"quantity":1.0,
                "mfe_r":mfe,"mae_r":mae,"targets_reached":targets,"ambiguity":ambiguity,
                "state":{"independent_track":True,"manager_actions_inherited":False}}


def metrics(values: Sequence[float]) -> dict[str,Any]:
    clean=[float(x) for x in values if _num(x) is not None]
    wins=[x for x in clean if x>0]; losses=[x for x in clean if x<=0]
    equity=peak=drawdown=0.0
    for value in clean:
        equity+=value; peak=max(peak,equity); drawdown=max(drawdown,peak-equity)
    gross_profit=sum(wins); gross_loss=abs(sum(losses))
    return {"n":len(clean),"win_rate":len(wins)/len(clean)*100 if clean else None,
            "expectancy":statistics.fmean(clean) if clean else None,
            "median":statistics.median(clean) if clean else None,
            "profit_factor":gross_profit/gross_loss if gross_loss else None,"max_drawdown_r":drawdown}


def chronological_splits(timestamps: Sequence[int]) -> dict[str,list[int]]:
    ordered=sorted(set(int(x) for x in timestamps)); a=int(len(ordered)*.6); b=int(len(ordered)*.8)
    return {"TRAIN":ordered[:a],"VALIDATION":ordered[a:b],"TEST":ordered[b:]}


def walk_forward_windows(start: int,end: int,train_days: int=180,test_days: int=30) -> list[dict[str,int]]:
    day=86400; cursor=start; windows=[]
    while cursor+train_days*day+test_days*day<=end:
        train_end=cursor+train_days*day; test_end=train_end+test_days*day
        windows.append({"train_start":cursor,"train_end":train_end,"test_start":train_end,"test_end":test_end})
        cursor+=test_days*day
    return windows


__all__=["ReplayConfig","ReplayEngine","chronological_splits","metrics","walk_forward_windows"]
