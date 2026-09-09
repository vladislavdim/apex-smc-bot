"""Bounded orchestration for backfill and feature materialisation."""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from .features import FEATURE_VERSION, TIMEFRAME_SECONDS, compute_feature_snapshot, levels_from_snapshot
from .gate_history import GateHistoryClient, backfill_pair, configured_universe, target_ranges
from .store import ResearchStore, stable_id, utc_now
from .replay import ReplayEngine
from .analytics import evaluate_profile


DATASET_VERSION = "gate-history-v1"
WORKING_TF = {"FAST":"15m","MTF":"15m","SWING":"1h","ZONE":"1h","WYCKOFF":"4h"}


class ResearchWorker:
    def __init__(self, store: ResearchStore | None = None, client: GateHistoryClient | None = None):
        self.store = store or ResearchStore(); self.client = client or GateHistoryClient()
        self.stop_requested = False
        self.profiles: dict[str,str] = {}

    def startup(self) -> None:
        self.store.ensure_schema()
        with self.store.transaction() as conn:
            conn.cursor().execute(self.store._sql("""INSERT INTO research_meta(key,value_json,updated_at)
                VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at"""),
                ("worker_state", '"STARTING"', utc_now()))
        self._seed_profiles()
        self._seed_feature_contracts()

    def _seed_feature_contracts(self) -> None:
        computed=("STRUCTURE","OB_FVG_BREAKER","VOLUME","VOLATILITY","VWAP",
                  "VOLUME_PROFILE","RSI","MACD","FIBONACCI","CVD_PROXY","REGIME","SESSION")
        unavailable=("CVD_REAL","OPEN_INTEREST","FUNDING","LONG_SHORT_RATIO","LIQUIDATIONS",
                     "LIQUIDITY_HEATMAP","MARKET_CAP","MARKET_BREADTH","BTC_DOMINANCE")
        for feature in computed:
            self.store.update_coverage(feature,source="GATE",quality="PENDING_BACKFILL",
                availability="HISTORICAL",metadata={"execution_authority":False,"shadow_only":True})
        for feature in unavailable:
            self.store.update_coverage(feature,source="UNCONFIGURED_EXTERNAL",quality="UNAVAILABLE",
                availability="REQUIRES_POINT_IN_TIME_SOURCE",metadata={"execution_authority":False,
                "shadow_only":True,"missing_is_not_zero":True})

    def _seed_profiles(self) -> None:
        sha=os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("GIT_COMMIT") or "unknown"
        locked=["NO_LOOK_AHEAD","CLOSED_CANDLES","VALID_DIRECTION","POSITIVE_LEVELS","SL_TP_ORDERING",
                "EXECUTION_SAFETY","RISK_LIMITS","MAX_EXPOSURE","BINANCE_VALIDATION","DATA_FRESHNESS","RR_GTE_2"]
        for strategy in WORKING_TF:
            self.store.upsert_profile(strategy,"production-current","PRODUCTION_REFERENCE",{},locked,sha)
            self.profiles[strategy]=self.store.upsert_profile(strategy,"research-v1","LIVE_SHADOW",{"working_timeframe":WORKING_TF[strategy]},locked,sha)

    def _set_state(self, state: str, **detail: Any) -> None:
        import json
        with self.store.transaction() as conn:
            conn.cursor().execute(self.store._sql("""INSERT INTO research_meta(key,value_json,updated_at)
                VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at"""),
                ("worker_state", json.dumps({"state":state,**detail},ensure_ascii=False), utc_now()))

    def backfill(self) -> None:
        universe=configured_universe(); ranges=target_ranges()
        fast=set(x.strip().upper() for x in os.environ.get("APEX_RESEARCH_FAST_PAIRS","BTCUSDT,ETHUSDT,SOLUSDT,AAVEUSDT,BNBUSDT").split(",") if x.strip())
        self._set_state("BACKFILL",pairs=len(universe),started_at=utc_now())
        metadata={}
        try:
            metadata=self.client.contract_metadata()
        except Exception as exc:
            logging.warning("[Research] Gate contract metadata unavailable: %s",exc)
        total_jobs=len(universe)*4+len([x for x in universe if x in fast]); done=0
        for symbol in universe:
            if self.stop_requested: return
            contract=symbol.replace("USDT","_USDT"); row=metadata.get(contract,{})
            listed=int(row.get("create_time") or 0) or None
            self.store.upsert_symbol(symbol,contract,listed_at=listed,delisted_at=int(time.time()) if row.get("in_delisting") else None,metadata=row)
            for timeframe in ("15m","1h","4h","1d") + (("5m",) if symbol in fast else ()):
                start,end=ranges[timeframe]
                if listed: start=max(start,listed)
                try:
                    result=backfill_pair(self.store,self.client,symbol,timeframe,start,end)
                    logging.info("[Research] backfill %s %s: %s",symbol,timeframe,result)
                except Exception as exc:
                    logging.warning("[Research] backfill failed %s %s: %s",symbol,timeframe,exc)
                done+=1; self._set_state("BACKFILL",pairs=len(universe),jobs_done=done,jobs_total=total_jobs,
                                         progress=round(done/total_jobs*100,2),gate_requests_today=self.client.budget.used_today)

    def materialize_features(self) -> None:
        universe=configured_universe(); ranges=target_ranges(); self._set_state("FEATURES",started_at=utc_now())
        jobs=[]
        for symbol in universe:
            for timeframe in ("15m","1h","4h","1d"):
                jobs.append((symbol,timeframe))
        for index,(symbol,timeframe) in enumerate(jobs,1):
            if self.stop_requested: return
            self._features_for(symbol,timeframe,ranges[timeframe])
            self._set_state("FEATURES",jobs_done=index,jobs_total=len(jobs),progress=round(index/len(jobs)*100,2))

    def replay(self) -> None:
        universe=configured_universe(); ranges=target_ranges(); start=min(x[0] for x in ranges.values()); end=max(x[1] for x in ranges.values())
        sha=os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("GIT_COMMIT") or "unknown"
        run_id=stable_id("continuous-replay",DATASET_VERSION,"research-v1",tuple(universe))
        run={"research_run_id":run_id,"run_type":"POINT_IN_TIME_CAUSAL_SHADOW","dataset_version":DATASET_VERSION,
             "strategy_version":"research-v1","feature_version":FEATURE_VERSION,"code_sha":sha,
             "range_start":start,"range_end":end,"universe":universe,"config":{"rr_floor":2.0,"closed_only":True,"auto_promote":False},
             "status":"RUNNING","progress":0,"started_at":utc_now()}
        run_id=self.store.save_run(run); engine=ReplayEngine(self.store); jobs=[(s,p) for p in universe for s in WORKING_TF]
        self._set_state("REPLAY",run_id=run_id,jobs_total=len(jobs))
        try:
            for index,(strategy,symbol) in enumerate(jobs,1):
                if self.stop_requested: return
                bounds=ranges[WORKING_TF[strategy]]
                job_id=stable_id("replay",self.profiles[strategy],symbol)
                existing=self.store.job(job_id)
                replay_start=max(bounds[0],int(existing.get("last_timestamp") or 0)+1)
                result=engine.replay_profile(run_id,self.profiles[strategy],strategy,symbol,replay_start,bounds[1])
                last_timestamp=result.get("last_timestamp") or int(existing.get("last_timestamp") or replay_start-1)
                self.store.checkpoint(job_id,job_type="REPLAY",strategy_version="research-v1",
                    symbol=symbol,timeframe=WORKING_TF[strategy],range_start=bounds[0],range_end=bounds[1],
                    last_timestamp=last_timestamp,completed_units=1,total_units=1,status="COMPLETED")
                run.update({"research_run_id":run_id,"progress":index/len(jobs)*100,"status":"RUNNING"}); self.store.save_run(run)
                self._set_state("REPLAY",run_id=run_id,jobs_done=index,jobs_total=len(jobs),progress=round(index/len(jobs)*100,2))
            engine.refresh_open_tracks(run_id,end)
            run.update({"research_run_id":run_id,"progress":100,"status":"COMPLETED","finished_at":utc_now()}); self.store.save_run(run)
            for profile_id in self.profiles.values():
                evaluate_profile(self.store,run_id,profile_id)
        except Exception as exc:
            run.update({"research_run_id":run_id,"status":"FAILED","error":str(exc)[:1000],"finished_at":utc_now()}); self.store.save_run(run); raise

    def _features_for(self,symbol: str,timeframe: str,bounds: tuple[int,int]) -> None:
        start,end=bounds; job_id=stable_id("features",symbol,timeframe,FEATURE_VERSION)
        existing=self.store.job(job_id); cursor=max(start,int(existing.get("last_timestamp") or 0)+1)
        total=max(1,(end-start)//TIMEFRAME_SECONDS[timeframe])
        completed=max(0,int(existing.get("completed_units") or 0))
        self.store.checkpoint(job_id,job_type="FEATURES",symbol=symbol,timeframe=timeframe,
                              range_start=start,range_end=end,last_timestamp=cursor-1,
                              completed_units=completed,total_units=total,status="RUNNING")
        # Work in bounded chunks. Each snapshot uses only candles available AS OF.
        with self.store.transaction() as conn:
            cur=conn.cursor(); cur.execute(self.store._sql("""SELECT close_time FROM market_candles
                WHERE source='GATE' AND symbol=? AND timeframe=? AND is_closed=1 AND close_time>=? AND close_time<=?
                ORDER BY close_time"""),(symbol,timeframe,cursor,end)); timestamps=[int(x[0]) for x in cur.fetchall()]
        for pos,as_of in enumerate(timestamps,1):
            candles=self.store.candles(symbol,timeframe,as_of=as_of,limit=240)
            if len(candles)<50: continue
            try:
                snapshot=compute_feature_snapshot(symbol,timeframe,candles,dataset_version=DATASET_VERSION)
                self.store.save_feature_snapshot(symbol,timeframe,as_of,snapshot,feature_version=FEATURE_VERSION,
                                                 dataset_version=DATASET_VERSION,quality=snapshot["data_quality"]["status"])
                for level in levels_from_snapshot(snapshot): self.store.upsert_level(level)
            except Exception as exc:
                self.store.save_quality_issue(symbol,timeframe,"FEATURE_CALCULATION",open_time=as_of,
                                              severity="ERROR",detail={"error":str(exc)[:500]})
            if pos%100==0:
                self.store.checkpoint(job_id,job_type="FEATURES",symbol=symbol,timeframe=timeframe,
                                      range_start=start,range_end=end,last_timestamp=as_of,
                                      completed_units=completed+pos,total_units=total,status="RUNNING")
        last=timestamps[-1] if timestamps else cursor-1
        self.store.update_coverage("FEATURE_SNAPSHOT",source="GATE",symbol=symbol,timeframe=timeframe,start=start,end=last,
                                  quality="VALID",availability="HISTORICAL",samples=len(timestamps),
                                  metadata={"feature_version":FEATURE_VERSION,"point_in_time":True})
        self.store.checkpoint(job_id,job_type="FEATURES",symbol=symbol,timeframe=timeframe,
                              range_start=start,range_end=end,last_timestamp=last,
                              completed_units=total,total_units=total,status="COMPLETED")

    def cycle(self) -> None:
        self.startup(); self.backfill(); self.materialize_features(); self.replay()
        manifest=self.store.dataset_manifest(); self.store.set_meta("dataset_manifest",manifest)
        self._set_state("READY",completed_at=utc_now(),manifest_hash=manifest["manifest_hash"])


__all__=["DATASET_VERSION","ResearchWorker","WORKING_TF"]
