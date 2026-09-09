"""Pair-sequential, bounded orchestration for the isolated Research worker."""
from __future__ import annotations

import gc
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


class ResourcePause(RuntimeError):
    """A recoverable pause before the process reaches its memory ceiling."""


class CooperativeThrottle:
    def __init__(self) -> None:
        self.max_rss_mb=max(0,int(os.environ.get("APEX_RESEARCH_MAX_RSS_MB","384")))
        self.duty=max(.05,min(float(os.environ.get("APEX_RESEARCH_CPU_DUTY_PERCENT","60"))/100.0,1.0))
        self.minimum_yield=max(0.0,float(os.environ.get("APEX_RESEARCH_YIELD_SECONDS","0.05")))

    @staticmethod
    def rss_mb() -> float:
        try:
            with open("/proc/self/status",encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1])/1024.0
        except (OSError,ValueError,IndexError):
            return 0.0
        return 0.0

    def yield_after(self, started: float) -> dict[str,float]:
        elapsed=max(0.0,time.monotonic()-started); rss=self.rss_mb()
        if self.max_rss_mb and rss>=self.max_rss_mb:
            raise ResourcePause(f"RSS {rss:.1f}MB reached research ceiling {self.max_rss_mb}MB")
        sleep_for=max(self.minimum_yield,elapsed*(1.0/self.duty-1.0)) if self.duty<1 else self.minimum_yield
        if sleep_for: time.sleep(min(sleep_for,5.0))
        return {"rss_mb":round(rss,1),"cpu_duty_percent":round(self.duty*100,1)}


class ResearchWorker:
    def __init__(self, store: ResearchStore | None = None, client: GateHistoryClient | None = None):
        self.store = store or ResearchStore(); self.client = client or GateHistoryClient(store=self.store)
        self.stop_requested = False
        self.profiles: dict[str,str] = {}
        self.throttle=CooperativeThrottle()
        self.pair_status: dict[str,str]={}

    def startup(self) -> None:
        self.store.ensure_schema()
        with self.store.transaction() as conn:
            conn.cursor().execute(self.store._sql("""INSERT INTO research_meta(key,value_json,updated_at)
                VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at"""),
                ("worker_state", '"STARTING"', utc_now()))
        self._seed_profiles()
        self._seed_feature_contracts()

    def _seed_feature_contracts(self) -> None:
        budget = getattr(self.client, "budget", None)
        self.store.upsert_source_contract("GATE", kind="CANONICAL_MARKET_DATA", owner="Gate.io",
            authority="CANONICAL", freshness_sla_seconds=120,
            rate_limits={"research_rps": round(1/getattr(budget,"minimum_interval",1.0), 3),
                         "research_minute": getattr(budget,"minute",None),
                         "research_daily": getattr(budget,"daily",None)},
            license_info={"origin":"official public Gate API","historical":True}, status="READY")
        computed=("STRUCTURE","OB_FVG_BREAKER","VOLUME","VOLATILITY","VWAP",
                  "VOLUME_PROFILE","RSI","MACD","FIBONACCI","CVD_PROXY","REGIME","SESSION")
        unavailable=("CVD_REAL","OPEN_INTEREST","FUNDING","LONG_SHORT_RATIO","LIQUIDATIONS",
                     "LIQUIDITY_HEATMAP","MARKET_CAP","MARKET_BREADTH","BTC_DOMINANCE")
        for feature in computed:
            self.store.update_coverage(feature,source="GATE",quality="PENDING_BACKFILL",
                availability="HISTORICAL",metadata={"execution_authority":False,"shadow_only":True})
        self.store.upsert_source_contract("GATE_FEATURES", kind="DERIVED_FEATURES", owner="APEX/Gate",
            authority="CONTEXT_ONLY", coverage={"features":list(computed)},
            license_info={"origin":"derived from closed Gate candles"}, status="READY")
        for feature in unavailable:
            self.store.update_coverage(feature,source="UNCONFIGURED_EXTERNAL",quality="UNAVAILABLE",
                availability="REQUIRES_POINT_IN_TIME_SOURCE",metadata={"execution_authority":False,
                "shadow_only":True,"missing_is_not_zero":True})
        self.store.upsert_source_contract("UNCONFIGURED_EXTERNAL", kind="OPTIONAL_CONTEXT", owner="Not configured",
            authority="CONTEXT_ONLY", coverage={"features":list(unavailable)},
            license_info={"origin":"no external source connected"}, status="UNCONFIGURED")

    def _seed_profiles(self) -> None:
        sha=os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("GIT_COMMIT") or "unknown"
        locked=["NO_LOOK_AHEAD","CLOSED_CANDLES","VALID_DIRECTION","POSITIVE_LEVELS","SL_TP_ORDERING",
                "EXECUTION_SAFETY","RISK_LIMITS","MAX_EXPOSURE","BINANCE_VALIDATION","DATA_FRESHNESS","RR_GTE_2"]
        for strategy in WORKING_TF:
            self.store.upsert_profile(strategy,"production-current","PRODUCTION_REFERENCE",{},locked,sha)
            self.profiles[strategy]=self.store.upsert_profile(strategy,"research-v1","LIVE_SHADOW",{"working_timeframe":WORKING_TF[strategy]},locked,sha)

    def _set_state(self, state: str, **detail: Any) -> None:
        self.store.set_meta("worker_state",{"state":state,**detail})

    def _pair_progress(self, symbol: str, pair_index: int, pair_total: int, percent: float,
                       stage: str, **detail: Any) -> None:
        percent=max(0.0,min(100.0,float(percent)))
        overall=((pair_index-1)+percent/100.0)/max(1,pair_total)*100.0
        payload={"current_symbol":symbol,"pair_index":pair_index,"pair_total":pair_total,
                 "pair_percent":round(percent,2),"overall_percent":round(overall,2),
                 "stage":stage,"strategy_status":dict(self.pair_status),
                 "rss_mb":round(self.throttle.rss_mb(),1),"updated_at":utc_now(),**detail}
        self.store.set_meta("pair_progress",payload)
        self._set_state("PAIR_PIPELINE",**payload)

    @staticmethod
    def _fast_pairs() -> set[str]:
        return {x.strip().upper() for x in os.environ.get(
            "APEX_RESEARCH_FAST_PAIRS","BTCUSDT,ETHUSDT,SOLUSDT,AAVEUSDT,BNBUSDT"
        ).split(",") if x.strip()}

    def _timeframes(self,symbol: str) -> tuple[str,...]:
        return ("15m","1h","4h","1d") + (("5m",) if symbol in self._fast_pairs() else ())

    def _backfill_symbol(self,symbol: str,pair_index: int,pair_total: int,
                         ranges: dict[str,tuple[int,int]],metadata: dict[str,dict[str,Any]]) -> None:
        contract=symbol.replace("USDT","_USDT"); row=metadata.get(contract,{})
        listed=int(row.get("create_time") or 0) or None
        self.store.upsert_symbol(symbol,contract,listed_at=listed,
            delisted_at=int(time.time()) if row.get("in_delisting") else None,metadata=row)
        timeframes=self._timeframes(symbol)
        for tf_index,timeframe in enumerate(timeframes):
            if self.stop_requested: return
            start,end=ranges[timeframe]
            if listed: start=max(start,listed)
            def progress(done,total,idx=tf_index,tf=timeframe):
                fraction=(idx+(done/max(1,total)))/len(timeframes)
                self._pair_progress(symbol,pair_index,pair_total,fraction*30,"GATE_HISTORY",
                    timeframe=tf,gate_requests_today=self.client.budget.used_today)
            result=backfill_pair(self.store,self.client,symbol,timeframe,start,end,
                should_stop=lambda:self.stop_requested,on_progress=progress)
            logging.info("[Research] backfill %s %s: %s",symbol,timeframe,result)
            if result.get("status")=="PAUSED":
                raise ResourcePause(f"History incomplete for {symbol} {timeframe}; retry required")
            self._pair_progress(symbol,pair_index,pair_total,(tf_index+1)/len(timeframes)*30,
                "GATE_HISTORY",timeframe=timeframe,gate_requests_today=self.client.budget.used_today)
        self._pair_progress(symbol,pair_index,pair_total,35,"DATA_QUALITY",quality="CHECKED")

    def _materialize_symbol(self,symbol: str,pair_index: int,pair_total: int,
                            ranges: dict[str,tuple[int,int]]) -> None:
        timeframes=self._timeframes(symbol)
        for index,timeframe in enumerate(timeframes):
            if self.stop_requested: return
            def progress(done,total,idx=index,tf=timeframe):
                fraction=(idx+(done/max(1,total)))/len(timeframes)
                self._pair_progress(symbol,pair_index,pair_total,35+fraction*35,"FEATURES",timeframe=tf)
            self._features_for(symbol,timeframe,ranges[timeframe],on_progress=progress)
            self._pair_progress(symbol,pair_index,pair_total,35+(index+1)/len(timeframes)*35,
                                "FEATURES",timeframe=timeframe)
            gc.collect()

    def _start_run(self,universe: list[str],ranges: dict[str,tuple[int,int]]) -> tuple[str,dict[str,Any]]:
        start=min(x[0] for x in ranges.values()); end=max(x[1] for x in ranges.values())
        sha=os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("GIT_COMMIT") or "unknown"
        run_id=stable_id("continuous-replay",DATASET_VERSION,"research-v1",tuple(universe))
        run={"research_run_id":run_id,"run_type":"POINT_IN_TIME_CAUSAL_SHADOW","dataset_version":DATASET_VERSION,
             "strategy_version":"research-v1","feature_version":FEATURE_VERSION,"code_sha":sha,
             "range_start":start,"range_end":end,"universe":universe,"config":{"rr_floor":2.0,"closed_only":True,"auto_promote":False,
                "detector_mode":"REPLAY_PROFILE_SURROGATE","live_parity":"NOT_ESTABLISHED",
                "tracks":["ACTUAL_LINKED_ONLY","NO_MANAGER","PLAYBOOK_ONLY"]},
             "status":"RUNNING","progress":0,"started_at":utc_now()}
        return self.store.save_run(run),run

    def _replay_symbol(self,run_id: str,run: dict[str,Any],symbol: str,pair_index: int,
                       pair_total: int,ranges: dict[str,tuple[int,int]]) -> None:
        engine=ReplayEngine(self.store); strategies=tuple(WORKING_TF)
        for index,strategy in enumerate(strategies,1):
            if self.stop_requested: return
            self.pair_status[strategy]="RUNNING"
            self._pair_progress(symbol,pair_index,pair_total,70+(index-1)*5,"REPLAY",strategy=strategy)
            bounds=ranges[WORKING_TF[strategy]]; job_id=stable_id("replay",self.profiles[strategy],symbol)
            existing=self.store.job(job_id); replay_start=max(bounds[0],int(existing.get("last_timestamp") or 0)+1)
            batch_started=time.monotonic()
            def progress(done,total,current=strategy):
                nonlocal batch_started
                fraction=done/max(1,total)
                self._pair_progress(symbol,pair_index,pair_total,70+(index-1+fraction)*5,
                                    "REPLAY",strategy=current)
                self.throttle.yield_after(batch_started); batch_started=time.monotonic()
            def checkpoint(last,done,total):
                self.store.checkpoint(job_id,job_type="REPLAY",strategy_version="research-v1",
                    symbol=symbol,timeframe=WORKING_TF[strategy],range_start=bounds[0],range_end=bounds[1],
                    last_timestamp=last,completed_units=done,total_units=total,status="RUNNING")
            result=engine.replay_profile(run_id,self.profiles[strategy],strategy,symbol,replay_start,bounds[1],
                on_progress=progress,on_checkpoint=checkpoint,should_stop=lambda:self.stop_requested)
            if result.get("status")=="PAUSED": return
            last=result.get("last_timestamp") or int(existing.get("last_timestamp") or replay_start-1)
            self.store.checkpoint(job_id,job_type="REPLAY",strategy_version="research-v1",
                symbol=symbol,timeframe=WORKING_TF[strategy],range_start=bounds[0],range_end=bounds[1],
                last_timestamp=last,completed_units=1,total_units=1,status="COMPLETED")
            self.pair_status[strategy]="COMPLETED"
            run.update({"research_run_id":run_id,"progress":((pair_index-1)+index/len(strategies))/pair_total*100,
                        "status":"RUNNING"}); self.store.save_run(run)
            self._pair_progress(symbol,pair_index,pair_total,70+index*5,"REPLAY",strategy=strategy)
            self.throttle.yield_after(time.monotonic())

    def _features_for(self,symbol: str,timeframe: str,bounds: tuple[int,int],*,on_progress=None) -> None:
        start,end=bounds; job_id=stable_id("features",symbol,timeframe,FEATURE_VERSION)
        existing=self.store.job(job_id); cursor=max(start,int(existing.get("last_timestamp") or 0)+1)
        total=max(1,(end-start)//TIMEFRAME_SECONDS[timeframe])
        completed=max(0,int(existing.get("completed_units") or 0))
        self.store.checkpoint(job_id,job_type="FEATURES",symbol=symbol,timeframe=timeframe,
                              range_start=start,range_end=end,last_timestamp=cursor-1,
                              completed_units=completed,total_units=total,status="RUNNING")
        # One stream read plus batched writes: millions of candles must not cause
        # millions of connections. Each rolling window is still strictly AS OF.
        stream=self.store.candles_between(symbol,timeframe,max(0,cursor-TIMEFRAME_SECONDS[timeframe]*241),end)
        positions=[(idx,int(x["close_time"])) for idx,x in enumerate(stream) if int(x["close_time"])>=cursor]
        snapshots=[]; levels=[]
        batch_started=time.monotonic()
        last=cursor-1; processed=0
        for pos,(idx,as_of) in enumerate(positions,1):
            if self.stop_requested: break
            last=as_of; processed=pos
            candles=stream[max(0,idx-239):idx+1]
            if len(candles)<50: continue
            try:
                snapshot=compute_feature_snapshot(symbol,timeframe,candles,dataset_version=DATASET_VERSION)
                snapshots.append({"symbol":symbol,"timeframe":timeframe,"as_of":as_of,"features":snapshot,
                    "feature_version":FEATURE_VERSION,"dataset_version":DATASET_VERSION,
                    "quality":snapshot["data_quality"]["status"]})
                levels.extend(levels_from_snapshot(snapshot))
            except Exception as exc:
                self.store.save_quality_issue(symbol,timeframe,"FEATURE_CALCULATION",open_time=as_of,
                                              severity="ERROR",detail={"error":str(exc)[:500]})
                # Do not advance the committed checkpoint past a failed feature.
                # Earlier uncommitted rows are safely recomputed after restart.
                raise ResourcePause(f"Feature calculation failed: {symbol} {timeframe} {as_of}") from exc
            if pos%100==0:
                self.store.save_feature_snapshots(snapshots); self.store.upsert_levels(levels)
                snapshots.clear(); levels.clear()
                self.store.checkpoint(job_id,job_type="FEATURES",symbol=symbol,timeframe=timeframe,
                                      range_start=start,range_end=end,last_timestamp=as_of,
                                      completed_units=completed+pos,total_units=total,status="RUNNING")
                if on_progress: on_progress(completed+pos,total)
                load=self.throttle.yield_after(batch_started); batch_started=time.monotonic()
                self.store.set_meta("research_load",{**load,"max_rss_mb":self.throttle.max_rss_mb,
                    "yield_seconds":self.throttle.minimum_yield,"updated_at":utc_now()})
        self.store.save_feature_snapshots(snapshots); self.store.upsert_levels(levels)
        self.store.update_coverage("FEATURE_SNAPSHOT",source="GATE",symbol=symbol,timeframe=timeframe,start=start,end=last,
                                  quality="VALID",availability="HISTORICAL",samples=processed,
                                  metadata={"feature_version":FEATURE_VERSION,"point_in_time":True})
        final_status="PAUSED" if self.stop_requested else "COMPLETED"
        self.store.checkpoint(job_id,job_type="FEATURES",symbol=symbol,timeframe=timeframe,
                              range_start=start,range_end=end,last_timestamp=last,
                              completed_units=(completed+processed if self.stop_requested else total),
                              total_units=total,status=final_status)

    def cycle(self) -> None:
        self.startup(); universe=configured_universe(); ranges=target_ranges()
        run_id,run=self._start_run(universe,ranges); metadata={}
        try:
            metadata=self.client.contract_metadata()
        except Exception as exc:
            logging.warning("[Research] Gate contract metadata unavailable: %s",exc)
        try:
            for pair_index,symbol in enumerate(universe,1):
                if self.stop_requested: break
                self.pair_status={strategy:"PENDING" for strategy in WORKING_TF}
                self._pair_progress(symbol,pair_index,len(universe),0,"START_PAIR")
                self._backfill_symbol(symbol,pair_index,len(universe),ranges,metadata)
                if self.stop_requested: break
                self._materialize_symbol(symbol,pair_index,len(universe),ranges)
                if self.stop_requested: break
                self._replay_symbol(run_id,run,symbol,pair_index,len(universe),ranges)
                if self.stop_requested: break
                manifest=self.store.dataset_manifest(); self.store.set_meta("dataset_manifest",manifest)
                self._pair_progress(symbol,pair_index,len(universe),100,"PAIR_COMPLETED",
                    manifest_hash=manifest["manifest_hash"])
                gc.collect(); self.throttle.yield_after(time.monotonic())
            if self.stop_requested:
                run.update({"research_run_id":run_id,"status":"PAUSED"}); self.store.save_run(run)
                self._set_state("PAUSED",reason="shutdown_requested",updated_at=utc_now()); return
            engine=ReplayEngine(self.store); engine.refresh_open_tracks(run_id,max(x[1] for x in ranges.values()))
            for profile_id in self.profiles.values(): evaluate_profile(self.store,run_id,profile_id)
            run.update({"research_run_id":run_id,"progress":100,"status":"COMPLETED","finished_at":utc_now()})
            self.store.save_run(run); self._set_state("READY",completed_at=utc_now(),pairs=len(universe))
        except ResourcePause as exc:
            run.update({"research_run_id":run_id,"status":"PAUSED","error":str(exc)[:1000]}); self.store.save_run(run)
            self._set_state("RESOURCE_PAUSED",reason=str(exc),rss_mb=self.throttle.rss_mb(),updated_at=utc_now())
            logging.warning("[Research] %s; checkpoint preserved",exc)
        except Exception as exc:
            run.update({"research_run_id":run_id,"status":"FAILED","error":str(exc)[:1000],
                        "finished_at":utc_now()}); self.store.save_run(run)
            self._set_state("FAILED",reason=str(exc)[:500],updated_at=utc_now())
            raise


__all__=["DATASET_VERSION","ResearchWorker","WORKING_TF"]
