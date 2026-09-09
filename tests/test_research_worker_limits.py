import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from research.store import ResearchStore
from research.features import FEATURE_VERSION
from research.replay import ReplayEngine
from research.worker import ResearchWorker


class ResearchWorkerLimitTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.store=ResearchStore(str(Path(self.temp.name)/"research.db"))
        self.store.ensure_schema()

    def tearDown(self):
        self.temp.cleanup()

    def test_api_budget_is_persisted_across_instances_and_utc_buckets(self):
        now=datetime(2026,9,9,12,34,tzinfo=timezone.utc)
        self.assertTrue(self.store.admit_api_request("GATE_RESEARCH",daily_limit=3,minute_limit=2,now=now))
        reopened=ResearchStore(self.store.database_url)
        self.assertTrue(reopened.admit_api_request("GATE_RESEARCH",daily_limit=3,minute_limit=2,now=now))
        self.assertFalse(reopened.admit_api_request("GATE_RESEARCH",daily_limit=3,minute_limit=2,now=now))
        usage=reopened.api_usage("GATE_RESEARCH",now=now)
        self.assertEqual(usage["day"]["used"],2)
        self.assertEqual(usage["minute"]["used"],2)
        self.assertEqual(usage["day"]["denied"],1)

    def test_pair_pipeline_finishes_all_strategies_before_next_pair(self):
        class Budget:
            used_today=0
        class Client:
            budget=Budget()
            def contract_metadata(self): return {}
        worker=ResearchWorker(self.store,Client())
        events=[]
        original=self.store.set_meta
        def capture(key,value):
            if key=="pair_progress": events.append(dict(value))
            return original(key,value)
        def backfill(symbol,index,total,ranges,metadata):
            worker._pair_progress(symbol,index,total,35,"DATA_QUALITY")
        def features(symbol,index,total,ranges):
            worker._pair_progress(symbol,index,total,70,"FEATURES")
        def replay(run_id,run,symbol,index,total,ranges):
            for strategy in worker.pair_status: worker.pair_status[strategy]="COMPLETED"
            worker._pair_progress(symbol,index,total,95,"REPLAY")
        ranges={tf:(1,1000) for tf in ("5m","15m","1h","4h","1d")}
        with patch("research.worker.configured_universe",return_value=["AAVEUSDT","BNBUSDT"]), \
             patch("research.worker.target_ranges",return_value=ranges), \
             patch.object(self.store,"set_meta",side_effect=capture), \
             patch.object(worker,"_backfill_symbol",side_effect=backfill), \
             patch.object(worker,"_materialize_symbol",side_effect=features), \
             patch.object(worker,"_replay_symbol",side_effect=replay), \
             patch("research.worker.ReplayEngine.refresh_open_tracks",return_value=0), \
             patch("research.worker.evaluate_profile",return_value={}):
            worker.cycle()
        first_100=next(i for i,event in enumerate(events)
                       if event["current_symbol"]=="AAVEUSDT" and event["pair_percent"]==100)
        second_0=next(i for i,event in enumerate(events)
                      if event["current_symbol"]=="BNBUSDT" and event["pair_percent"]==0)
        self.assertLess(first_100,second_0)
        self.assertTrue(all(value=="COMPLETED" for value in events[first_100]["strategy_status"].values()))
        self.assertEqual(events[-1]["overall_percent"],100)

    def test_dashboard_has_pair_progress_and_load_budget(self):
        source=Path("stats_server.py").read_text(encoding="utf-8")
        for marker in ("pair_progress","overall_percent","strategy_status","api_usage","RSS guard"):
            self.assertIn(marker,source)

    def test_replay_reads_point_in_time_features_in_batches(self):
        snapshots=[]
        for tf in ("15m","1h","4h","1d"):
            for as_of in (100,200,300):
                snapshots.append({"symbol":"AAVEUSDT","timeframe":tf,"as_of":as_of,
                    "features":{"price":100,"structure":{"direction":""}},
                    "feature_version":FEATURE_VERSION,"dataset_version":"test","quality":"VALID"})
        self.store.save_feature_snapshots(snapshots)
        engine=ReplayEngine(self.store)
        with patch.object(self.store,"feature_snapshot",side_effect=AssertionError("per-row query")):
            result=engine.replay_profile("run","profile","FAST","AAVEUSDT",100,300)
        self.assertEqual(result["timestamps"],3)
        self.assertEqual(result["attempts"],3)


if __name__=="__main__":
    unittest.main()
