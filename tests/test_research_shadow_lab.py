import json
from pathlib import Path
from unittest.mock import patch

from research.analytics import promotion_proposal, wilson_interval
from research.gate_history import target_ranges
from research.features import FEATURE_VERSION, compute_feature_snapshot, validate_candles
from research.gate_history import ResearchBudget, backfill_pair
from research.live_cache import read as cache_read
from research.replay import ReplayConfig, ReplayEngine
from research.store import ResearchStore
from research.worker import ResearchWorker


def candle(ts, close=100.0, timeframe="15m"):
    period={"5m":300,"15m":900,"1h":3600,"4h":14400,"1d":86400}[timeframe]
    return {"symbol":"AAVEUSDT","timeframe":timeframe,"open_time":ts,
            "close_time":ts+period,"open":close-.2,"high":close+1,"low":close-1,
            "close":close,"volume":100+ts%17,"is_closed":True}


def test_store_is_idempotent_and_range_is_chronological(tmp_path):
    store=ResearchStore(str(tmp_path/"history.db")); store.ensure_schema()
    rows=[candle(0),candle(900),candle(1800)]
    assert store.upsert_candles(rows)==3
    assert store.upsert_candles(rows)==3
    assert store.candle_count("AAVEUSDT","15m")==3
    assert [x["open_time"] for x in store.candles_between("AAVEUSDT","15m",0,2700)]==[0,900,1800]


def test_features_are_point_in_time_and_closed_only():
    rows=[candle(i*900,100+i*.1) for i in range(80)]
    future=candle(80*900,999); future["is_closed"]=False
    snapshot=compute_feature_snapshot("AAVEUSDT","15m",rows+[future],dataset_version="test")
    assert snapshot["as_of"]==rows[-1]["close_time"]
    assert snapshot["price"]==rows[-1]["close"]
    assert snapshot["closed_candles_only"] is True


def test_feature_snapshot_materializes_live_regime_references_point_in_time():
    hourly=[candle(i*3600,100+i*.1,"1h") for i in range(80)]
    four_hour=[candle(i*14400,100+i*.2,"4h") for i in range(80)]
    one=compute_feature_snapshot("AAVEUSDT","1h",hourly,dataset_version="test")
    four=compute_feature_snapshot("AAVEUSDT","4h",four_hour,dataset_version="test")
    assert one["live_regime_reference"]["formula"]=="live_get_market_regime_v1"
    assert one["live_regime_reference"]["mode"] in {"SIDEWAYS","VOLATILE","TRENDING"}
    assert four["live_regime_reference"]["formula"]=="live_detect_market_regime_v2"
    assert four["live_regime_reference"]["type"] in {"accumulation","trend","trend_slow","range"}


def test_quality_detects_gap_duplicate_and_bad_ohlc():
    rows=[candle(0),candle(1800),candle(1800)]
    rows[-1]["low"]=200
    kinds={x["type"] for x in validate_candles(rows,"15m")}
    assert {"MISSING_CANDLES","DUPLICATE","OHLC_INCONSISTENT"} <= kinds


def test_backfill_resumes_without_duplicates(tmp_path):
    store=ResearchStore(str(tmp_path/"history.db")); store.ensure_schema()
    class Client:
        def candles(self,symbol,timeframe,start,end):
            return [candle(ts) for ts in range(start,end+1,900) if ts+900<=end+900]
    one=backfill_pair(store,Client(),"AAVEUSDT","15m",0,3600)
    two=backfill_pair(store,Client(),"AAVEUSDT","15m",0,3600)
    assert one["candles"]==4 and two["upserts"]==0
    assert store.candle_count("AAVEUSDT","15m")==4


def test_backfill_rewinds_and_repairs_a_persisted_gap(tmp_path):
    store=ResearchStore(str(tmp_path/"history.db")); store.ensure_schema()
    class Client:
        repaired=False
        def candles(self,symbol,timeframe,start,end):
            values = (0, 1800, 2700) if not self.repaired else (0, 900, 1800, 2700)
            return [candle(ts) for ts in values if start <= ts and ts+900 <= end]
    client=Client()
    first=backfill_pair(store,client,"AAVEUSDT","15m",0,3600)
    assert first["status"]=="PAUSED"
    assert store.earliest_open_quality_issue("AAVEUSDT","15m",issue_types=("MISSING_CANDLES",))==1800
    client.repaired=True
    second=backfill_pair(store,client,"AAVEUSDT","15m",0,3600)
    assert second["status"]=="COMPLETED"
    assert store.earliest_open_quality_issue("AAVEUSDT","15m",issue_types=("MISSING_CANDLES",)) is None
    assert store.candle_count("AAVEUSDT","15m")==4


def test_replay_waits_for_entry_and_uses_conservative_same_bar(tmp_path):
    store=ResearchStore(str(tmp_path/"history.db")); store.ensure_schema()
    # First future candle fills 100 and touches both 98 SL and 104 TP.
    row=candle(900,100); row.update({"low":97,"high":105})
    store.upsert_candles([row])
    engine=ReplayEngine(store,ReplayConfig(entry_expiry_bars=2,fee_bps=4,slippage_bps=1))
    result=engine.simulate_trade("a","PROFILE_SHADOW",{
        "symbol":"AAVEUSDT","timeframe":"15m","direction":"BULLISH",
        "entry":100,"sl":98,"tp1":102,"tp2":104,"terminal_tp":104},0,1800)
    assert result["status"]=="CLOSED" and result["exit_reason"]=="SL"
    assert result["ambiguity"]=="TP_AND_SL_SAME_CANDLE_SL_FIRST"
    assert result["entry_time"]==1800 and result["net_r"] < result["gross_r"]
    assert result["state"]["funding_r"] is None


def test_research_replay_labels_actual_as_unavailable_and_keeps_virtual_tracks(tmp_path):
    store=ResearchStore(str(tmp_path/"history.db")); store.ensure_schema()
    engine=ReplayEngine(store)
    rows=[candle(900,100)]
    rows[0].update({"low":99,"high":106})
    result=engine._research_track_rows("attempt","FAST",{
        "symbol":"AAVEUSDT","timeframe":"15m","direction":"BULLISH",
        "entry":100,"sl":95,"tp1":105,"tp2":110,"terminal_tp":110},900,rows)
    assert {row["track"] for row in result}=={"ACTUAL","NO_MANAGER","PLAYBOOK_ONLY"}
    actual=next(row for row in result if row["track"]=="ACTUAL")
    assert actual["status"]=="UNAVAILABLE" and actual["net_r"] is None
    assert all(row["state"]["manager_actions_inherited"] is False for row in result)


def test_promotion_is_proposal_only_and_requires_all_safety_gates():
    good=promotion_proposal([.1]*30,baseline_drawdown=2,candidate_drawdown=1)
    assert good["promotion_proposed"] is True and good["auto_activate"] is False
    assert promotion_proposal([.1]*29,baseline_drawdown=2,candidate_drawdown=1)["promotion_proposed"] is False
    assert promotion_proposal([.1]*30,baseline_drawdown=1,candidate_drawdown=2)["promotion_proposed"] is False
    assert promotion_proposal([.1]*30,baseline_drawdown=2,candidate_drawdown=1,safety_violations=1)["promotion_proposed"] is False
    assert wilson_interval(8,10)[0] < .8 < wilson_interval(8,10)[1]


def test_live_cache_is_opt_in(monkeypatch):
    monkeypatch.delenv("APEX_MARKET_DATABASE_URL",raising=False)
    assert cache_read("AAVEUSDT","15m",10)==[]


def test_dashboard_contains_separate_research_tab():
    source=Path("stats_server.py").read_text(encoding="utf-8")
    assert "id=researchTab" in source
    assert "id=researchShadowV2" in source
    assert 'p.path=="/api/research"' in source
    assert "NO REAL EXECUTION" in source
    assert "renderResearchDiagnostics" in source
    assert "Counterfactual edges" in source
    assert "id=liveDashboardV2" in source
    assert "switchDashboard" in source
    assert "Все найденные сетапы и результаты" in source


def test_dashboard_preserves_last_successful_snapshot_during_502():
    source = Path("stats_server.py").read_text(encoding="utf-8")
    assert "function dashboardLoadWarning" in source
    assert "последняя успешная статистика сохранена; нули не подставляются" in source
    assert "const next=await r.json();LAST=next" in source
    assert "function researchLoadWarning" in source
    assert "Research HTTP "+"'"+"+r.status+"+"'"+" · сохранён последний успешный снимок." in source


def test_default_history_is_one_year_and_configurable(monkeypatch):
    monkeypatch.delenv("APEX_RESEARCH_HISTORY_DAYS", raising=False)
    ranges = target_ranges(400 * 86400)
    assert ranges["15m"] == (400 * 86400 - 9990 * 900, 400 * 86400)
    assert ranges["1h"] == (35 * 86400, 400 * 86400)
    monkeypatch.setenv("APEX_RESEARCH_HISTORY_DAYS", "180")
    assert target_ranges(400 * 86400)["4h"] == (220 * 86400, 400 * 86400)
    assert target_ranges(400 * 86400 + 12345)["1d"][1] == 400 * 86400


def test_attempt_checks_and_source_registry_are_idempotent(tmp_path):
    store=ResearchStore(str(tmp_path/"history.db")); store.ensure_schema()
    store.upsert_source_contract("GATE",kind="MARKET_DATA",owner="Gate",authority="CANONICAL",
                                freshness_sla_seconds=120,status="READY")
    store.upsert_source_contract("GATE",kind="MARKET_DATA",owner="Gate",authority="CANONICAL",
                                freshness_sla_seconds=120,status="READY")
    checks=[{"check_code":"LOCATION","label":"OB/FVG","status":"PASS","role":"HARD_GATE",
             "domain":"LOCATION","measured":{"ob":True},"threshold":{"value":True}}]
    store.save_attempt({"attempt_id":"a","research_run_id":"r","profile_id":"p",
                       "parent_strategy":"FAST","symbol":"AAVEUSDT","decision_time":1,
                       "snapshot":{}})
    assert store.save_attempt_checks("a",checks)==1
    assert store.save_attempt_checks("a",checks)==1
    dashboard=store.dashboard()
    assert dashboard["schema_version"]==4
    assert dashboard["sources"][0]["source"]=="GATE"
    assert dashboard["checks"][0]["count"]==1


def test_worker_end_to_end_is_checkpointed_and_execution_isolated(tmp_path):
    store=ResearchStore(str(tmp_path/"history.db"))
    periods={"5m":300,"15m":900,"1h":3600,"4h":14400,"1d":86400}
    class Budget:
        used_today=0
    class Client:
        budget=Budget()
        def contract_metadata(self): return {"AAVE_USDT":{"name":"AAVE_USDT","create_time":1}}
        def candles(self,symbol,timeframe,start,end):
            p=periods[timeframe]
            return [candle(ts,100+ts/p*.02,timeframe) for ts in range(start,end,p) if ts+p<=end]
    ranges={tf:(1,1+p*70) for tf,p in periods.items()}
    worker=ResearchWorker(store,Client())
    with patch("research.worker.configured_universe",return_value=["AAVEUSDT"]), \
         patch("research.worker.target_ranges",return_value=ranges), \
         patch("research.worker.time.time",return_value=10**9):
        worker.cycle()
    dashboard=store.dashboard()
    assert dashboard["candles"] and dashboard["jobs"] and dashboard["runs"]
    assert all(run["run_type"]=="POINT_IN_TIME_CAUSAL_SHADOW" for run in dashboard["runs"])
    source=(Path("research/worker.py").read_text()+Path("research/replay.py").read_text()).lower()
    assert "import binance" not in source and "import telegram" not in source and "import groq" not in source
