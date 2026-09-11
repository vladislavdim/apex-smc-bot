import os
from unittest.mock import patch

os.environ.setdefault("TELEGRAM_TOKEN", "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi")
os.environ.setdefault("GROQ_API_KEY", "test-key")

with patch("groq.Groq", return_value=object()):
    import market
import stats_server
from core import market_data_health
from core import smc_engine


def test_market_data_events_are_transition_throttled():
    captured = []
    market_data_health.reset_market_data_state_for_tests()
    with patch.object(market_data_health, "emit_event", side_effect=lambda *args, **kwargs: captured.append((args, kwargs))):
        market_data_health.record_market_data("AAVEUSDT", "15m", False, reason="Gate HTTP 500")
        market_data_health.record_market_data("AAVEUSDT", "15m", False, reason="Gate HTTP 500")
        market_data_health.record_market_data(
            "AAVEUSDT", "15m", True, candle_count=120,
            last_closed_candle_at=1788777900000,
        )
    assert len(captured) == 2
    assert captured[0][0][3]["status"] == "FAILED"
    assert captured[1][0][3]["status"] == "OK"
    assert captured[1][0][3]["last_closed_candle_at"] == "2026-09-07T10:45:00+00:00"


def test_dashboard_aggregates_gate_and_ltf_lifecycle():
    events = [
        {"event_key": "m1", "kind": "market_data", "strategy": "SYSTEM", "symbol": "AAVEUSDT",
         "occurred_at": "2026-09-07T10:00:00+00:00", "payload": {"timeframe": "15m", "status": "OK", "source": "Gate", "last_success_at": "2026-09-07T10:00:00+00:00"}},
        {"event_key": "m2", "kind": "market_data", "strategy": "SYSTEM", "symbol": "AAVEUSDT",
         "occurred_at": "2026-09-07T10:05:00+00:00", "payload": {"timeframe": "15m", "status": "FAILED", "source": "Gate", "reason": "Gate HTTP 500"}},
        {"event_key": "l1", "kind": "ltf_watch", "strategy": "MTF", "symbol": "ETHUSDT",
         "occurred_at": "2026-09-07T10:06:00+00:00", "payload": {"state": "WAITING", "required_timeframe": "15m", "reason": "waiting BOS", "attempts": 2}},
    ]
    with patch.object(stats_server, "_fetch", return_value=events):
        data = stats_server.build_dashboard(days=1)
    assert data["market_data"]["total"] == 1
    assert data["market_data"]["failed"] == 1
    assert data["market_data"]["rows"][0]["last_success_at"] == "2026-09-07T10:00:00+00:00"
    assert data["ltf_watch"]["waiting"] == 1
    assert data["ltf_watch"]["rows"][0]["required_timeframe"] == "15m"


def test_market_freshness_is_separate_from_request_success():
    from datetime import datetime, timezone
    now = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)
    fresh = stats_server._market_freshness("2026-09-07T11:30:00+00:00", "15m", now)
    stale = stats_server._market_freshness("2026-09-07T10:00:00+00:00", "15m", now)
    assert fresh["freshness_status"] == "FRESH"
    assert stale["freshness_status"] == "STALE"


def test_ltf_dashboard_deduplicates_by_setup_id_not_cycles():
    base = {"kind": "ltf_watch", "strategy": "ZONE", "symbol": "APTUSDT"}
    events = [
        {**base, "event_key": "l1", "occurred_at": "2026-09-07T10:00:00+00:00", "payload": {"setup_id": "same", "state": "WAITING", "required_timeframe": "1h", "attempts": 1}},
        {**base, "event_key": "l2", "occurred_at": "2026-09-07T10:05:00+00:00", "payload": {"setup_id": "same", "state": "WAITING", "required_timeframe": "1h", "attempts": 2}},
    ]
    with patch.object(stats_server, "_fetch", return_value=events):
        data = stats_server.build_dashboard(days=1)
    assert data["ltf_watch"]["waiting"] == 1
    assert data["ltf_watch"]["rows"][0]["attempts"] == 2


def test_rendered_dashboard_contains_operational_blocks():
    from core import runtime_observability

    rendered = runtime_observability._patch_stats_html(stats_server.HTML)
    assert "Market Data / Gate" in rendered
    assert "PENDING LTF lifecycle" in rendered
    assert "Gate requests OK" in rendered
    assert "Stale TF" in rendered
    assert "SWING volume shadow" in rendered


def test_gate_adapter_error_is_exposed_to_health_telemetry():
    captured = []
    with patch.dict(market.candle_cache, {}, clear=True), \
         patch.object(market, "get_global_candles", return_value=[]), \
         patch.object(market, "_ROUTER_OK", False), \
         patch.object(market, "_SMC_ENGINE_OK", True), \
         patch.object(market, "get_candles_smart", return_value={"candles": [], "error": "gate_io:Gate HTTP 503"}), \
         patch.object(market, "_record_market_data", side_effect=lambda *args, **kwargs: captured.append((args, kwargs))):
        assert market.get_candles("AAVEUSDT", "15m", 120) == []

    assert captured[-1][1]["reason"] == "SMC adapter: gate_io:Gate HTTP 503"


def test_operational_telemetry_survives_strategy_filter():
    executed = {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            executed["query"] = query
            executed["params"] = params

        def fetchall(self):
            return []

    class Connection:
        def cursor(self, **_kwargs):
            return Cursor()

        def close(self):
            pass

    with patch.object(stats_server, "_connect", return_value=Connection()):
        stats_server._fetch(1, "FAST", "")

    assert "(strategy=%s OR kind='market_data')" in executed["query"]
    assert "FAST" in executed["params"]


def test_smc_short_candle_request_is_not_a_false_failure():
    candles = [{"close": 1.0}] * 3
    smc_engine._candle_cache.clear()
    with patch.object(smc_engine, "_ordered_sources_for_interval", return_value=["gate_io"]), \
         patch.dict(smc_engine._FETCHERS, {"gate_io": lambda *_args: candles}, clear=True), \
         patch.object(smc_engine, "_record"), \
         patch.object(smc_engine, "_learn_fact"):
        result = smc_engine.get_candles_smart("AAVEUSDT", "5m", 3)
    assert result["source"] == "gate_io"
    assert result["candles"] == candles
    assert result["error"] == ""
