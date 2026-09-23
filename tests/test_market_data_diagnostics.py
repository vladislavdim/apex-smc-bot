import os
from unittest.mock import patch

os.environ.setdefault("TELEGRAM_TOKEN", "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi")
os.environ.setdefault("GROQ_API_KEY", "test-key")

with patch("groq.Groq", return_value=object()):
    import market
from apex.ui.dashboard import server as stats_server


def test_integration_health_exposes_heatmaps_and_remaining_budget():
    snapshot = {
        "source_registry": [{
            "source": "gate", "budget_key": "gate", "mode": "PRIMARY_MARKET",
            "provenance": "Gate public market data",
        }],
        "api_budget": [{
            "source": "gate", "used": {"minute": 2, "hour": 7, "day": 11},
            "remaining_day": 89, "allocation": {"minute": 10, "hour": 50, "day": 100},
            "health": {"failures": 0, "rate_limits": 0, "denied": 0, "blocked_until": 0},
        }],
        "gate_microstructure": [{
            "status": "FRESH", "sequence_status": "VERIFIED", "heatmap_levels": [{}, {}],
            "updated_at": "2026-09-22T07:00:00+00:00",
        }],
    }

    result = stats_server._integration_health(snapshot, {"ok": 5})

    assert result["sources"][0]["status"] == "ACTIVE"
    assert result["sources"][0]["remaining_day"] == 89
    assert "remaining day 89" in result["sources"][0]["provenance"]
    assert {row["feature"] for row in result["features"]} == {
        "structural_liquidity_heatmap", "live_orderbook_heatmap",
    }
    assert result["features"][1]["levels"] == 2


def test_integration_health_does_not_claim_unobserved_live_heatmap_is_connected():
    result = stats_server._integration_health({"source_registry": [], "api_budget": []}, {"ok": 0})

    live = next(row for row in result["features"] if row["feature"] == "live_orderbook_heatmap")
    assert live["status"] == "NO_TELEMETRY"
    assert live["reason_code"] == "NO_SEQUENCE_VERIFIED_DEPTH"


def test_function_health_combines_worker_providers_features_and_on_demand_context():
    snapshot = {
        "runtime_health": {
            "status": "READY", "health": "HEALTHY", "ready": True,
            "new_entries": "ON", "release_sha": "a" * 40,
            "components": {
                "scanner_fast": {
                    "state": "READY", "updated_at": "2026-09-23T08:00:00+00:00",
                    "detail": "", "required": False,
                },
            },
        },
    }
    integrations = {
        "sources": [{
            "source": "gate", "status": "ACTIVE", "reason_code": "REQUESTS_OBSERVED",
            "used_day": 11, "remaining_day": 89, "failures": 0, "rate_limits": 0,
        }],
        "features": [{
            "feature": "live_orderbook_heatmap", "status": "FRESH",
            "reason_code": "VERIFIED", "updated_at": "2026-09-23T08:00:00+00:00",
        }],
    }

    result = stats_server._function_health(snapshot, integrations)

    assert result["ready"] is True
    assert result["release_sha"] == "a" * 12
    rows = {(row["category"], row["function"]): row for row in result["rows"]}
    assert rows[("runtime", "scanner_fast")]["status"] == "READY"
    assert rows[("provider", "gate")]["remaining_day"] == 89
    assert rows[("feature", "live_orderbook_heatmap")]["status"] == "FRESH"
    assert rows[("context", "news_rss")]["status"] == "ON_DEMAND"
    assert rows[("context", "dxy")]["reason_code"] == "REQUEST_DRIVEN_NO_PERIODIC_PROBE"
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
        data = stats_server._build_dashboard_uncached(days=1)
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
        data = stats_server._build_dashboard_uncached(days=1)
    assert data["ltf_watch"]["waiting"] == 1
    assert data["ltf_watch"]["rows"][0]["attempts"] == 2


def test_rendered_dashboard_contains_operational_blocks():
    rendered = stats_server.HTML
    assert "APEX V3 · Production" in rendered
    assert "Gate freshness" in rendered
    assert "LIVE_CONTEXT" in rendered
    assert "Статистика реальных сделок" in rendered
    assert "Research" not in rendered


def test_gate_adapter_error_is_exposed_to_health_telemetry():
    captured = []
    from apex.market.candle_router import GateCandleRouter
    router = GateCandleRouter(
        cache={}, get_shared=lambda *_: [], update_shared=lambda *_: None,
        fetch_gate=lambda *_: {"candles": [], "error": "gate_io:Gate HTTP 503"},
        gate_available=lambda: True,
        record_health=lambda *args, **kwargs: captured.append((args, kwargs)),
        last_closed_at=lambda _rows: None,
    )
    assert router.get_candles("AAVEUSDT", "15m", 120) == []

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

    assert "(strategy=%s OR kind IN" in executed["query"]
    assert "'market_data'" in executed["query"]
    assert "'incident_snapshot'" in executed["query"]
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
