import os
from datetime import datetime, timezone
from unittest.mock import patch

os.environ.setdefault("TELEGRAM_TOKEN", "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi")
os.environ.setdefault("GROQ_API_KEY", "test-key")

with patch("groq.Groq", return_value=object()):
    import market








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


def test_smc_cache_refetches_when_larger_history_is_requested():
    smc_engine._candle_cache.clear()
    calls = []

    def fetch(_symbol, _interval, limit):
        calls.append(limit)
        return [{"close": float(i)} for i in range(limit)]

    with patch.object(smc_engine, "_ordered_sources_for_interval", return_value=["gate_io"]), \
         patch.dict(smc_engine._FETCHERS, {"gate_io": fetch}, clear=True), \
         patch.object(smc_engine, "_record"):
        assert len(smc_engine.get_candles_smart("AUDITUSDT", "4h", 50)["candles"]) == 50
        larger = smc_engine.get_candles_smart("AUDITUSDT", "4h", 61)
        assert len(larger["candles"]) == 61
        from apex.market.runtime_cache import get_confirmed_candles
        assert len(get_confirmed_candles(larger["candles"])) == 60
        smaller = smc_engine.get_candles_smart("AUDITUSDT", "4h", 3)
        assert smaller["candles"] == larger["candles"][-3:]
        assert len(smc_engine.get_candles_smart("AUDITUSDT", "4h", 61)["candles"]) == 61
    assert calls == [50, 61]
    smc_engine._candle_cache.clear()
