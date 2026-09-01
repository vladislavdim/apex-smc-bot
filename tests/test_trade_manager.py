from core.trade_manager import (
    build_structure_facts,
    detect_events,
    r_multiple,
    review_active_trade,
)


def _state(**extra):
    state = {
        "signal_id": 1,
        "symbol": "BTCUSDT",
        "strategy": "SWING",
        "direction": "BULLISH",
        "management_tf": "1h",
        "initial_entry": 100.0,
        "initial_sl": 98.0,
        "initial_tp1": 106.0,
        "initial_tp2": 110.0,
        "initial_tp3": 114.0,
        "initial_rr": 3.0,
        "tp1_seen": 0,
        "tp2_seen": 0,
    }
    state.update(extra)
    return state


def test_initial_rr_is_not_rewritten_by_manager_r():
    assert r_multiple("BULLISH", 100, 98, 112) == 6.0
    assert _state()["initial_rr"] == 3.0


def test_tp1_is_a_material_event():
    assert "TP1_HIT" in detect_events(_state(), 106.1, {})


def test_no_event_does_not_call_groq():
    calls = []
    review = review_active_trade(_state(), [], {}, lambda *a, **k: calls.append(1))
    assert review["action"] == "HOLD"
    assert review["groq_called"] is False
    assert calls == []


def test_initial_invalidation_exits_without_groq():
    calls = []
    review = review_active_trade(
        _state(), ["INVALIDATION_HIT"], {}, lambda *a, **k: calls.append(1)
    )
    assert review["action"] == "EXIT"
    assert calls == []


def test_groq_cannot_invent_management_target():
    review = review_active_trade(
        _state(tp1_seen=1),
        ["BOS"],
        {"structural_target": 112.0, "confirmed_protection_level": 104.8},
        lambda *a, **k: '{"action":"LET_RUN","confidence":0.8,"management_target":999,"protect_level":103,"reason":"continue"}',
    )
    assert review["management_target"] is None
    assert review["protect_level"] is None


def test_groq_may_select_only_supplied_structural_levels():
    review = review_active_trade(
        _state(tp1_seen=1),
        ["BOS"],
        {"structural_target": 112.0, "confirmed_protection_level": 104.8},
        lambda *a, **k: '{"action":"LET_RUN","confidence":0.8,"management_target":112,"protect_level":104.8,"reason":"continue"}',
    )
    assert review["management_target"] == 112.0
    assert review["protect_level"] == 104.8


def test_structure_uses_closed_candles_only():
    candles = []
    for i in range(30):
        candles.append({"timestamp": i, "open": 100+i*.1, "high": 101+i*.1, "low": 99+i*.1, "close": 100.5+i*.1, "volume": 1})
    candles[-1]["close"] = 9999  # mutable edge must be ignored
    facts = build_structure_facts(candles, "BULLISH")
    assert facts["latest_close"] != 9999
