from pathlib import Path

from core.trade_manager import (
    _prompt,
    build_structure_facts,
    detect_events,
    management_matrix,
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


def test_manager_v2_remains_advisory_without_live_execution_imports():
    source = Path(__import__("core.trade_manager", fromlist=["x"]).__file__).read_text(encoding="utf-8")
    assert "core.trade_execution" not in source
    assert "place_order" not in source
    assert "cancel_order" not in source


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
        {"structural_target": 112.0, "confirmed_protection_level": 104.8,
         "latest_close": 108.0, "structure_with_trade": True},
        lambda *a, **k: '{"action":"LET_RUN","confidence":0.8,"management_target":999,"protect_level":103,"reason":"continue"}',
    )
    assert review["management_target"] is None
    assert review["protect_level"] is None


def test_groq_may_select_only_supplied_structural_levels():
    review = review_active_trade(
        _state(tp1_seen=1),
        ["BOS"],
        {"structural_target": 112.0, "confirmed_protection_level": 104.8,
         "latest_close": 108.0, "structure_with_trade": True},
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
    assert facts["latest_closed_high"] != 9999


def test_closed_candle_high_low_detects_missed_long_barriers_conservatively():
    events = detect_events(_state(), 102, {
        "closed_candle": True, "new_management_candle": True,
        "latest_closed_high": 115, "latest_closed_low": 97,
    })
    assert "TP1_HIT" in events
    assert "TP2_HIT" in events
    assert "TP3_HIT" in events
    assert "INVALIDATION_HIT" in events
    assert "AMBIGUOUS_BARRIERS" in events


def test_closed_candle_high_low_detects_short_tp_and_stop():
    state = _state(direction="BEARISH", initial_sl=102, initial_tp1=96,
                   initial_tp2=94, initial_tp3=92)
    events = detect_events(state, 100, {
        "closed_candle": True, "new_management_candle": True,
        "latest_closed_high": 103, "latest_closed_low": 95,
    })
    assert {"TP1_HIT", "INVALIDATION_HIT", "AMBIGUOUS_BARRIERS"}.issubset(events)


def test_short_protection_and_target_are_directionally_validated():
    state = _state(direction="BEARISH", initial_sl=105, initial_tp1=94,
                   initial_tp2=90, initial_tp3=86, last_price=98)
    review = review_active_trade(
        state, ["BOS"],
        {"structural_target": 92, "confirmed_protection_level": 101,
         "latest_close": 98, "structure_with_trade": True},
        lambda *_a, **_k: '{"action":"PROTECT","confidence":0.8,"management_target":92,"protect_level":101}',
    )
    assert review["protect_level"] == 101
    assert review["management_target"] == 92


def test_levels_against_direction_or_without_continuation_are_rejected():
    review = review_active_trade(
        _state(last_price=104), ["BOS"],
        {"structural_target": 103, "confirmed_protection_level": 97,
         "latest_close": 104, "structure_with_trade": False},
        lambda *_a, **_k: '{"action":"PROTECT","confidence":0.8,"management_target":103,"protect_level":97}',
    )
    assert review["protect_level"] is None
    assert review["management_target"] is None


def test_prompt_contains_original_thesis_and_strategy_matrix():
    thesis = {"setup_class": "STRONG", "CORE": ["4h location"],
              "TRIGGER": ["15m BOS"], "conflicts": ["funding"]}
    prompt = _prompt(
        _state(strategy="FAST", thesis_json=__import__("json").dumps(thesis)),
        ["BOS"], {"management_matrix": {"cadence": "every closed 5m candle"}},
    )
    assert '"setup_class": "STRONG"' in prompt
    assert '"CORE": ["4h location"]' in prompt
    assert "every closed 5m candle" in prompt


def test_each_strategy_has_its_own_management_policy():
    policies = {name: management_matrix(name) for name in ("FAST", "MTF", "ZONE", "SWING", "WYCKOFF")}
    assert len({policy["protect"] for policy in policies.values()}) == 5
    assert policies["FAST"]["cadence"] == "every closed 5m candle"
    assert "phase" in policies["WYCKOFF"]["let_run"]
