import sqlite3

from core.manager_playbooks import promotion_assessment, shadow_features
from core.trade_manager import (
    MANAGEMENT_TF, NO_PROGRESS_BARS, PROGRESS_TF, TRANSITION_MATRIX, activate_v2_once,
    confirm_manager_action, ensure_trade_manager_schema, load_state, no_progress_event_due,
    register_active_trade, replay_closed_candle, review_active_trade,
    validate_transition,
)


def state(**changes):
    value = {
        "signal_id": 1, "symbol": "AAVEUSDT", "strategy": "MTF",
        "direction": "BULLISH", "manager_version": 2, "manager_state": "PROTECTED",
        "management_tf": "15m", "initial_entry": 100.0, "initial_sl": 95.0,
        "initial_tp1": 105.0, "initial_tp2": 110.0, "initial_tp3": 115.0,
        "initial_rr": 3.0, "tp1_seen": 0, "no_progress_bars": 0,
        "progress_anchor_r": 0.0,
    }
    value.update(changes)
    return value


def test_every_declared_transition_and_closed_reconciliation_invariants():
    for source, actions in TRANSITION_MATRIX.items():
        for action, target in actions.items():
            assert validate_transition(source, action) == (True, target)
    assert not validate_transition("CLOSED", "CLOSE")[0]
    assert validate_transition("RECONCILIATION_REQUIRED", "HOLD")[0]
    assert not validate_transition("RECONCILIATION_REQUIRED", "PROTECT")[0]


def test_groq_contract_rejects_compound_action_and_failure_is_hold():
    malformed = review_active_trade(
        state(), ["BOS"], {},
        lambda *_a, **_k: '{"action":"PARTIAL_EXIT+LET_RUN","confidence":1}',
    )
    assert malformed["action"] == "HOLD"
    failed = review_active_trade(state(), ["BOS"], {}, lambda *_a, **_k: (_ for _ in ()).throw(TimeoutError()))
    assert failed["action"] == "HOLD"


def test_no_progress_is_strategy_specific_and_event_only():
    facts = {"new_management_candle": True}
    for strategy, threshold in NO_PROGRESS_BARS.items():
        row = state(strategy=strategy, no_progress_bars=threshold - 1)
        assert no_progress_event_due(row, 0.1, facts)
    assert MANAGEMENT_TF == {"FAST": "5m", "MTF": "15m", "ZONE": "15m", "SWING": "1h", "WYCKOFF": "1h"}
    assert PROGRESS_TF == {"FAST": "15m", "MTF": "15m", "SWING": "1h", "ZONE": "1h", "WYCKOFF": "4h"}


def test_cutover_is_atomic_idempotent_and_fences_live(tmp_path):
    db = str(tmp_path / "brain.db")
    ensure_trade_manager_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE trade_executions(signal_id INTEGER,mode TEXT,status TEXT)")
    register_active_trade({"id": 1, "symbol": "AAVEUSDT", "grade": "MTF", "direction": "BULLISH", "entry": 100, "sl": 95, "tp1": 105, "tp2": 110, "tp3": 115}, db_path=db)
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO trade_executions VALUES(1,'live','PROTECTED')")
        conn.execute("DELETE FROM trade_manager_runtime WHERE key='v2_cutover_complete'")
        conn.execute("UPDATE trade_manager_state SET manager_version=1")
    first = activate_v2_once(db)
    assert first["reconciliation_required"] == 1
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT value FROM trade_manager_runtime WHERE key='opens_enabled'").fetchone()[0] == "0"
    second = activate_v2_once(db)
    assert second["reconciliation_required"] == 1
    assert load_state(1, db)["manager_state"] == "RECONCILIATION_REQUIRED"


def test_three_replay_tracks_are_deduplicated_and_isolated(tmp_path):
    db = str(tmp_path / "brain.db")
    register_active_trade({"id": 1, "symbol": "AAVEUSDT", "grade": "MTF", "direction": "BULLISH", "entry": 100, "sl": 95, "tp1": 105, "tp2": 110, "tp3": 115}, db_path=db)
    row = load_state(1, db)
    facts = {"new_management_candle": True, "management_candle_id": "c1", "latest_closed_high": 106, "latest_closed_low": 99, "latest_close": 105, "_book_shadow": {"effort_without_result": True}}
    replay_closed_candle(row, facts, {"action": "HOLD"}, db)
    replay_closed_candle(row, facts, {"action": "CLOSE"}, db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM trade_manager_replay_events").fetchone()[0] == 3
        tracks = dict(conn.execute("SELECT track,exit_reason FROM trade_manager_replay_tracks"))
    assert tracks["ACTUAL"] is None
    assert tracks["NO_MANAGER"] is None
    assert tracks["PLAYBOOK_ONLY"] == "SHADOW_EFFORT_WITHOUT_RESULT_AFTER_TP1"


def test_exchange_transition_commits_only_after_confirmation(tmp_path):
    db = str(tmp_path / "brain.db")
    register_active_trade({"id": 1, "symbol": "AAVEUSDT", "grade": "MTF", "direction": "BULLISH", "entry": 100, "sl": 95, "tp1": 105}, db_path=db)
    assert not confirm_manager_action(1, "PROTECT", "ERROR", db)
    assert load_state(1, db)["manager_state"] == "PROTECTED"
    assert confirm_manager_action(1, "PROTECT", "EXECUTED", db)
    assert load_state(1, db)["manager_state"] == "MANAGING"


def test_book_rules_are_gate_relative_shadow_and_never_auto_activate():
    candles = [{"open": 1, "high": 2, "low": 1, "close": 1.8, "volume": 10} for _ in range(19)]
    candles.append({"open": 1, "high": 1.5, "low": 1, "close": 1.1, "volume": 20})
    features = shadow_features(candles, "BULLISH")
    assert features["source"] == "Gate_relative_volume"
    assert features["execution_scope"] == "PLAYBOOK_ONLY_SHADOW"
    rows = [{"old_r": 0.1, "new_r": 0.2, "delta_r": 0.1} for _ in range(30)]
    result = promotion_assessment(rows)
    assert result["promotion_proposed"]
    assert result["auto_activated"] is False
