import json
import sqlite3

from core.trade_manager import (
    finalize_manager_trade,
    format_telegram_update,
    load_manager_message,
    load_active_states,
    load_state,
    manager_cycle,
    reconcile_manager_states_from_signals,
    register_pending_signals,
    store_manager_message,
    configure_manager_message_state,
    configure_manager_state,
    confirm_manager_action,
    confirm_v2_reconciliation,
)
from apex.db.state_db import migrate_state
from apex.db.manager_migration import import_legacy_manager
from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
from apex.db.repositories.executions import ExecutionRepository
from core.trade_manager_telegram import (
    configure_manager_dashboard_state, fetch_manager_trade,
    fetch_manager_trades, format_final_trade_card,
)


def _db(tmp_path, status="active"):
    path = str(tmp_path / "brain.db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE signals (
            id INTEGER PRIMARY KEY, symbol TEXT, direction TEXT, entry REAL,
            sl REAL, tp1 REAL, tp2 REAL, tp3 REAL, timeframe TEXT,
            grade TEXT, signal_type TEXT, result TEXT
        );
        CREATE TABLE signal_execution_state (
            signal_id INTEGER PRIMARY KEY, status TEXT, activated_at TEXT,
            last_checked_at TEXT, closed_at TEXT, cancel_reason TEXT,
            created_at TEXT
        );
        CREATE TABLE setup_assessments (
            signal_id INTEGER, stage TEXT, updated_at TEXT, assessment_json TEXT,
            symbol TEXT, strategy TEXT, direction TEXT
        );
        """
    )
    conn.execute(
        "INSERT INTO signals VALUES (1,'BTCUSDT','BULLISH',100,95,110,115,120,'1h','MTF','MTF','pending')"
    )
    conn.execute(
        "INSERT INTO signal_execution_state(signal_id,status) VALUES (1,?)",
        (status,),
    )
    conn.execute(
        "INSERT INTO setup_assessments VALUES (1,'FINAL','2026-09-01',?,?,?,?)",
        (json.dumps({
            "state": "STRONG", "thesis": "HTF location to closed BOS",
            "evidence_roles": {"CORE": ["HTF location"], "TRIGGER": ["closed BOS"],
                               "TIER1": ["displacement"]},
            "conflicts": [], "dimensions": {"trigger_quality": "STRONG"},
        }), "BTCUSDT", "MTF", "BULLISH"),
    )
    conn.commit()
    conn.close()
    return path


def _candles():
    return [
        {
            "timestamp": index,
            "open": 100 + index * 0.05,
            "high": 101 + index * 0.05,
            "low": 99 + index * 0.05,
            "close": 100.5 + index * 0.05,
            "volume": 10,
        }
        for index in range(30)
    ]


def test_waiting_entry_is_not_registered(tmp_path):
    db_path = _db(tmp_path, "waiting_entry")
    assert register_pending_signals(db_path) == 0
    assert load_state(1, db_path) is None


def test_active_trade_registration_survives_restart_without_duplicates(tmp_path):
    db_path = _db(tmp_path)
    assert register_pending_signals(db_path) == 1
    assert register_pending_signals(db_path) == 0
    state = load_state(1, db_path)
    assert state["initial_entry"] == 100
    assert state["initial_sl"] == 95
    assert state["initial_tp1"] == 110
    thesis = json.loads(state["thesis_json"])
    assert thesis["setup_class"] == "STRONG"
    assert thesis["CORE"] == ["HTF location"]
    assert thesis["TRIGGER"] == ["closed BOS"]
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM trade_manager_state").fetchone()[0] == 1
    conn.close()


def test_manager_cycle_processes_each_closed_candle_once_and_keeps_levels(tmp_path):
    db_path = _db(tmp_path)
    calls = []

    def groq(*args, **kwargs):
        calls.append(1)
        return '{"action":"HOLD","confidence":0.8,"reason":"structure intact","protect_level":null,"management_target":null}'

    first = manager_cycle(
        lambda: {"BTCUSDT": {"price": 102}},
        lambda *_args: _candles(),
        groq,
        db_path=db_path,
    )
    second = manager_cycle(
        lambda: {"BTCUSDT": {"price": 102}},
        lambda *_args: _candles(),
        groq,
        db_path=db_path,
    )

    assert len(first) == 1
    assert second == []
    # A routine closed-candle heartbeat is persisted for replay but must not
    # invoke Groq when no material management event occurred.
    assert len(calls) == 0
    state = load_state(1, db_path)
    assert (state["initial_entry"], state["initial_sl"], state["initial_tp1"]) == (100, 95, 110)


def test_manager_prompt_receives_compact_fresh_external_context(tmp_path):
    db_path = _db(tmp_path)
    prompts = []

    def groq(prompt, **_kwargs):
        prompts.append(prompt)
        return '{"action":"HOLD","confidence":0.8,"reason":"context checked"}'

    manager_cycle(
        lambda: {"BTCUSDT": {"price": 111}},
        lambda *_args: _candles(),
        groq,
        external_context=lambda *_args: {
            "open_interest": {"change_1h_pct": 2.1, "trend": "rising", "status": "fresh"},
            "liquidations": {"dominance": "short", "status": "fresh"},
            "external_bias": "bullish", "external_confidence": .71,
            "significant_conflict": True,
            "large_orders": {"source_values": {"bulky": "excluded"}, "bias": "bullish"},
        },
        db_path=db_path,
    )
    assert prompts
    assert '"change_1h_pct": 2.1' in prompts[0]
    assert '"dominance": "short"' in prompts[0]
    assert "source_values" not in prompts[0]


def test_manager_notification_escapes_groq_text():
    text = format_telegram_update(
        {
            "symbol": "BTCUSDT", "strategy": "MTF", "direction": "BULLISH",
            "initial_entry": 100, "initial_sl": 95,
            "manager_protect_level": None, "manager_target": None,
        },
        102,
        ["BOS"],
        {"action": "HOLD", "confidence": 0.8, "reason": "price < risk", "next_trigger": "close > level"},
    )
    assert "price &lt; risk" in text
    assert "close &gt; level" in text


def test_manager_alerts_once_after_three_missing_tf_cycles_and_recovers(tmp_path):
    db_path = _db(tmp_path)
    for _ in range(2):
        assert manager_cycle(
            lambda: {"BTCUSDT": {"price": 102}},
            lambda *_args: [],
            lambda *_args, **_kwargs: "{}",
            db_path=db_path,
        ) == []
    third = manager_cycle(
        lambda: {"BTCUSDT": {"price": 102}},
        lambda *_args: [],
        lambda *_args, **_kwargs: "{}",
        db_path=db_path,
    )
    assert len(third) == 1
    assert third[0]["degraded"] is True
    assert "15m data unavailable 3 cycles" in third[0]["telegram"]
    assert manager_cycle(
        lambda: {"BTCUSDT": {"price": 102}},
        lambda *_args: [],
        lambda *_args, **_kwargs: "{}",
        db_path=db_path,
    ) == []
    manager_cycle(
        lambda: {"BTCUSDT": {"price": 102}},
        lambda *_args: _candles(),
        lambda *_args, **_kwargs: '{"action":"HOLD","confidence":0.8}',
        db_path=db_path,
    )
    state = load_state(1, db_path)
    assert state["data_failure_count"] == 0
    assert state["data_failure_notified"] == 0


def test_compact_message_mapping_is_per_destination_and_restart_safe(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    store_manager_message(1, -1001, 41, "first", db_path=db_path)
    store_manager_message(1, -1002, 52, "second", thread_id=262, db_path=db_path)
    store_manager_message(1, -1001, 41, "edited", db_path=db_path)
    assert load_manager_message(1, -1001, db_path=db_path)["message_id"] == 41
    assert load_manager_message(1, -1002, 262, db_path)["message_id"] == 52


def test_manager_message_identity_can_be_owned_by_state_db(tmp_path):
    legacy_path = str(tmp_path / "brain.db")
    state_path = str(tmp_path / "apex_state.db")

    def factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = factory()
    migrate_state(conn)
    conn.close()
    configure_manager_message_state(factory)
    try:
        store_manager_message(7, -1001, 88, "manager", db_path=legacy_path)
        stored = load_manager_message(7, -1001, db_path=legacy_path)
    finally:
        configure_manager_message_state(None)
    assert stored["message_id"] == 88
    assert stored["is_final"] == 0
    assert not (tmp_path / "brain.db").exists()


def test_manager_review_is_state_only_after_cutover(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    import_legacy_manager(lambda: sqlite3.connect(db_path), state_factory, refresh=True)
    configure_manager_state(state_factory)
    try:
        state = load_state(1, db_path)
        from core.trade_manager import persist_review
        persist_review(
            state, 102, ["BOS"],
            {"management_candle_id": "closed-15m-1", "new_management_candle": True},
            {"action": "HOLD", "confidence": 0.8, "reason": "continue"}, db_path,
        )
    finally:
        configure_manager_state(None)
    legacy = sqlite3.connect(db_path)
    assert legacy.execute(
        "SELECT COUNT(*) FROM trade_manager_events WHERE event_type='BOS'"
    ).fetchone()[0] == 0
    legacy.close()
    target = state_factory()
    assert target.execute(
        "SELECT COUNT(*) FROM manager_events WHERE event_type='BOS'"
    ).fetchone()[0] == 1
    target.close()


def test_manager_data_availability_is_state_only_after_cutover(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    import_legacy_manager(lambda: sqlite3.connect(db_path), state_factory, refresh=True)
    configure_manager_state(state_factory)
    try:
        state = load_state(1, db_path)
        legacy = sqlite3.connect(db_path)
        legacy.execute("UPDATE trade_manager_state SET status='CLOSED' WHERE signal_id=1")
        legacy.commit(); legacy.close()
        assert load_state(1, db_path)["status"] == "ACTIVE"
        assert [item["signal_id"] for item in load_active_states(db_path)] == [1]
        from core.trade_manager import _record_data_availability
        assert _record_data_availability(state, False, "Gate timeout", db_path) == (1, False)
        target = state_factory()
        target_row = target.execute(
            "SELECT data_failure_count,last_data_error FROM manager_positions WHERE signal_id=1"
        ).fetchone()
        target.close()
        assert tuple(target_row) == (1, "Gate timeout")
        legacy = sqlite3.connect(db_path)
        legacy_row = legacy.execute(
            "SELECT data_failure_count,last_data_error FROM trade_manager_state WHERE signal_id=1"
        ).fetchone()
        legacy.close()
        assert tuple(legacy_row) == (0, None)
    finally:
        configure_manager_state(None)


def test_confirmed_manager_transition_reads_state_not_legacy(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    import_legacy_manager(lambda: sqlite3.connect(db_path), state_factory, refresh=True)
    legacy = sqlite3.connect(db_path)
    legacy.execute("UPDATE trade_manager_state SET manager_state='CLOSED' WHERE signal_id=1")
    legacy.commit(); legacy.close()
    configure_manager_state(state_factory)
    try:
        assert confirm_manager_action(1, "HOLD", "INTERNAL_CONFIRMED", db_path)
        state = load_state(1, db_path)
    finally:
        configure_manager_state(None)
    assert state["manager_state"] == "PROTECTED"
    legacy = sqlite3.connect(db_path)
    assert legacy.execute(
        "SELECT manager_state FROM trade_manager_state WHERE signal_id=1"
    ).fetchone()[0] == "CLOSED"
    legacy.close()


def test_exchange_reconciliation_resolves_state_before_legacy_copy(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    import_legacy_manager(lambda: sqlite3.connect(db_path), state_factory, refresh=True)
    conn = state_factory()
    conn.execute(
        "UPDATE manager_positions SET manager_state='RECONCILIATION_REQUIRED' WHERE signal_id=1"
    )
    conn.commit(); conn.close()
    configure_manager_state(state_factory)
    try:
        assert confirm_v2_reconciliation(1, "PROTECTED", db_path)
        assert confirm_v2_reconciliation(1, "PROTECTED", db_path)
        state = load_state(1, db_path)
    finally:
        configure_manager_state(None)
    assert state["manager_state"] == "PROTECTED"
    legacy = sqlite3.connect(db_path)
    assert legacy.execute(
        "SELECT manager_state FROM trade_manager_state WHERE signal_id=1"
    ).fetchone()[0] == "PROTECTED"
    legacy.close()


def test_new_manager_registration_is_state_first_and_immutable(tmp_path):
    db_path = _db(tmp_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    SignalLifecycleRepository(state_factory).import_row({
        "signal_id": 1, "status": "active", "result": "pending",
    })
    ExecutionRepository(state_factory).register({
        "signal_id": 1, "mode": "live", "symbol": "BTCUSDT",
        "direction": "BULLISH", "status": "PROTECTED",
        "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
        "quantity": 0.25, "entry_order_id": "entry-1", "stop_order_id": "stop-1",
    })
    configure_manager_state(state_factory)
    try:
        assert register_pending_signals(db_path) == 1
        # A retry is idempotent in the canonical State store.
        assert register_pending_signals(db_path) == 0
    finally:
        configure_manager_state(None)
    target = state_factory()
    row = target.execute(
        "SELECT signal_id,initial_entry,initial_sl,initial_tp1 FROM manager_positions"
    ).fetchone()
    target.close()
    assert tuple(row) == (1, 100.0, 95.0, 110.0)
    legacy = sqlite3.connect(db_path)
    assert legacy.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='trade_manager_state'"
    ).fetchone()[0] == 0
    legacy.close()


def test_state_lifecycle_is_required_before_manager_registration(tmp_path):
    db_path = _db(tmp_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    configure_manager_state(state_factory)
    try:
        assert register_pending_signals(db_path) == 0
        SignalLifecycleRepository(state_factory).import_row({
            "signal_id": 1, "status": "active", "result": "pending",
        })
        assert register_pending_signals(db_path) == 0
        ExecutionRepository(state_factory).register({
            "signal_id": 1, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "PROTECTED",
            "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
            "quantity": 0.25, "entry_order_id": "entry-1", "stop_order_id": "stop-1",
        })
        assert register_pending_signals(db_path) == 1
    finally:
        configure_manager_state(None)


def test_state_execution_rejection_is_not_registered_as_live_position(tmp_path):
    db_path = _db(tmp_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    SignalLifecycleRepository(state_factory).import_row({
        "signal_id": 1, "status": "active", "result": "pending",
    })
    ExecutionRepository(state_factory).register({
        "signal_id": 1, "mode": "live", "symbol": "BTCUSDT",
        "direction": "BULLISH", "status": "BLOCKED_RISK",
        "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
        "quantity": 0, "entry_order_id": "",
    })
    configure_manager_state(state_factory)
    try:
        assert register_pending_signals(db_path) == 0
        assert load_state(1, db_path) is None
    finally:
        configure_manager_state(None)


def test_manager_dashboard_reads_state_without_legacy_db(tmp_path):
    state_path = str(tmp_path / "apex_state.db")

    def factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = factory(); migrate_state(conn); conn.close()
    repository = __import__(
        "apex.db.repositories.manager", fromlist=["ManagerRepository"]
    ).ManagerRepository(factory)
    repository.register({
        "signal_id": 44, "symbol": "AAVEUSDT", "strategy": "ZONE",
        "direction": "BULLISH", "management_tf": "15m",
        "initial_entry": 100, "initial_sl": 95, "initial_tp1": 110,
        "manager_version": 3,
    })
    configure_manager_dashboard_state(factory)
    try:
        rows = fetch_manager_trades(str(tmp_path / "must-not-exist.db"))
        detail = fetch_manager_trade(str(tmp_path / "must-not-exist.db"), 44)
    finally:
        configure_manager_dashboard_state(None)
    assert rows[0]["signal_id"] == 44
    assert detail["state"]["initial_sl"] == 95
    assert not (tmp_path / "must-not-exist.db").exists()


def test_state_manager_normal_writes_do_not_create_legacy_db(tmp_path):
    state_path = str(tmp_path / "apex_state.db")
    legacy_path = str(tmp_path / "must-not-exist.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    configure_manager_state(state_factory)
    try:
        from core.trade_manager import (
            _record_data_availability, persist_review, register_active_trade,
        )
        register_active_trade({
            "id": 77, "symbol": "ETHUSDT", "strategy": "MTF",
            "direction": "BULLISH", "entry": 100, "sl": 95,
            "tp1": 110, "tp2": 115, "tp3": 120, "rr": 2,
        }, thesis={"source": "state-test"}, db_path=legacy_path)
        state = load_state(77, legacy_path)
        persist_review(
            state, 102, ["BOS"],
            {"management_candle_id": "closed-15m-77", "new_management_candle": True},
            {"action": "HOLD", "confidence": 0.8, "reason": "continue"},
            legacy_path,
        )
        assert confirm_manager_action(77, "HOLD", "INTERNAL_CONFIRMED", legacy_path)
        assert _record_data_availability(state, False, "Gate timeout", legacy_path) == (1, False)
    finally:
        configure_manager_state(None)

    assert not (tmp_path / "must-not-exist.db").exists()
    target = state_factory()
    assert target.execute(
        "SELECT COUNT(*) FROM manager_events WHERE signal_id=77 AND event_type='BOS'"
    ).fetchone()[0] == 1
    assert target.execute(
        "SELECT data_failure_count FROM manager_positions WHERE signal_id=77"
    ).fetchone()[0] == 1
    target.close()


def test_closed_trade_remains_in_manager_and_has_final_accounting(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    state = finalize_manager_trade(1, "tp1", 110, db_path=db_path)
    assert state["status"] == "CLOSED"
    assert state["realized_pct"] == 10
    assert state["realized_r"] == 2
    rows = fetch_manager_trades(db_path)
    assert rows[0]["close_result"] == "tp1"
    card = format_final_trade_card(state)
    assert "СДЕЛКА ЗАКРЫТА" in card
    assert "+10.00%" in card


def test_restart_reconciles_stale_manager_state_from_closed_signal(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE signals SET result='sl' WHERE id=1")
    assert reconcile_manager_states_from_signals(db_path) == 1
    state = load_state(1, db_path)
    assert state["status"] == "CLOSED"
    assert state["close_result"] == "sl"


def test_live_candle_close_waits_for_confirmed_binance_accounting(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""CREATE TABLE trade_executions(
            signal_id INTEGER,mode TEXT,status TEXT,quantity REAL,entry_order_id TEXT
        )""")
        conn.execute("INSERT INTO trade_executions VALUES(1,'live','PROTECTED',1,'entry-1')")
        conn.execute("UPDATE signals SET result='sl' WHERE id=1")
    assert reconcile_manager_states_from_signals(db_path) == 1
    state = load_state(1, db_path)
    assert state["status"] == "CLOSING"
    assert state["manager_state"] == "RECONCILIATION_REQUIRED"
    assert state["realized_r"] is None
    assert state["last_event"] == "AWAITING_CONFIRMED_BINANCE_CLOSE"


def test_live_candle_close_is_fenced_in_state_immediately(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""CREATE TABLE trade_executions(
            signal_id INTEGER,mode TEXT,status TEXT,quantity REAL,entry_order_id TEXT
        )""")
        conn.execute("INSERT INTO trade_executions VALUES(1,'live','PROTECTED',1,'entry-1')")
        conn.execute("UPDATE signals SET result='sl' WHERE id=1")
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    import_legacy_manager(lambda: sqlite3.connect(db_path), state_factory, refresh=True)
    SignalLifecycleRepository(state_factory).import_row({
        "signal_id": 1, "status": "closed", "result": "sl",
    })
    ExecutionRepository(state_factory).register({
        "signal_id": 1, "mode": "live", "symbol": "BTCUSDT",
        "direction": "BULLISH", "status": "PROTECTED",
        "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
        "quantity": 1, "entry_order_id": "entry-1", "stop_order_id": "stop-1",
    })
    # Restore ACTIVE to prove this reconciliation call performs the State fence.
    conn = state_factory()
    conn.execute(
        """UPDATE manager_positions SET status='ACTIVE',manager_state='PROTECTED',
                  last_event=NULL,reconciliation_reason=NULL WHERE signal_id=1"""
    )
    conn.commit(); conn.close()
    configure_manager_state(state_factory)
    try:
        assert reconcile_manager_states_from_signals(db_path) == 1
        state = load_state(1, db_path)
    finally:
        configure_manager_state(None)
    assert state["status"] == "CLOSING"
    assert state["manager_state"] == "RECONCILIATION_REQUIRED"
    assert state["realized_r"] is None
    assert state["reconciliation_reason"] == "SIGNAL_CLOSED_AWAITING_BINANCE"


def test_state_manager_rejects_direct_analytical_finalization(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    import_legacy_manager(lambda: sqlite3.connect(db_path), state_factory, refresh=True)
    ExecutionRepository(state_factory).register({
        "signal_id": 1, "mode": "live", "symbol": "BTCUSDT",
        "direction": "BULLISH", "status": "PROTECTED",
        "entry": 100, "sl": 95, "tp1": 110, "tp2": 115,
        "quantity": 1, "entry_order_id": "entry-1", "stop_order_id": "stop-1",
    })
    configure_manager_state(state_factory)
    try:
        result = finalize_manager_trade(1, "tp1", 110, db_path=db_path)
        state = load_state(1, db_path)
    finally:
        configure_manager_state(None)

    assert result is None
    assert state["status"] == "CLOSING"
    assert state["manager_state"] == "RECONCILIATION_REQUIRED"
    assert state["realized_r"] is None
    assert state["close_result"] is None
    with sqlite3.connect(db_path) as conn:
        legacy = conn.execute(
            "SELECT status,manager_state,realized_r FROM trade_manager_state WHERE signal_id=1"
        ).fetchone()
    assert legacy == ("ACTIVE", "PROTECTED", None)


def test_migrated_manager_without_live_execution_closes_without_pnl(tmp_path):
    db_path = _db(tmp_path)
    register_pending_signals(db_path)
    state_path = str(tmp_path / "apex_state.db")

    def state_factory():
        conn = sqlite3.connect(state_path)
        conn.row_factory = sqlite3.Row
        return conn

    conn = state_factory(); migrate_state(conn); conn.close()
    import_legacy_manager(lambda: sqlite3.connect(db_path), state_factory, refresh=True)
    SignalLifecycleRepository(state_factory).import_row({
        "signal_id": 1, "status": "active", "result": "pending",
    })
    configure_manager_state(state_factory)
    try:
        assert reconcile_manager_states_from_signals(db_path) == 1
        state = load_state(1, db_path)
    finally:
        configure_manager_state(None)
    assert state["status"] == "CLOSED"
    assert state["close_result"] == "NOT_OPENED:NO_CONFIRMED_LIVE_EXECUTION"
    assert state["exit_price"] is None
    assert state["realized_r"] is None
    conn = state_factory()
    event = conn.execute(
        "SELECT event_type,execution_status FROM manager_events WHERE signal_id=1"
    ).fetchone()
    conn.close()
    assert tuple(event) == ("EXECUTION_NOT_OPENED", "NO_CONFIRMED_LIVE_EXECUTION")


def test_live_execution_that_never_opened_is_not_managed_as_actual_position(tmp_path):
    db_path = _db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE trade_executions (
                   signal_id INTEGER PRIMARY KEY, mode TEXT, status TEXT,
                   quantity REAL, entry_order_id TEXT
               )"""
        )
        conn.execute(
            "INSERT INTO trade_executions VALUES (1,'live','SKIPPED_BELOW_MIN_NOTIONAL',0,'')"
        )

    # A known rejected live execution must never create a Manager ACTUAL row.
    assert register_pending_signals(db_path) == 0
    assert load_state(1, db_path) is None

    # Also repair a row created by an older build before execution was checked.
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM trade_executions")
    assert register_pending_signals(db_path) == 1
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO trade_executions VALUES (1,'live','SKIPPED_BELOW_MIN_NOTIONAL',0,'')"
        )
    assert reconcile_manager_states_from_signals(db_path) == 1
    state = load_state(1, db_path)
    assert state["status"] == "CLOSED"
    assert state["manager_state"] == "CLOSED"
    assert state["close_result"] == "NOT_OPENED:SKIPPED_BELOW_MIN_NOTIONAL"
    assert state["realized_r"] is None
