import json
import sqlite3

from core.trade_manager import (
    format_telegram_update,
    load_state,
    manager_cycle,
    register_pending_signals,
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
    assert len(calls) == 1
    state = load_state(1, db_path)
    assert (state["initial_entry"], state["initial_sl"], state["initial_tp1"]) == (100, 95, 110)


def test_manager_prompt_receives_compact_fresh_external_context(tmp_path):
    db_path = _db(tmp_path)
    prompts = []

    def groq(prompt, **_kwargs):
        prompts.append(prompt)
        return '{"action":"HOLD","confidence":0.8,"reason":"context checked"}'

    manager_cycle(
        lambda: {"BTCUSDT": {"price": 102}},
        lambda *_args: _candles(),
        groq,
        external_context=lambda *_args: {
            "open_interest": {"change_1h_pct": 2.1, "trend": "rising", "status": "fresh"},
            "liquidations": {"dominance": "short", "status": "fresh"},
            "external_bias": "bullish", "external_confidence": .71,
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
