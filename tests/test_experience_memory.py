import sqlite3
import time

from core.experience_memory import (
    apply_candles_to_candidate,
    capture_candidate,
    ensure_experience_schema,
    evaluate_rule_lifecycle,
    experience_dashboard,
    record_decision,
    refresh_shadow_positions,
    wilson_lower,
    bind_candidate_to_signal,
    record_management_review,
    resolve_management_reviews,
)
from core.telegram_dashboard import format_experience_dashboard


def candidate(**overrides):
    base = {
        "symbol": "BTCUSDT", "strategy": "MTF", "grade": "MTF",
        "direction": "BULLISH", "timeframe": "1h", "regime": "TREND",
        "entry": 100.0, "sl": 95.0, "tp1": 110.0, "tp2": 115.0,
        "tp3": 120.0, "rr": 2.0, "detected_at": "2026-09-01 00:00:00",
        "technical_evidence": {"bos": True},
    }
    base.update(overrides)
    return base


def test_capture_is_idempotent_and_survives_restart(tmp_path):
    path = str(tmp_path / "brain.db")
    first = capture_candidate(candidate(), path)
    second = capture_candidate(candidate(), path)
    assert first == second
    conn = sqlite3.connect(path)
    assert conn.execute("SELECT COUNT(*) FROM experience_candidates").fetchone()[0] == 1
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_old_experience_schema_adds_signal_link_before_index(tmp_path):
    path = str(tmp_path / "old.db")
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE experience_candidates (
           id INTEGER PRIMARY KEY,status TEXT,detected_at TEXT,strategy TEXT,
           regime TEXT,direction TEXT,outcome TEXT)"""
    )
    conn.commit(); conn.close()
    ensure_experience_schema(path)
    ensure_experience_schema(path)
    conn = sqlite3.connect(path)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(experience_candidates)")}
    indexes = {row[1] for row in conn.execute("PRAGMA index_list(experience_candidates)")}
    assert "signal_id" in columns
    assert "idx_experience_signal" in indexes


def test_wait_and_reject_are_persisted_with_context(tmp_path):
    path = str(tmp_path / "brain.db")
    item = candidate()
    record_decision(item, "WAIT", "funding conflict", {
        "confidence": 0.61, "context": {"data_quality": {"available_sources": ["gate"]}},
    }, path)
    conn = sqlite3.connect(path)
    row = conn.execute(
        "SELECT decision,decision_reason,groq_confidence,external_json FROM experience_candidates"
    ).fetchone()
    assert row[:3] == ("WAIT", "funding conflict", 0.61)
    assert "gate" in row[3]


def test_ambiguous_bar_resolves_stop_first_without_changing_levels():
    row = {**candidate(), "status": "WAITING_ENTRY", "last_candle_ts": None,
           "mfe_price": 0, "mae_price": 0}
    updated = apply_candles_to_candidate(row, [{
        "time": 1788224400, "open": 100, "high": 111, "low": 94, "close": 108,
    }])
    assert updated["status"] == "CLOSED"
    assert updated["outcome"] == "SL"
    assert (updated["entry"], updated["sl"], updated["tp1"]) == (100.0, 95.0, 110.0)


def test_mfe_mae_and_tuple_candle_for_short():
    row = {**candidate(direction="BEARISH", entry=100, sl=105, tp1=90),
           "status": "WAITING_ENTRY", "last_candle_ts": None, "mfe_price": 0, "mae_price": 0}
    updated = apply_candles_to_candidate(row, [
        [1788224400000, 100, 103, 96, 98],
        [1788228000000, 98, 101, 94, 95],
    ])
    assert updated["status"] == "ACTIVE"
    assert updated["mfe_price"] == 6
    assert updated["mae_price"] == 3
    assert updated["mfe_r"] == 1.2
    assert updated["mae_r"] == 0.6


def test_refresh_expires_unfilled_candidate_without_exchange(tmp_path):
    path = str(tmp_path / "brain.db")
    item = candidate(estimated_hours=1)
    capture_candidate(item, path)
    calls = []
    result = refresh_shadow_positions(
        lambda *args: calls.append(args) or [], path, now_ts=time.time() + 86400,
    )
    assert result["expired"] == 1
    assert calls == []


def _insert_outcomes(path, strategy, regime, direction, wins, losses, start=0):
    conn = sqlite3.connect(path)
    for offset, outcome in enumerate(["TP1"] * wins + ["SL"] * losses):
        ident = start + offset
        conn.execute(
            """INSERT INTO experience_candidates
               (fingerprint,symbol,strategy,timeframe,direction,regime,entry,sl,tp1,status,outcome,closed_at)
               VALUES (?,?,?,?,?,?,?,?,?,'CLOSED',?,CURRENT_TIMESTAMP)""",
            (f"{strategy}-{regime}-{direction}-{ident}", "BTCUSDT", strategy, "1h", direction,
             regime, 100, 95, 110, outcome),
        )
    conn.commit(); conn.close()


def test_rule_requires_discovery_then_out_of_sample_probation(tmp_path):
    path = str(tmp_path / "brain.db")
    ensure_experience_schema(path)
    # TREND is 20/30 while RANGE lowers the strategy baseline to 50%.
    _insert_outcomes(path, "MTF", "TREND", "BULLISH", 20, 10)
    _insert_outcomes(path, "MTF", "ZZZ_RANGE", "BULLISH", 10, 20, 100)
    evaluate_rule_lifecycle(path)
    conn = sqlite3.connect(path)
    state = conn.execute(
        "SELECT state,rule_kind,probation_start_candidate_id FROM experience_rules "
        "WHERE strategy='MTF' AND regime='TREND'"
    ).fetchone()
    assert state[0:2] == ("PROBATION", "CONFIRM")
    start = state[2]
    conn.close()

    _insert_outcomes(path, "MTF", "TREND", "BULLISH", 20, 0, 1000)
    evaluate_rule_lifecycle(path)
    conn = sqlite3.connect(path)
    row = conn.execute(
        "SELECT state,probation_samples FROM experience_rules WHERE strategy='MTF' AND regime='TREND'"
    ).fetchone()
    assert row == ("ACTIVE", 20)
    assert start is not None


def test_active_rule_rolls_back_after_objective_contradictions(tmp_path):
    path = str(tmp_path / "brain.db")
    ensure_experience_schema(path)
    _insert_outcomes(path, "ZONE", "TREND", "BULLISH", 20, 10)
    _insert_outcomes(path, "ZONE", "ZZZ_RANGE", "BULLISH", 10, 20, 100)
    evaluate_rule_lifecycle(path)
    _insert_outcomes(path, "ZONE", "TREND", "BULLISH", 20, 0, 1000)
    evaluate_rule_lifecycle(path)
    _insert_outcomes(path, "ZONE", "TREND", "BULLISH", 0, 20, 2000)
    evaluate_rule_lifecycle(path)
    conn = sqlite3.connect(path)
    assert conn.execute(
        "SELECT state FROM experience_rules WHERE strategy='ZONE' AND regime='TREND'"
    ).fetchone()[0] == "ROLLED_BACK"
    assert conn.execute("SELECT COUNT(*) FROM experience_rule_audit").fetchone()[0] >= 3


def test_wilson_and_dashboard_have_no_control_buttons(tmp_path):
    assert 0 < wilson_lower(20, 30) < 20 / 30
    path = str(tmp_path / "brain.db")
    ensure_experience_schema(path)
    capture_candidate(candidate(), path)
    text = format_experience_dashboard(experience_dashboard(path))
    assert "Experience / Shadow" in text
    assert "Активировать" not in text
    assert "Отклонить" not in text


def test_management_decisions_are_linked_and_resolved_objectively(tmp_path):
    path = str(tmp_path / "brain.db")
    ensure_experience_schema(path)
    item = candidate()
    capture_candidate(item, path)
    bind_candidate_to_signal(item, 1, path)
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE signals (id INTEGER PRIMARY KEY,result TEXT,direction TEXT,
           entry REAL,sl REAL,tp1 REAL,tp2 REAL,tp3 REAL)"""
    )
    conn.execute("INSERT INTO signals VALUES (1,'pending','BULLISH',100,95,110,115,120)")
    conn.commit(); conn.close()
    state = {"signal_id": 1, "strategy": "MTF", "direction": "BULLISH", "current_r": .4}
    facts = {"management_candle_id": "123"}
    review = {"action": "LET_RUN", "reason": "continuation", "protect_level": None,
              "management_target": None}
    record_management_review(state, 102, ["BOS"], facts, review, path)
    conn = sqlite3.connect(path)
    conn.execute("UPDATE signals SET result='tp1' WHERE id=1")
    conn.commit(); conn.close()
    assert resolve_management_reviews(path) == 1
    conn = sqlite3.connect(path)
    row = conn.execute(
        "SELECT status,effect,final_r FROM experience_management_reviews"
    ).fetchone()
    assert row[0:2] == ("RESOLVED", "HELPED")
    assert row[2] == 2.0
    text = format_experience_dashboard(experience_dashboard(path))
    assert "LET_RUN" in text
    assert "помогло" in text
