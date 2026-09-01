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
