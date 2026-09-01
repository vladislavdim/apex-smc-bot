import sqlite3

from core.control_loop import (
    begin_scan,
    due_ltf_watches,
    finish_scan,
    record_scan_event,
    scan_heartbeat,
    scanner_dashboard,
    set_scan_round,
    set_scan_scope,
    take_strategy_round_batch,
    touch_ltf_watch,
    upsert_ltf_watch,
)
from core.telegram_dashboard import format_scanner_dashboard


def _finish_batch(path, strategy, scanner, batch):
    run_id = begin_scan(strategy, scanner, batch["target"], len(batch["pairs"]), path)
    set_scan_round(run_id, batch["round_id"], path)
    set_scan_scope(run_id, batch["target"], len(batch["pairs"]), path)
    for symbol in batch["pairs"]:
        scan_heartbeat(run_id, symbol, path)
        record_scan_event(run_id, strategy, symbol, "DETECTOR", "FILTERED", "TEST", {}, path)
    finish_scan(run_id, "COMPLETED", db_path=path)


def test_complete_round_uses_restart_safe_non_overlapping_batches(tmp_path):
    path = str(tmp_path / "brain.db")
    universe = [f"PAIR{i}" for i in range(80)]
    first = take_strategy_round_batch("MTF", universe, 40, path)
    _finish_batch(path, "MTF", "auto_scan_1h", first)
    second = take_strategy_round_batch("MTF", universe, 40, path)

    assert first["round_id"] == second["round_id"]
    assert not set(first["pairs"]) & set(second["pairs"])
    assert set(first["pairs"] + second["pairs"]) == set(universe)

    _finish_batch(path, "MTF", "auto_scan_1h", second)
    dashboard = scanner_dashboard(path)
    mtf = next(row for row in dashboard["runs"] if row["strategy"] == "MTF")
    assert mtf["round_status"] == "COMPLETED"
    assert mtf["round_covered_size"] == 80
    assert mtf["round_universe_size"] == 80


def test_incomplete_run_is_partial_and_remaining_pairs_resume(tmp_path):
    path = str(tmp_path / "brain.db")
    universe = [f"PAIR{i}" for i in range(8)]
    batch = take_strategy_round_batch("ZONE", universe, 4, path)
    run_id = begin_scan("ZONE", "auto_zone_scan", 8, 4, path)
    set_scan_round(run_id, batch["round_id"], path)
    set_scan_scope(run_id, 8, 4, path)
    for symbol in batch["pairs"][:2]:
        scan_heartbeat(run_id, symbol, path)
        record_scan_event(run_id, "ZONE", symbol, "DETECTOR", "FILTERED", "TEST", {}, path)
    finish_scan(run_id, "COMPLETED", db_path=path)

    with sqlite3.connect(path) as conn:
        status = conn.execute("SELECT status FROM scan_runs WHERE id=?", (run_id,)).fetchone()[0]
    assert status == "PARTIAL"

    resumed = take_strategy_round_batch("ZONE", universe, 4, path)
    assert resumed["round_id"] == batch["round_id"]
    assert set(batch["pairs"][:2]).isdisjoint(resumed["pairs"])


def test_process_restart_recovers_in_progress_pair(tmp_path):
    path = str(tmp_path / "brain.db")
    universe = ["BTCUSDT", "ETHUSDT"]
    batch = take_strategy_round_batch("MTF", universe, 1, path)
    run_id = begin_scan("MTF", "auto_scan_1h", 2, 1, path)
    set_scan_round(run_id, batch["round_id"], path)
    scan_heartbeat(run_id, "BTCUSDT", path)

    # Simulate a hard process stop: there is deliberately no finish_scan call.
    recovered = take_strategy_round_batch("MTF", universe, 1, path)
    assert recovered["round_id"] == batch["round_id"]
    assert recovered["pairs"] == ["ETHUSDT"]

    next_recovery = take_strategy_round_batch("MTF", universe, 2, path)
    assert "BTCUSDT" in next_recovery["pairs"]


def test_data_failure_is_retried_before_round_completes(tmp_path):
    path = str(tmp_path / "brain.db")
    universe = ["BTCUSDT", "ETHUSDT"]
    batch = take_strategy_round_batch("SWING", universe, 2, path)
    run_id = begin_scan("SWING", "auto_scan_swing", 2, 2, path)
    set_scan_round(run_id, batch["round_id"], path)
    set_scan_scope(run_id, 2, 2, path)
    for symbol in universe:
        scan_heartbeat(run_id, symbol, path)
    record_scan_event(run_id, "SWING", "BTCUSDT", "DETECTOR", "FILTERED", "NO_SETUP", {}, path)
    record_scan_event(run_id, "SWING", "ETHUSDT", "DETECTOR", "DATA_FAILED", "NO_CANDLES", {}, path)
    finish_scan(run_id, "COMPLETED", db_path=path)

    retry = take_strategy_round_batch("SWING", universe, 2, path)
    assert retry["pairs"] == ["ETHUSDT"]
    assert retry["covered"] == 1
    assert retry["retry"] == 1


def test_ltf_watch_survives_calls_and_is_visible_in_radar(tmp_path):
    path = str(tmp_path / "brain.db")
    upsert_ltf_watch("MTF", "BTCUSDT", "BULLISH", "15m", "wait BOS", 4, path)
    rows = due_ltf_watches(12, path)
    assert [(row["strategy"], row["symbol"], row["required_timeframe"]) for row in rows] == [
        ("MTF", "BTCUSDT", "15m")
    ]
    touch_ltf_watch("MTF", "BTCUSDT", "still waiting", False, path)
    dashboard = scanner_dashboard(path)
    assert dashboard["watches"][0]["attempts"] == 1
    text = format_scanner_dashboard(dashboard)
    assert "Младшие ТФ" in text
    assert "BTCUSDT" in text
    touch_ltf_watch("MTF", "BTCUSDT", "confirmed", True, path)
    assert due_ltf_watches(12, path) == []


def test_default_strategy_functions_keep_passive_mode_disabled():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    bot_source = (root / "bot.py").read_text(encoding="utf-8")
    market_source = (root / "market.py").read_text(encoding="utf-8")
    assert 'def full_scan_raw(symbol, timeframe="1h", auto=False, passive_watch=False):' in bot_source
    assert 'def detect_zone_setup(symbol: str, timeframe: str = "4h", passive_watch: bool = False)' in market_source
    assert 'if passive_watch and not _mtf_score_bos:' in bot_source
    assert 'if passive_watch and not _zone_ltf_structure:' in market_source
