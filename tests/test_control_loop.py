import sqlite3
from datetime import datetime, timedelta, timezone

from core.control_loop import (
    begin_scan,
    ensure_control_schema,
    finish_scan,
    rebuild_strategy_risk_states,
    scan_heartbeat,
    scanner_dashboard,
    take_persistent_batch,
)
from core.telegram_dashboard import format_scanner_dashboard


def _signals_schema(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE signals (
          id INTEGER PRIMARY KEY, grade TEXT, signal_type TEXT, result TEXT,
          created_at TEXT, closed_at TEXT
        );
        CREATE TABLE signal_execution_state (signal_id INTEGER, status TEXT);
        """
    )
    conn.commit()
    conn.close()


def test_persistent_batch_cursor_survives_calls(tmp_path):
    path = str(tmp_path / "brain.db")
    assert take_persistent_batch("swing", [1, 2, 3, 4], 2, path) == [1, 2]
    assert take_persistent_batch("swing", [1, 2, 3, 4], 2, path) == [3, 4]


def test_strategy_risk_is_separate_and_uses_only_activated_outcomes(tmp_path):
    path = str(tmp_path / "brain.db")
    _signals_schema(path)
    recent = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(path)
    for signal_id in range(1, 6):
        conn.execute(
            "INSERT INTO signals VALUES (?,?,?,?,?,?)",
            (signal_id, "FAST", "", "sl", recent, recent),
        )
        conn.execute("INSERT INTO signal_execution_state VALUES (?,?)", (signal_id, "closed"))
    conn.execute("INSERT INTO signals VALUES (?,?,?,?,?,?)", (10, "SWING", "", "tp1", recent, recent))
    conn.execute("INSERT INTO signal_execution_state VALUES (?,?)", (10, "waiting_entry"))
    conn.commit(); conn.close()

    states = rebuild_strategy_risk_states(path)
    assert states["FAST"]["mode"] == "PAUSED"
    assert states["FAST"]["live_risk_multiplier"] == 0.0
    assert states["SWING"]["mode"] == "NORMAL"
    assert states["SWING"]["consecutive_wins"] == 0


def test_strategy_pause_expires_without_being_extended(tmp_path):
    path = str(tmp_path / "brain.db")
    _signals_schema(path)
    old = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(path)
    for signal_id in range(1, 6):
        conn.execute("INSERT INTO signals VALUES (?,?,?,?,?,?)", (signal_id, "ZONE", "", "sl", old, old))
        conn.execute("INSERT INTO signal_execution_state VALUES (?,?)", (signal_id, "closed"))
    conn.commit(); conn.close()
    state = rebuild_strategy_risk_states(path)["ZONE"]
    assert state["mode"] == "NORMAL"
    assert state["live_paused_until"] is None


def test_dashboard_reports_real_run(tmp_path):
    path = str(tmp_path / "brain.db")
    ensure_control_schema(path)
    run_id = begin_scan("MTF", "auto_scan_1h", 120, 40, path)
    for index in range(40):
        scan_heartbeat(run_id, f"PAIR{index}", path)
    finish_scan(run_id, "COMPLETED", db_path=path)
    data = scanner_dashboard(path)
    mtf = next(row for row in data["runs"] if row["strategy"] == "MTF")
    assert mtf["status"] == "COMPLETED"
    assert "Сканеры и контроль" in format_scanner_dashboard(data)
