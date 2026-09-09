import os
import sqlite3
import tempfile

from core.execution_ledger import actual_result, connect, register_execution_orders, register_order, save_fills
from core.replay_lab import FrozenEntry, replay_three_tracks
from research.store import ResearchStore


def test_core_replay_does_not_use_pre_entry_wicks_and_reports_positive_mae():
    snapshot = FrozenEntry(9, "BTCUSDT", "FAST", "BULLISH", 100, 90, 110, 120, 120, 2,
                           "2026-01-01T00:00:00+00:00")
    result = replay_three_tracks(snapshot, [
        {"id": "before", "open": 100, "high": 101, "low": 80, "close": 100,
         "closed_at": "2025-12-31T23:00:00+00:00"},
        {"id": "fill", "open": 99, "high": 103, "low": 96, "close": 100,
         "closed_at": "2026-01-01T00:15:00+00:00"},
    ], entry_expiry_bars=3)["NO_MANAGER"]
    assert result["status"] == "OPEN"
    assert result["entry_state"] == "FILLED"
    assert result["entry_time"].startswith("2026-01-01")
    assert result["mae_r"] == 0.4
    assert result["duration_bars"] == 1


def test_research_setup_identity_is_idempotent(tmp_path):
    store = ResearchStore(str(tmp_path / "history.db")); store.ensure_schema()
    common = {
        "research_run_id": "run", "profile_id": "profile", "parent_strategy": "FAST",
        "symbol": "AAVEUSDT", "direction": "BULLISH", "stage": "SCAN", "outcome": "FILTERED",
        "snapshot": {"candidate": {"timeframe": "15m", "technical_evidence": {"zone_id": "z1"}}},
    }
    store.save_attempts([{**common, "decision_time": 1}, {**common, "decision_time": 2}])
    store.save_attempts([{**common, "decision_time": 1}, {**common, "decision_time": 2}])
    with sqlite3.connect(store.database_url) as conn:
        assert conn.execute("SELECT COUNT(*) FROM research_setups").fetchone()[0] == 1
        assert conn.execute("SELECT checks_count FROM research_setups").fetchone()[0] == 2


def test_actual_excursion_is_context_only_and_fees_missing_never_become_zero():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "brain.db")
        snap = FrozenEntry(1, "AAVEUSDT", "MTF", "BULLISH", 100, 90, 110, 120, 120, 2)
        register_order(path, 1, "AAVEUSDT", "ENTRY", "10", "BUY")
        register_order(path, 1, "AAVEUSDT", "CLOSE", "11", "SELL")
        with connect(path) as conn:
            orders = {row["remote_id"]: dict(row) for row in conn.execute("SELECT * FROM confirmed_execution_orders")}
        save_fills(path, orders["10"], [{"symbol": "AAVEUSDT", "orderId": "10", "id": "1", "side": "BUY",
            "qty": "2", "price": "100", "commission": "0.1", "commissionAsset": "USDT", "time": 1000}])
        save_fills(path, orders["11"], [{"symbol": "AAVEUSDT", "orderId": "11", "id": "2", "side": "SELL",
            "qty": "2", "price": "110", "commission": "0.1", "commissionAsset": "BNB", "time": 2000}])
        result = actual_result(path, snap, [{"high": 120, "low": 95, "close_time": 1.5}])
        assert result["status"] == "FEES_UNRESOLVED"
        assert result["net_r"] is None
        assert result["mfe_r"] == 2.0 and result["mae_r"] == 0.5
        assert result["targets_reached"] is None
        assert result["gate_targets_touched"] == ["TP1", "TP2", "TP"]


def test_pending_execution_is_discovered_even_when_signal_pending(tmp_path):
    path = str(tmp_path / "brain.db")
    with connect(path) as conn:
        conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, result TEXT)")
        conn.execute("CREATE TABLE trade_executions (signal_id INTEGER, mode TEXT, entry_order_id TEXT, symbol TEXT, direction TEXT, quantity REAL, stop_order_id TEXT, tp1_order_id TEXT, tp2_order_id TEXT)")
        conn.execute("INSERT INTO signals VALUES (1, 'pending')")
        conn.execute("INSERT INTO trade_executions VALUES (1, 'live', '10', 'AAVEUSDT', 'BULLISH', 1, NULL, NULL, NULL)")
    register_execution_orders(path)
    with connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM confirmed_execution_orders WHERE remote_id='10'").fetchone()[0] == 1
