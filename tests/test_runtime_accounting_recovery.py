import io
import math
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.error import HTTPError
from unittest.mock import MagicMock, patch

import pytest

from apex.ops.instance_fencing import LeaseError, _post_json
from apex.telemetry.execution_accounting import confirmed_execution_events
from apex.ui.dashboard import server


def event(kind, action, basis=None, **values):
    return {"event_key": f"{kind}:{action}", "kind": kind, "strategy": "ZONE",
            "symbol": "PIEVERSEUSDT", "occurred_at": "2026-10-08T04:25:54+00:00",
            "payload": {"signal_id": 469, "action": action, "accounting_basis": basis, **values}}


def test_analytical_sl_cannot_be_a_live_loss():
    with patch.object(server, "_fetch", return_value=[event("trade_event", "CLOSE", result="sl", realized_r=-1, pnl_pct=-1.42)]):
        data = server._build_dashboard_uncached()
    assert data["trade_stats"]["closed"] == data["trade_stats"]["losses"] == 0
    assert data["signal_analytics"]["closed"] == 1


def test_complete_financial_costs_and_idempotence():
    # A gross TP with negative net after costs is a financial loss.
    close = event("execution_accounting", "CLOSE", "confirmed_fills_after_commissions_and_funding", result="tp1", realized_r=-.02, pnl_pct=-.03)
    events = [close, {**close, "event_key": "retry"}, event("execution_accounting", "CLOSE", "CANDLE_BARRIERS", realized_r=9)]
    with patch.object(server, "_fetch", return_value=events):
        data = server._build_dashboard_uncached()
    assert data["trade_stats"]["closed"] == data["trade_stats"]["losses"] == 1
    assert data["trade_stats"]["wins"] == 0


def test_funding_pending_is_not_final_net_pnl():
    snapshot = SimpleNamespace(signal_id=468, symbol="ZROUSDT", strategy="ZONE", direction="BEARISH", entry=100)
    accounting = {"entry_state": "FILLED", "entry_time": 1791418000000, "quantity": 1,
                  "exit_time": 1791419000000, "status": "CLOSED", "net_r": 1,
                  "accounting_basis": "confirmed_fills_after_commissions_funding_pending"}
    assert [e["action"] for e in confirmed_execution_events(snapshot, accounting)] == ["OPEN"]
    accounting.update(accounting_basis="confirmed_fills_after_commissions_and_funding", realized_pct=.9, funding_quote=-.1, entry=100)
    events = confirmed_execution_events(snapshot, accounting)
    assert events[-1]["pnl_pct"] == pytest.approx(.9)
    accounting["entry_state"] = "UNFILLED"
    assert confirmed_execution_events(snapshot, accounting) == []


def test_readiness_database_error_is_a_controlled_503():
    with patch.object(server, "_connect", side_effect=RuntimeError("private DB connection details")):
        payload, status = server.worker_readiness("a" * 40)
    assert status == 503 and payload["ready"] is False
    assert "private" not in str(payload)


def test_readiness_sql_timeout_closes_connection():
    conn = MagicMock(); cur = conn.cursor.return_value.__enter__.return_value
    cur.execute.side_effect = TimeoutError()
    with patch.object(server, "_connect", return_value=conn):
        assert server.worker_readiness("a" * 40)[1] == 503
    conn.close.assert_called_once()


def test_lease_backend_failure_is_not_a_held_lease():
    error = HTTPError("https://example.invalid/runtime/lease", 503, "unavailable", {}, io.BytesIO(b'{"error":"database unavailable"}'))
    with patch("apex.ops.instance_fencing.urlopen", side_effect=error):
        with pytest.raises(LeaseError, match="runtime_lease_http_503"):
            _post_json("https://example.invalid/runtime/lease", {}, {}, 5)


def test_database_requests_are_bounded_below_client_timeout():
    with patch.object(server, "DATABASE_URL", "postgresql://example.invalid/apex"), patch.object(server.psycopg2, "connect") as connect:
        server._connect()
    assert connect.call_args.kwargs["connect_timeout"] == 2
    assert "statement_timeout=2000" in connect.call_args.kwargs["options"]
