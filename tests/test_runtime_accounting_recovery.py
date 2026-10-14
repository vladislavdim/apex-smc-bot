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


def event(kind, action, basis=None, **values):
    return {"event_key": f"{kind}:{action}", "kind": kind, "strategy": "ZONE",
            "symbol": "PIEVERSEUSDT", "occurred_at": "2026-10-08T04:25:54+00:00",
            "payload": {"signal_id": 469, "action": action, "accounting_basis": basis, **values}}






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






def test_lease_backend_failure_is_not_a_held_lease():
    error = HTTPError("https://example.invalid/runtime/lease", 503, "unavailable", {}, io.BytesIO(b'{"error":"database unavailable"}'))
    with patch("apex.ops.instance_fencing.urlopen", side_effect=error):
        with pytest.raises(LeaseError, match="runtime_lease_http_503"):
            _post_json("https://example.invalid/runtime/lease", {}, {}, 5)
