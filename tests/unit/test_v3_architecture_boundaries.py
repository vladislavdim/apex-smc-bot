from datetime import datetime,timezone
import sqlite3
import pytest
from apex.db.integrity import check_integrity
from apex.execution.execution_quality import slippage_bps
from apex.learning.outcomes import require_real_outcome
from apex.risk.kill_switch import KillSwitch
from apex.risk.sizing import quantity_for_risk
from apex.domain.models import TradeOutcome
from apex.domain.ids import new_id

def test_integrity_accepts_clean_sqlite():
    conn=sqlite3.connect(":memory:");check_integrity(conn);conn.close()

def test_sizing_is_geometry_based_and_pure():
    assert quantity_for_risk(1000,1,100,95)==pytest.approx(2.0)

def test_kill_switch_fail_closed():
    assert not KillSwitch(enabled=True,reason="test").entries_allowed

def test_execution_quality_slippage():
    assert slippage_bps(100,101)==pytest.approx(100.0)

def test_learning_requires_real_correlated_outcome():
    outcome=TradeOutcome(new_id("outcome"),new_id("position"),100,101,1,0,0,datetime.now(timezone.utc))
    assert require_real_outcome(outcome) is outcome
