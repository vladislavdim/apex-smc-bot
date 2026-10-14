import sqlite3
from apex.db.integrity import check_integrity
from apex.risk.sizing import quantity_for_risk
from apex.risk.kill_switch import KillSwitch
from apex.execution.execution_quality import slippage_bps

def test_quantity_for_risk():
    assert quantity_for_risk(1000,1,100,95)==2

def test_integrity_memory_db():
    c=sqlite3.connect(":memory:");check_integrity(c);c.close()

def test_kill_switch_is_fail_closed_when_enabled():
    assert KillSwitch(True,"test").entries_allowed is False

def test_slippage_bps():
    assert round(slippage_bps(100,101),6)==100
