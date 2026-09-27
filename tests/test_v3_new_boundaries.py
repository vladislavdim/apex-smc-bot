from apex.db.integrity import check_integrity
from apex.risk.sizing import quantity_for_risk
from apex.risk.kill_switch import KillSwitch
from apex.ui.dashboard.auth import authorized
from apex.quality.groq_calibration import approval_rate
import sqlite3

def test_new_boundaries_are_deterministic():
    assert quantity_for_risk(1000,1,100,95)==2
    assert KillSwitch().entries_allowed
    assert authorized("x","x") and not authorized("x","y")
    assert approval_rate([{"decision":"APPROVE"},{"decision":"REJECT"}])==0.5
    conn=sqlite3.connect(":memory:");check_integrity(conn);conn.close()
