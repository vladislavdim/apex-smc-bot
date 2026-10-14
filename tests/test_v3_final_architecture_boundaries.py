"""Architecture boundary tests for the final APEX V3 cutover branch."""
from __future__ import annotations
import sqlite3
from datetime import datetime,timezone
import pytest
from apex.db.integrity import check_integrity
from apex.execution.protection import StopProtectionRequest
from apex.quality.groq_gate import geometry
from apex.risk.sizing import quantity_for_risk
from apex.ui.telegram.menu import MENU



def test_db_integrity_accepts_clean_sqlite():
    conn=sqlite3.connect(":memory:")
    try: check_integrity(conn)
    finally: conn.close()

def test_risk_sizing_is_geometry_based_and_pure():
    assert quantity_for_risk(equity_quote=1000,risk_pct=0.5,entry=100,stop=95)==1.0

def test_execution_protection_validates_request():
    request=StopProtectionRequest("position_1","BTCUSDT",90.0,0.1)
    assert request.stop_price==90.0
    with pytest.raises(ValueError): StopProtectionRequest("","BTCUSDT",90.0,0.1)

def test_telegram_has_only_production_sections():
    lowered={x.lower() for x in MENU}
    assert not (lowered & {"shadow","research","replay","backtest"})
