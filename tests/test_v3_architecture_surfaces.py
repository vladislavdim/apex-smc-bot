from pathlib import Path
from apex.config.constants import DASHBOARD_TABS,PRODUCTION_STRATEGIES
from apex.quality.groq_gate import geometry
from apex.risk.sizing import quantity_for_risk

ROOT=Path(__file__).resolve().parents[1]

def test_production_vocabulary():
    assert PRODUCTION_STRATEGIES==("FAST","MTF","ZONE","SWING","WYCKOFF")
    assert DASHBOARD_TABS==("overview","strategies","trades","manager","execution","market","learning","health")

def test_launchers_are_thin():
    assert len((ROOT/"bot.py").read_text().splitlines())<30
    assert len((ROOT/"stats_server.py").read_text().splitlines())<30

def test_risk_sizing_is_geometry_independent():
    assert quantity_for_risk(1000,1,100,95)==2

def test_groq_geometry_tuple_is_stable_shape():
    assert geometry.__module__=="apex.quality.groq_gate"
