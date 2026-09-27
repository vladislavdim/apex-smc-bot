"""Architecture smoke tests for canonical V3 boundaries."""
from apex.ui.telegram.app import register_handlers
from apex.quality.groq_gate import geometry
from apex.risk.sizing import quantity_for_risk
from apex.domain.models import Candidate

def test_telegram_boundary_is_wired():
    assert callable(register_handlers)

def test_risk_sizing_is_geometry_independent():
    assert quantity_for_risk(1000,1,100,90)==1.0

def test_groq_geometry_surface_exists():
    assert callable(geometry)
