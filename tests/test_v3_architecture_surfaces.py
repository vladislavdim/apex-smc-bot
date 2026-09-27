from apex.config.constants import DASHBOARD_TABS,PRODUCTION_STRATEGIES
from apex.domain.models import TradeOutcome
from apex.risk.sizing import quantity_for_risk
from apex.ui.dashboard.api import PROJECTORS

def test_v3_production_surface_is_exact():
    assert PRODUCTION_STRATEGIES==("FAST","MTF","ZONE","SWING","WYCKOFF")
    assert tuple(PROJECTORS)==DASHBOARD_TABS

def test_risk_sizing_is_geometry_derived(): assert quantity_for_risk(1000,1,100,95)==2

def test_trade_outcome_has_position_identity(): assert "position_id" in TradeOutcome.__dataclass_fields__
