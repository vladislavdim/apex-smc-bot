"""Architecture contracts for canonical APEX V3 boundaries."""
from apex.quality.groq_gate import geometry
from apex.risk.sizing import quantity_for_risk
from apex.ui.dashboard.api import PROJECTORS
from apex.ui.telegram.app import COMMAND_ROUTES,TelegramHandlers,register_telegram_handlers


def test_dashboard_has_exact_production_tabs():
    assert tuple(PROJECTORS)==("overview","strategies","trades","manager","execution","market","learning","health")


def test_risk_sizing_is_geometry_independent():
    assert quantity_for_risk(equity_quote=1000, risk_pct=1, entry=100, stop=95)==2


def test_telegram_app_exports_real_router_contract():
    assert COMMAND_ROUTES
    assert TelegramHandlers
    assert callable(register_telegram_handlers)
