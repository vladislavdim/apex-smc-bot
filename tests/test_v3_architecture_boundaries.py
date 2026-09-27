"""Architecture contracts for canonical APEX V3 boundaries."""
import importlib
import pytest
from apex.quality.groq_gate import geometry
from apex.risk.sizing import quantity_for_risk
from apex.ui.dashboard.api import PROJECTORS
from apex.ui.telegram.app import COMMAND_ROUTES,TelegramHandlers,register_telegram_handlers

BOUNDARIES=(
 "apex.app.shutdown","apex.config.constants","apex.domain.events","apex.db.integrity",
 "apex.market.liquidity","apex.market.external_context","apex.market.options","apex.market.onchain",
 "apex.quality.groq_gate","apex.quality.groq_calibration","apex.risk.portfolio","apex.risk.kill_switch",
 "apex.execution.binance_client","apex.execution.execution_quality","apex.execution.reconcile",
 "apex.manager.events","apex.manager.playbooks","apex.manager.reconcile","apex.manager.structure",
 "apex.learning.outcomes","apex.learning.execution_quality","apex.learning.groq_performance",
 "apex.telemetry.metrics","apex.telemetry.health","apex.ops.graceful_shutdown",
)

@pytest.mark.parametrize("module",BOUNDARIES)
def test_v3_boundary_imports(module): assert importlib.import_module(module)

def test_dashboard_has_exact_production_tabs():
    assert tuple(PROJECTORS)==("overview","strategies","trades","manager","execution","market","learning","health")

def test_risk_sizing_is_geometry_independent():
    assert quantity_for_risk(equity_quote=1000,risk_pct=1,entry=100,stop=95)==2

def test_telegram_app_exports_real_router_contract():
    assert COMMAND_ROUTES and TelegramHandlers and callable(register_telegram_handlers)

def test_kill_switch_fails_closed():
    from apex.risk.kill_switch import KillSwitch
    assert KillSwitch(True,"test").entries_allowed is False
