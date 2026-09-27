"""Smoke tests for canonical APEX V3 boundaries."""
from __future__ import annotations
import importlib
MODULES=("apex.app.shutdown","apex.config.constants","apex.domain.events","apex.db.integrity","apex.db.repositories.signals","apex.db.repositories.positions","apex.db.repositories.incidents","apex.db.repositories.learning","apex.market.liquidity","apex.market.external_context","apex.market.options","apex.market.onchain","apex.quality.groq_gate","apex.quality.groq_calibration","apex.risk.limits","apex.risk.sizing","apex.risk.portfolio","apex.risk.kill_switch","apex.execution.binance_client","apex.execution.execution_quality","apex.execution.protection","apex.execution.reconcile","apex.manager.events","apex.manager.playbooks","apex.manager.reconcile","apex.manager.structure","apex.learning.outcomes","apex.learning.execution_quality","apex.learning.groq_performance","apex.telemetry.metrics","apex.telemetry.health","apex.ops.graceful_shutdown","apex.ui.dashboard.auth","apex.ui.dashboard.api","apex.ui.dashboard.overview","apex.ui.dashboard.strategies","apex.ui.dashboard.trades","apex.ui.dashboard.manager","apex.ui.dashboard.execution","apex.ui.dashboard.market","apex.ui.dashboard.learning","apex.ui.dashboard.health","apex.ui.telegram.app","apex.ui.telegram.menu","apex.ui.telegram.scanners","apex.ui.telegram.strategies","apex.ui.telegram.trades","apex.ui.telegram.manager","apex.ui.telegram.execution","apex.ui.telegram.formatters")
def test_v3_boundary_modules_import_cleanly():
    for name in MODULES: assert importlib.import_module(name) is not None

def test_dashboard_has_exact_production_tabs():
    from apex.config.constants import DASHBOARD_TABS
    from apex.ui.dashboard.api import PROJECTORS
    assert tuple(PROJECTORS)==DASHBOARD_TABS

def test_risk_sizing_is_geometry_independent():
    from apex.risk.sizing import quantity_for_risk
    assert quantity_for_risk(equity_quote=1000,risk_pct=1,entry=100,stop=90)==1.0
