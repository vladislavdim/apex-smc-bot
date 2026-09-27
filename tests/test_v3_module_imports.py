"""Smoke-import canonical V3 module surfaces before production wiring changes."""
import importlib
MODULES=(
"apex.app.shutdown","apex.config.constants","apex.domain.events","apex.db.integrity",
"apex.execution.binance_client","apex.execution.execution_quality","apex.execution.reconcile",
"apex.learning.execution_quality","apex.learning.groq_performance","apex.learning.outcomes",
"apex.manager.events","apex.manager.playbooks","apex.manager.reconcile","apex.manager.structure",
"apex.market.external_context","apex.market.gate_ws","apex.market.liquidity","apex.market.onchain","apex.market.options",
"apex.ops.graceful_shutdown","apex.quality.groq_calibration","apex.quality.groq_gate",
"apex.risk.kill_switch","apex.risk.limits","apex.risk.portfolio","apex.risk.sizing",
"apex.telemetry.health","apex.telemetry.metrics","apex.ui.dashboard.api","apex.ui.telegram.app",
)
def test_canonical_modules_import():
    for name in MODULES: importlib.import_module(name)
