"""Static guardrails for the final APEX V3 module boundaries."""
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
REQUIRED=(
"apex/config/constants.py","apex/domain/events.py","apex/db/integrity.py",
"apex/risk/sizing.py","apex/risk/limits.py","apex/risk/portfolio.py","apex/risk/kill_switch.py",
"apex/execution/binance_client.py","apex/execution/execution_quality.py","apex/execution/protection.py","apex/execution/reconcile.py",
"apex/manager/events.py","apex/manager/playbooks.py","apex/manager/structure.py","apex/manager/reconcile.py",
"apex/learning/outcomes.py","apex/learning/execution_quality.py","apex/learning/groq_performance.py",
"apex/telemetry/metrics.py","apex/telemetry/health.py","apex/ops/graceful_shutdown.py","apex/app/shutdown.py",
"apex/ui/dashboard/api.py","apex/ui/dashboard/auth.py","apex/ui/dashboard/overview.py","apex/ui/dashboard/strategies.py",
"apex/ui/dashboard/trades.py","apex/ui/dashboard/manager.py","apex/ui/dashboard/execution.py","apex/ui/dashboard/market.py",
"apex/ui/dashboard/learning.py","apex/ui/dashboard/health.py","scripts/verify_release.py","scripts/migrate_v3.py","scripts/inspect_state.py")

def test_required_v3_surfaces_exist():
    missing=[p for p in REQUIRED if not (ROOT/p).is_file()]
    assert not missing,missing

def test_launchers_stay_thin():
    for name in ("bot.py","stats_server.py"):
        lines=[x for x in (ROOT/name).read_text().splitlines() if x.strip() and not x.lstrip().startswith("#")]
        assert len(lines)<30,(name,len(lines))
