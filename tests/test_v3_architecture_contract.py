from pathlib import Path
import ast


def _imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            yield node.module


def test_launchers_are_thin():
    assert len(Path("bot.py").read_text().splitlines()) < 30
    assert len(Path("stats_server.py").read_text().splitlines()) < 40


def test_required_v3_surfaces_exist():
    required = [
        "apex/app/shutdown.py", "apex/config/constants.py", "apex/domain/events.py",
        "apex/db/integrity.py", "apex/market/gate_ws.py", "apex/market/liquidity.py",
        "apex/market/external_context.py", "apex/market/options.py", "apex/market/onchain.py",
        "apex/risk/limits.py", "apex/risk/sizing.py", "apex/risk/portfolio.py", "apex/risk/kill_switch.py",
        "apex/execution/binance_client.py", "apex/execution/reconcile.py", "apex/execution/protection.py",
        "apex/execution/execution_quality.py", "apex/manager/playbooks.py", "apex/manager/events.py",
        "apex/manager/structure.py", "apex/manager/reconcile.py", "apex/learning/outcomes.py",
        "apex/learning/execution_quality.py", "apex/learning/groq_performance.py", "apex/telemetry/metrics.py",
        "apex/telemetry/health.py", "apex/ops/graceful_shutdown.py", "apex/quality/groq_gate.py",
        "apex/quality/groq_calibration.py", "apex/ui/dashboard/api.py", "apex/ui/dashboard/auth.py",
        "apex/ui/telegram/app.py", "apex/ui/telegram/menu.py", "apex/ui/telegram/formatters.py",
        "scripts/verify_release.py", "scripts/inspect_state.py",
    ]
    assert not [p for p in required if not Path(p).is_file()]


def test_execution_boundary_has_no_manager_or_ui_dependency():
    bad = []
    for p in Path("apex/execution").glob("*.py"):
        imports = tuple(_imports(p))
        if any(name == "apex.manager" or name.startswith("apex.manager.") or name.startswith("apex.ui.") for name in imports):
            bad.append(str(p))
    assert not bad, bad


def test_protection_owner_is_execution():
    source = Path("apex/execution/protection.py").read_text(encoding="utf-8")
    assert "class ProtectionState" in source
    assert "apex.manager" not in source
