from pathlib import Path

def test_launchers_are_thin():
    assert len(Path("bot.py").read_text().splitlines()) < 30
    assert len(Path("stats_server.py").read_text().splitlines()) < 40

def test_required_v3_surfaces_exist():
    required=["apex/app/shutdown.py","apex/config/constants.py","apex/domain/events.py","apex/db/integrity.py","apex/market/gate_ws.py","apex/risk/portfolio.py","apex/execution/binance_client.py","apex/execution/reconcile.py","apex/manager/playbooks.py","apex/learning/outcomes.py","apex/telemetry/metrics.py","apex/ops/graceful_shutdown.py","apex/ui/dashboard/api.py","scripts/verify_release.py","scripts/inspect_state.py"]
    assert not [p for p in required if not Path(p).is_file()]

def test_execution_boundary_does_not_import_ui():
    for p in Path("apex/execution").glob("*.py"):
        text=p.read_text()
        assert "apex.ui.telegram" not in text
        assert "apex.ui.dashboard" not in text
