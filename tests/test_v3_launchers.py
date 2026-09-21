from pathlib import Path


def test_worker_root_is_launcher_only():
    source = Path("bot.py").read_text(encoding="utf-8")
    assert "from apex.compatibility.legacy_bot_runtime import main" in source
    assert "def full_scan_raw(" not in source
    assert len(source.splitlines()) <= 8


def test_dashboard_root_is_launcher_only():
    source = Path("stats_server.py").read_text(encoding="utf-8")
    assert "from apex.ui.dashboard.server import main" in source
    assert "class Handler" not in source
    assert len(source.splitlines()) <= 8
