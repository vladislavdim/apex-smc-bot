from pathlib import Path

from scripts import verify_production_authority as audit


def test_repository_production_authority_is_read_only():
    assert audit.violations() == ()


def test_audit_detects_forbidden_runtime_mutation(tmp_path, monkeypatch):
    worker = tmp_path / "worker.py"
    worker.write_text("git push origin main\n", encoding="utf-8")
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "TARGETS", (worker,))
    monkeypatch.setattr(audit, "WORKFLOW_ROOT", tmp_path / "workflows")
    (tmp_path / "workflows").mkdir()
    assert audit.violations() == ("worker.py:1:git push origin main",)
