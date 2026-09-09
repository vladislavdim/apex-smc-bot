import gzip
import hashlib
import json

import stats_server


class Response:
    def __init__(self, body): self.body = body; self.offset = 0
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def read(self, size=-1):
        if self.offset >= len(self.body):
            return b""
        if size is None or size < 0:
            size = len(self.body) - self.offset
        chunk = self.body[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


def test_dashboard_falls_back_to_bounded_github_release_asset(monkeypatch):
    payload = {"candles": [{"timeframe": "15m", "candles": 1}],
               "runs": [{"research_run_id": "run-1", "status": "COMPLETED", "progress": 100}],
               "storage": {"snapshot_version": "research-snapshot-v2", "research_run_id": "run-1"}}
    compressed = gzip.compress(json.dumps(payload).encode())
    manifest = {"snapshot_version": "research-snapshot-v2", "symbol": "BTCUSDT",
                "timeframes": ["15m", "1h", "4h", "1d"], "no_real_execution": True,
                "live_activation": "FORBIDDEN", "latest_run": {"research_run_id": "run-1"},
                "dashboard_gz": {"sha256": hashlib.sha256(compressed).hexdigest()}}
    calls = []
    def urlopen(request, timeout):
        calls.append(request.full_url)
        if "/releases/tags/" in request.full_url:
            return Response(json.dumps({"assets": [
                {"name": "BTCUSDT.dashboard.json.gz", "browser_download_url": "https://example.test/BTCUSDT.dashboard.json.gz"},
                {"name": "BTCUSDT.manifest.json", "browser_download_url": "https://example.test/BTCUSDT.manifest.json"},
            ]}).encode())
        if request.full_url.endswith("manifest.json"):
            return Response(json.dumps(manifest).encode())
        return Response(compressed)
    monkeypatch.setattr(stats_server.urllib.request, "urlopen", urlopen)
    monkeypatch.setitem(stats_server._RESEARCH_RELEASE_CACHE, "value", None)
    result = stats_server._github_research_dashboard()
    assert result["storage"]["source"] == "GITHUB_RELEASE"
    assert len(calls) == 3
    assert stats_server._github_research_dashboard() is result
    assert len(calls) == 3


def test_dashboard_rejects_manifest_checksum_mismatch(monkeypatch):
    payload = gzip.compress(json.dumps({"runs": [{"status": "COMPLETED", "progress": 100}],
                                        "storage": {}}).encode())
    manifest = {"snapshot_version": "research-snapshot-v2", "symbol": "BTCUSDT",
                "timeframes": ["15m", "1h", "4h", "1d"], "no_real_execution": True,
                "live_activation": "FORBIDDEN", "dashboard_gz": {"sha256": "0" * 64}}
    def urlopen(request, timeout):
        if "/releases/tags/" in request.full_url:
            return Response(json.dumps({"assets": [
                {"name": "BTCUSDT.dashboard.json.gz", "browser_download_url": "https://example.test/dashboard"},
                {"name": "BTCUSDT.manifest.json", "browser_download_url": "https://example.test/manifest"},
            ]}).encode())
        return Response(json.dumps(manifest).encode() if request.full_url.endswith("manifest") else payload)
    monkeypatch.setattr(stats_server.urllib.request, "urlopen", urlopen)
    monkeypatch.setitem(stats_server._RESEARCH_RELEASE_CACHE, "value", None)
    try:
        stats_server._github_research_dashboard()
    except RuntimeError as exc:
        assert "checksum" in str(exc)
    else:
        raise AssertionError("checksum mismatch must fail closed")


def test_btc_research_workflow_is_isolated_from_render_worker():
    source = open(".github/workflows/btc-research.yml", encoding="utf-8").read()
    assert "runs-on: ubuntu-latest" in source
    assert 'APEX_RESEARCH_GATE_RPS: "1"' in source
    assert "BTCUSDT.research.db.gz" in source
    assert "BTCUSDT.manifest.json" in source
    assert "--verify-compressed" in source
    assert "set -euo pipefail" in source
    assert "gh api -i" in source
    assert "Only an explicit GitHub 404" in source
    assert "schedule:" in source
