import gzip
import json

import stats_server


class Response:
    def __init__(self, body): self.body = body
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def read(self, *_): return self.body


def test_dashboard_falls_back_to_bounded_github_release_asset(monkeypatch):
    payload = {"candles": [{"timeframe": "15m", "candles": 1}], "runs": [{}], "storage": {}}
    calls = []
    def urlopen(request, timeout):
        calls.append(request.full_url)
        if "/releases/tags/" in request.full_url:
            return Response(json.dumps({"assets": [{"name": "BTCUSDT.dashboard.json.gz",
                "browser_download_url": "https://example.test/BTCUSDT.dashboard.json.gz"}]}).encode())
        return Response(gzip.compress(json.dumps(payload).encode()))
    monkeypatch.setattr(stats_server.urllib.request, "urlopen", urlopen)
    monkeypatch.setitem(stats_server._RESEARCH_RELEASE_CACHE, "value", None)
    result = stats_server._github_research_dashboard()
    assert result["storage"]["source"] == "GITHUB_RELEASE"
    assert len(calls) == 2
    assert stats_server._github_research_dashboard() is result
    assert len(calls) == 2


def test_btc_research_workflow_is_isolated_from_render_worker():
    source = open(".github/workflows/btc-research.yml", encoding="utf-8").read()
    assert "runs-on: ubuntu-latest" in source
    assert 'APEX_RESEARCH_GATE_RPS: "1"' in source
    assert "BTCUSDT.research.db.gz" in source
    assert "schedule:" in source
