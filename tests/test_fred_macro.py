from unittest.mock import Mock, patch

import brain_builder


def test_fred_skips_missing_latest_observation(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "test-key")
    response = Mock(status_code=200)
    response.json.return_value = {
        "observations": [
            {"date": "2026-09-09", "value": "."},
            {"date": "2026-09-08", "value": "4.11"},
            {"date": "2026-09-05", "value": "4.07"},
        ]
    }

    with patch.object(brain_builder.requests, "get", return_value=response), \
            patch.object(brain_builder.time, "sleep"):
        result = brain_builder.fetch_fred_macro()

    assert result["DGS10"]["value"] == 4.11
    assert result["DGS10"]["change"] == 0.04
    assert result["DGS10"]["date"] == "2026-09-08"
