from unittest.mock import Mock

from research.gate_history import GateHistoryClient, ResearchBudget


def _client(response):
    session = Mock()
    session.headers = {}
    session.get.return_value = response
    return GateHistoryClient(session=session, budget=ResearchBudget(per_second=1000))


def test_candle_range_does_not_mix_limit_with_from_and_to(monkeypatch):
    response = Mock(status_code=200)
    response.json.return_value = []
    response.raise_for_status.return_value = None
    monkeypatch.setattr("research.gate_history.time.time", lambda: 2_000_000_000)
    client = _client(response)

    client.candles("BTCUSDT", "15m", 1_700_000_000, 1_700_000_900)

    params = client.session.get.call_args.kwargs["params"]
    assert params == {
        "contract": "BTC_USDT",
        "interval": "15m",
        "from": 1_700_000_000,
        "to": 1_700_000_900,
    }


def test_gate_400_exposes_public_reason_without_retry():
    response = Mock(status_code=400)
    response.json.return_value = {
        "label": "INVALID_PARAM_VALUE",
        "message": "limit and from and to cannot be present at the same time",
    }
    client = _client(response)

    try:
        client.candles("BTCUSDT", "15m", 1, 901)
    except RuntimeError as exc:
        assert "INVALID_PARAM_VALUE" in str(exc)
        assert "cannot be present" in str(exc)
    else:
        raise AssertionError("Gate 400 must fail closed")
    assert client.session.get.call_count == 1
