from research.replay import _geometry
from research.gate_history import backfill_pair
from research.store import ResearchStore


def snapshot(highs, lows):
    return {"price": 100, "volatility": {"atr": 1}, "structure": {
        "swings": [{"side": "HIGH", "price": x} for x in highs]
        + [{"side": "LOW", "price": x} for x in lows]}}


def test_no_synthetic_target_or_stop():
    assert _geometry(snapshot([], [98]), "BULLISH") is None
    assert _geometry(snapshot([106], []), "BULLISH") is None
    assert _geometry(snapshot([102], []), "BEARISH") is None


def test_near_target_cannot_be_skipped_to_manufacture_rr():
    assert _geometry(snapshot([101, 110], [98]), "BULLISH") is None
    assert _geometry(snapshot([102], [99, 90]), "BEARISH") is None


def test_first_target_itself_meets_floor():
    for direction, data in [("BULLISH", snapshot([106], [98])),
                            ("BEARISH", snapshot([102], [94]))]:
        levels = _geometry(data, direction)
        assert levels is not None
        assert abs(levels["tp1"]-levels["entry"])/abs(levels["entry"]-levels["sl"]) >= 2


def test_empty_history_response_retries_same_page(tmp_path):
    store = ResearchStore(str(tmp_path / "market.db"))
    store.ensure_schema()
    calls = []
    class Client:
        def candles(self, symbol, timeframe, start, end):
            calls.append(start)
            return []
    for _ in range(2):
        result = backfill_pair(store, Client(), "AAVEUSDT", "15m", 900, 3600)
        assert result["status"] == "PAUSED"
    assert calls == [900, 900]
