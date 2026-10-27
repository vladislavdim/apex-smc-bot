"""Active analytical history shows fresh Gate progress without inventing execution."""

from datetime import datetime, timezone

import pytest

from apex.ui.telegram.trades import (
    enrich_active_analytical_progress,
    fetch_trade_view_rows,
    format_trade_view,
)


NOW = datetime(2026, 10, 27, 17, 22, tzinfo=timezone.utc)


def _row(**overrides):
    row = {
        "id": 480,
        "symbol": "SOLUSDT",
        "direction": "BULLISH",
        "grade": "ZONE",
        "timeframe": "4h",
        "entry": 100.08,
        "sl": 99.288,
        "tp1": 102.69,
        "lifecycle_status": "active",
        "result": "pending",
        "execution_status": "DISABLED",
        "_view_kind": "analytical",
    }
    row.update(overrides)
    return row


def test_active_long_shows_live_gate_r_and_percent():
    rows = enrich_active_analytical_progress(
        [_row()], {"SOLUSDT": {"price": 100.85}}, observed_at=NOW
    )
    text = format_trade_view("active", rows)
    assert "Цена Gate <code>100.85</code>" in text
    assert "текущий результат <b>+0.97R · +0.77%</b>" in text
    assert "не означает исполненную сделку на Binance" in text
    assert "без плеча, комиссий, funding и проскальзывания" in text


def test_active_short_uses_directional_result():
    rows = enrich_active_analytical_progress(
        [_row(direction="BEARISH", entry=100, sl=105)],
        {"SOLUSDT": {"price": 97.5}},
        observed_at=NOW,
    )
    text = format_trade_view("active", rows)
    assert "текущий результат <b>+0.50R · +2.50%</b>" in text


@pytest.mark.parametrize("row", [
    _row(lifecycle_status="waiting_entry"),
    _row(_view_kind="live"),
])
def test_waiting_or_live_manager_rows_are_not_given_synthetic_analytical_progress(row):
    result = enrich_active_analytical_progress(
        [row], {"SOLUSDT": {"price": 100.85}}, observed_at=NOW
    )[0]
    assert "display_r" not in result


@pytest.mark.parametrize("quote", [{}, {"price": 0}, {"price": "nan"}, {"price": "bad"}])
def test_invalid_or_missing_quote_fails_display_closed(quote):
    row = enrich_active_analytical_progress(
        [_row()], {"SOLUSDT": quote}, observed_at=NOW
    )[0]
    text = format_trade_view("active", [row])
    assert "Текущий результат: цена временно недоступна" in text
    assert "текущий результат <b>" not in text


def test_active_view_reads_market_once_after_state_query(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "apex.ui.telegram.trades.fetch_live_trades",
        lambda category, limit, factory: [_row()],
    )

    def prices():
        calls.append("market")
        return {"SOLUSDT": {"price": 100.85}}

    rows = fetch_trade_view_rows("active", 12, lambda: None, prices)
    assert calls == ["market"]
    assert rows[0]["display_r"] == pytest.approx(0.9722222222)


def test_non_active_view_does_not_request_market(monkeypatch):
    monkeypatch.setattr(
        "apex.ui.telegram.trades.fetch_live_trades",
        lambda category, limit, factory: [_row(lifecycle_status="closed")],
    )
    rows = fetch_trade_view_rows(
        "closed", 12, lambda: None,
        lambda: pytest.fail("market provider must not be called"),
    )
    assert "display_r" not in rows[0]
