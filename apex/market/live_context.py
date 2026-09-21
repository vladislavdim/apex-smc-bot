"""Build non-authoritative live context from current Gate public responses."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .derivatives import PointInTimeContext, normalize_contract_stats, normalize_funding
from .gate_client import GateMarketClient, GateResponse
from .microstructure import GateOrderBook
from .orderflow import real_cvd


def _rows(response: GateResponse) -> list[Mapping[str, Any]]:
    payload = response.payload
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, Mapping)]


def _unknown(feature: str, source: str, error: str) -> dict[str, Any]:
    return {
        "feature": feature, "source": source, "event_time": None,
        "received_at": None, "age_seconds": None, "freshness": "UNAVAILABLE",
        "availability": "UNKNOWN", "quality": "UNKNOWN", "point_in_time": True,
        "available": False, "value": None, "error": error,
    }


@dataclass(frozen=True)
class LiveContextBuild:
    derivatives: Mapping[str, Any]
    microstructure: Mapping[str, Any]
    errors: tuple[str, ...]


def fetch_live_context(client: GateMarketClient, symbol: str, *, as_of: int) -> LiveContextBuild:
    """Fetch each optional source independently; one failure never blocks strategy."""
    observations = []
    errors: list[str] = []
    missing: dict[str, dict[str, Any]] = {}

    try:
        stats = client.contract_stats(symbol)
        for row in _rows(stats):
            observations.extend(normalize_contract_stats(row, received_at=stats.received_at))
    except Exception as exc:
        code = f"contract_stats:{type(exc).__name__}"
        errors.append(code)
        for feature in ("open_interest", "long_short_ratio", "liquidations"):
            missing[feature] = _unknown(feature.upper(), "gate_contract_stats", code)

    try:
        funding = client.funding_history(symbol)
        observations.extend(normalize_funding(row, received_at=funding.received_at) for row in _rows(funding))
    except Exception as exc:
        code = f"funding:{type(exc).__name__}"
        errors.append(code)
        missing["funding"] = _unknown("FUNDING", "gate_funding", code)

    derivatives = PointInTimeContext(observations).as_of(int(as_of))
    derivatives.update(missing)

    microstructure: dict[str, Any] = {}
    try:
        trades = client.public_trades(symbol)
        if trades.received_at > as_of:
            raise ValueError("received_after_as_of")
        trade_rows = [row for row in _rows(trades) if int(float(row.get("create_time", row.get("time", 0))) or 0) <= as_of]
        event_times = [
            int(float(row.get("create_time", row.get("time", 0))) or 0)
            for row in trade_rows
        ]
        event_time = max(event_times) if event_times else None
        cvd = real_cvd(trade_rows, source="gate_rest_trades")
        microstructure["cvd_real"] = {
            **cvd,
            "received_at": trades.received_at,
            "event_time": event_time,
            "age_seconds": max(0, int(as_of) - int(event_time)) if event_time else None,
            "status": "FRESH" if cvd.get("available") else "UNAVAILABLE",
            "freshness_status": "FRESH" if cvd.get("available") else "UNAVAILABLE",
            "point_in_time": True,
        }
    except Exception as exc:
        code = f"trades:{type(exc).__name__}"
        errors.append(code)
        microstructure["cvd_real"] = _unknown("CVD_REAL", "gate", code)

    try:
        book_response = client.order_book(symbol)
        if book_response.received_at > as_of:
            raise ValueError("received_after_as_of")
        book_payload = book_response.payload if isinstance(book_response.payload, Mapping) else {}
        book = GateOrderBook(symbol)
        book.snapshot(
            book_payload.get("bids", book_payload.get("b")),
            book_payload.get("asks", book_payload.get("a")),
            book_payload.get("id", book_payload.get("current")),
            observed_at=book_response.received_at,
        )
        # A REST snapshot is visible liquidity at one instant, never a historical stop map.
        microstructure["visible_orderbook"] = {
            **book.features(now=float(book_response.received_at)),
            "received_at": book_response.received_at,
            "availability": "FORWARD_ONLY",
            "point_in_time": True,
        }
    except Exception as exc:
        code = f"order_book:{type(exc).__name__}"
        errors.append(code)
        microstructure["visible_orderbook"] = _unknown("VISIBLE_ORDERBOOK_LIQUIDITY", "gate", code)

    return LiveContextBuild(derivatives, microstructure, tuple(errors))


__all__ = ["LiveContextBuild", "fetch_live_context"]
