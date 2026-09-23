"""Atomic production delivery claims owned by the V3 State DB."""

from __future__ import annotations

import sqlite3
from typing import Callable

from apex.db.connection import connect_compatibility


class DeliveryClaimRepository:
    def __init__(self, conn_factory: Callable[[], sqlite3.Connection]) -> None:
        self._conn_factory = conn_factory

    def claim(self, cache_key: str, claimed_at: float, cooldown_seconds: float) -> bool:
        if not str(cache_key).strip():
            raise ValueError("delivery_cache_key_required")
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT claimed_at FROM delivery_claims WHERE cache_key=?",
                (cache_key,),
            ).fetchone()
            last_claim = float(row[0]) if row is not None else 0.0
            if last_claim > 0 and float(claimed_at) - last_claim < float(cooldown_seconds):
                conn.rollback()
                return False
            conn.execute(
                """INSERT INTO delivery_claims(cache_key,claimed_at,delivered_at)
                   VALUES(?,?,NULL)
                   ON CONFLICT(cache_key) DO UPDATE SET
                     claimed_at=excluded.claimed_at,
                     delivered_at=NULL,
                     updated_at=CURRENT_TIMESTAMP""",
                (cache_key, float(claimed_at)),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def release(self, cache_key: str, claimed_at: float) -> None:
        conn = self._conn_factory()
        try:
            conn.execute(
                "DELETE FROM delivery_claims WHERE cache_key=? AND claimed_at=? AND delivered_at IS NULL",
                (cache_key, float(claimed_at)),
            )
            conn.commit()
        finally:
            conn.close()

    def confirm(self, cache_key: str, claimed_at: float, delivered_at: float) -> bool:
        conn = self._conn_factory()
        try:
            updated = conn.execute(
                """UPDATE delivery_claims SET delivered_at=?,updated_at=CURRENT_TIMESTAMP
                     WHERE cache_key=? AND claimed_at=? AND delivered_at IS NULL""",
                (float(delivered_at), cache_key, float(claimed_at)),
            ).rowcount
            conn.commit()
            return updated == 1
        finally:
            conn.close()


def signal_delivery_key(candidate: dict, strategy: str) -> str:
    """Build a stable key independent of display labels or quality grades."""
    return ":".join((
        str(candidate.get("symbol") or "").upper(),
        str(strategy or "MTF").upper(),
        str(candidate.get("direction") or "").upper(),
        str(candidate.get("timeframe") or "1h").lower(),
    ))


def _repository(db_path: str) -> DeliveryClaimRepository:
    return DeliveryClaimRepository(lambda: connect_compatibility(db_path, timeout=30))


def claim_signal_delivery(
    db_path: str, cache_key: str, now_ts: float, cooldown_seconds: float,
) -> bool:
    return _repository(db_path).claim(cache_key, now_ts, cooldown_seconds)


def release_signal_delivery_claim(db_path: str, cache_key: str, claim_ts: float) -> None:
    _repository(db_path).release(cache_key, claim_ts)


def confirm_signal_delivery(
    db_path: str, cache_key: str, claim_ts: float, delivered_at: float,
) -> bool:
    return _repository(db_path).confirm(cache_key, claim_ts, delivered_at)


__all__ = [
    "DeliveryClaimRepository", "claim_signal_delivery", "confirm_signal_delivery",
    "release_signal_delivery_claim", "signal_delivery_key",
]
