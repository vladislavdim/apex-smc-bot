"""Atomic production delivery claims owned by the V3 State DB."""

from __future__ import annotations

import sqlite3
from typing import Callable


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


__all__ = ["DeliveryClaimRepository"]
