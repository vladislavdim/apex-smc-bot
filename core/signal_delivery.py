"""Atomic Telegram signal delivery claims.

The worker can reach the same candidate from overlapping/manual scan paths.  A
claim is written before Telegram delivery so only one coroutine in the current
database generation may send it.  Failed/cancelled deliveries release their
claim; successful claims remain the normal cooldown record and are included in
the immediate durable brain checkpoint.
"""

from __future__ import annotations

from apex.db.connection import connect_compatibility as _connect_compatibility_db
from apex.db.repositories.deliveries import DeliveryClaimRepository


def signal_delivery_key(candidate: dict, strategy: str) -> str:
    """Build a stable key independent of display labels or quality grades."""
    return ":".join((
        str(candidate.get("symbol") or "").upper(),
        str(strategy or "MTF").upper(),
        str(candidate.get("direction") or "").upper(),
        str(candidate.get("timeframe") or "1h").lower(),
    ))


def claim_signal_delivery(
    db_path: str,
    cache_key: str,
    now_ts: float,
    cooldown_seconds: float,
) -> bool:
    """Atomically reserve delivery in the production State DB."""
    repository = DeliveryClaimRepository(
        lambda: _connect_compatibility_db(db_path, timeout=30)
    )
    return repository.claim(cache_key, now_ts, cooldown_seconds)


def release_signal_delivery_claim(db_path: str, cache_key: str, claim_ts: float) -> None:
    """Release only this attempt's claim after a confirmed delivery failure."""
    repository = DeliveryClaimRepository(
        lambda: _connect_compatibility_db(db_path, timeout=30)
    )
    repository.release(cache_key, claim_ts)


def confirm_signal_delivery(
    db_path: str, cache_key: str, claim_ts: float, delivered_at: float,
) -> bool:
    repository = DeliveryClaimRepository(
        lambda: _connect_compatibility_db(db_path, timeout=30)
    )
    return repository.confirm(cache_key, claim_ts, delivered_at)
