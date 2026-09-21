"""Persistent Binance account snapshots owned by the production State DB."""

from __future__ import annotations

import sqlite3
from typing import Any, Callable, Mapping


class ExecutionAccountRepository:
    def __init__(self, connection_factory: Callable[[], sqlite3.Connection]) -> None:
        self.connection_factory = connection_factory

    def read(self, exchange: str = "binance_futures") -> dict[str, Any] | None:
        conn = self.connection_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT wallet_balance,available_balance,cross_unrealized_pnl,
                          fetched_at_epoch,attempted_at_epoch,last_error
                   FROM execution_account_cache WHERE exchange=?""",
                (str(exchange),),
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    def store_attempt(
        self,
        *,
        attempted_at: float,
        balance: Mapping[str, Any] | None = None,
        error: str = "",
        exchange: str = "binance_futures",
    ) -> None:
        conn = self.connection_factory()
        try:
            if balance is not None:
                conn.execute(
                    """INSERT INTO execution_account_cache
                       (exchange,wallet_balance,available_balance,cross_unrealized_pnl,
                        fetched_at_epoch,attempted_at_epoch,last_error,updated_at)
                       VALUES(?,?,?,?,?,?,NULL,CURRENT_TIMESTAMP)
                       ON CONFLICT(exchange) DO UPDATE SET
                           wallet_balance=excluded.wallet_balance,
                           available_balance=excluded.available_balance,
                           cross_unrealized_pnl=excluded.cross_unrealized_pnl,
                           fetched_at_epoch=excluded.fetched_at_epoch,
                           attempted_at_epoch=excluded.attempted_at_epoch,
                           last_error=NULL,updated_at=CURRENT_TIMESTAMP""",
                    (
                        str(exchange),
                        float(balance.get("wallet_balance", 0) or 0),
                        float(balance.get("available_balance", 0) or 0),
                        float(balance.get("cross_unrealized_pnl", 0) or 0),
                        float(attempted_at),
                        float(attempted_at),
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO execution_account_cache
                       (exchange,attempted_at_epoch,last_error,updated_at)
                       VALUES(?,?,?,CURRENT_TIMESTAMP)
                       ON CONFLICT(exchange) DO UPDATE SET
                           attempted_at_epoch=excluded.attempted_at_epoch,
                           last_error=excluded.last_error,updated_at=CURRENT_TIMESTAMP""",
                    (str(exchange), float(attempted_at), str(error)[:300]),
                )
            conn.commit()
        finally:
            conn.close()


__all__ = ["ExecutionAccountRepository"]
