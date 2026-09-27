"""Canonical confirmed-position repository surface.

Positions are exchange-confirmed execution state.  This module deliberately does
not alias Manager storage: Manager consumes positions, it does not own them.
"""
from __future__ import annotations
import sqlite3
from typing import Callable,Any
class PositionRepository:
    def __init__(self,conn_factory:Callable[[],sqlite3.Connection])->None:self._conn_factory=conn_factory
    def get(self,position_id:str)->dict[str,Any]|None:
        conn=self._conn_factory()
        try:
            conn.row_factory=sqlite3.Row
            for table in ("execution_positions","positions"):
                try: row=conn.execute(f"SELECT * FROM {table} WHERE position_id=?",(position_id,)).fetchone()
                except sqlite3.OperationalError: continue
                return dict(row) if row else None
            return None
        finally: conn.close()
__all__=["PositionRepository"]
