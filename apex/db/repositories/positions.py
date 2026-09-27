"""Canonical confirmed-position repository.

Positions are exchange-confirmed execution facts. Manager lifecycle state is
kept in ManagerRepository and is deliberately not aliased here.
"""
from __future__ import annotations
import sqlite3
from typing import Any,Callable

class PositionRepository:
    def __init__(self,conn_factory:Callable[[],sqlite3.Connection])->None:
        self._conn_factory=conn_factory

    def get(self,position_id:str)->dict[str,Any]|None:
        conn=self._conn_factory()
        try:
            conn.row_factory=sqlite3.Row
            row=conn.execute("SELECT * FROM executions WHERE position_id=?",(str(position_id),)).fetchone()
            return dict(row) if row else None
        finally: conn.close()

    def active(self,*,limit:int=500)->list[dict[str,Any]]:
        conn=self._conn_factory()
        try:
            conn.row_factory=sqlite3.Row
            cols={str(r[1]) for r in conn.execute("PRAGMA table_info(executions)").fetchall()}
            status_col="status" if "status" in cols else "execution_status"
            rows=conn.execute(f"SELECT * FROM executions WHERE {status_col} IN ('OPEN','ACTIVE','FILLED','PROTECTED') ORDER BY rowid DESC LIMIT ?",(max(1,min(int(limit),5000)),)).fetchall()
            return [dict(r) for r in rows]
        finally: conn.close()

__all__=["PositionRepository"]
