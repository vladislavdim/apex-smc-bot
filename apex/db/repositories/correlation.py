"""Immutable candidate-to-outcome correlation chain."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from typing import Any, Callable, Mapping

from apex.domain.models import VersionManifest
from apex.domain.ids import is_id


class CorrelationError(RuntimeError):
    pass


class TradeCorrelationRepository:
    _STEPS = (
        ("signal_id", None),
        ("execution_id", "signal_id"),
        ("position_id", "execution_id"),
        ("outcome_id", "position_id"),
    )

    def __init__(self, conn_factory: Callable[[], sqlite3.Connection]) -> None:
        self._conn_factory = conn_factory

    def create_candidate(
        self,
        candidate_id: str,
        *,
        release_sha: str,
        versions: VersionManifest | Mapping[str, Any],
    ) -> None:
        if not is_id(candidate_id, "candidate"):
            raise CorrelationError("invalid_entity_id:candidate_id")
        payload = asdict(versions) if isinstance(versions, VersionManifest) else dict(versions)
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT release_sha,versions_json FROM trade_correlation WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            if existing is None:
                conn.execute(
                    "INSERT INTO trade_correlation(candidate_id,release_sha,versions_json) VALUES(?,?,?)",
                    (candidate_id, release_sha, encoded),
                )
            elif str(existing[0]) != release_sha or str(existing[1]) != encoded:
                raise CorrelationError("candidate_identity_conflict")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def attach(self, candidate_id: str, field: str, entity_id: str) -> None:
        predecessors = dict(self._STEPS)
        if field not in predecessors:
            raise CorrelationError(f"unknown_correlation_step:{field}")
        if not entity_id:
            raise CorrelationError(f"empty_correlation_id:{field}")
        entity = field.removesuffix("_id")
        if not is_id(entity_id, entity):
            raise CorrelationError(f"invalid_entity_id:{field}")
        conn = self._conn_factory()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT signal_id,execution_id,position_id,outcome_id FROM trade_correlation WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
            if row is None:
                raise CorrelationError("candidate_not_found")
            names = ("signal_id", "execution_id", "position_id", "outcome_id")
            current = dict(zip(names, row))
            predecessor = predecessors[field]
            if predecessor and not current[predecessor]:
                raise CorrelationError(f"missing_predecessor:{predecessor}")
            if current[field] is not None and str(current[field]) != entity_id:
                raise CorrelationError(f"immutable_step_conflict:{field}")
            if current[field] is None:
                try:
                    conn.execute(
                        f"UPDATE trade_correlation SET {field}=?,updated_at=CURRENT_TIMESTAMP WHERE candidate_id=?",  # noqa: S608 - field is allowlisted
                        (entity_id, candidate_id),
                    )
                except sqlite3.IntegrityError as exc:
                    raise CorrelationError(f"duplicate_entity_id:{field}") from exc
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get(self, candidate_id: str) -> dict[str, Any] | None:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM trade_correlation WHERE candidate_id=?", (candidate_id,),
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["versions"] = json.loads(result.pop("versions_json"))
            return result
        finally:
            conn.close()

    def get_by_signal(self, signal_id: str) -> dict[str, Any] | None:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM trade_correlation WHERE signal_id=?", (str(signal_id),),
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["versions"] = json.loads(result.pop("versions_json"))
            return result
        finally:
            conn.close()

    def pending_outcomes(self) -> list[dict[str, Any]]:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM trade_correlation
                     WHERE position_id IS NOT NULL AND outcome_id IS NULL
                     ORDER BY created_at"""
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["versions"] = json.loads(item.pop("versions_json"))
                result.append(item)
            return result
        finally:
            conn.close()


__all__ = ["CorrelationError", "TradeCorrelationRepository"]
