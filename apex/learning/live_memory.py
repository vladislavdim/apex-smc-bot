"""Persistence boundary that refuses simulated or unconfirmed outcomes."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from typing import Any, Callable, Mapping

from apex.domain.models import Candidate, GroqReview, RiskDecision, TradeOutcome


class LiveMemoryError(RuntimeError):
    pass


def _payload(value: Any) -> str:
    if value is None:
        return "null"
    if hasattr(value, "__dataclass_fields__"):
        value = asdict(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class LiveMemoryRepository:
    def __init__(self, conn_factory: Callable[[], sqlite3.Connection]) -> None:
        self._conn_factory = conn_factory

    def record_candidate(
        self,
        candidate: Candidate,
        *,
        signal_id: str | None,
        release_sha: str,
        groq: GroqReview | Mapping[str, Any] | None = None,
        risk: RiskDecision | Mapping[str, Any] | None = None,
        executed: bool = False,
    ) -> None:
        if not candidate.candidate_id or not release_sha:
            raise LiveMemoryError("candidate_identity_missing")
        encoded = _payload(candidate)
        conn = self._conn_factory()
        try:
            existing = conn.execute(
                "SELECT candidate_json,release_sha,signal_id FROM live_candidates WHERE candidate_id=?",
                (candidate.candidate_id,),
            ).fetchone()
            if existing and (str(existing[0]) != encoded or str(existing[1]) != release_sha):
                raise LiveMemoryError("candidate_identity_conflict")
            if existing and existing[2] is not None and signal_id is not None and str(existing[2]) != signal_id:
                raise LiveMemoryError("signal_identity_conflict")
            conn.execute(
                """INSERT OR IGNORE INTO live_candidates(
                       candidate_id,signal_id,strategy,symbol,direction,candidate_json,
                       groq_json,risk_json,executed,release_sha,created_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    candidate.candidate_id, signal_id, candidate.strategy.value,
                    candidate.symbol, candidate.direction.value, encoded,
                    _payload(groq), _payload(risk), int(bool(executed)), release_sha,
                    candidate.created_at.isoformat(),
                ),
            )
            conn.execute(
                """UPDATE live_candidates
                      SET signal_id=COALESCE(signal_id,?),
                          groq_json=CASE WHEN ?=0 THEN groq_json ELSE ? END,
                          risk_json=CASE WHEN ?=0 THEN risk_json ELSE ? END,
                          executed=MAX(executed,?)
                    WHERE candidate_id=?""",
                (
                    signal_id,
                    int(groq is not None), _payload(groq),
                    int(risk is not None), _payload(risk), int(bool(executed)),
                    candidate.candidate_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_executed(self, candidate_id: str, *, signal_id: str) -> None:
        """Mark a candidate executable only after a confirmed live position exists."""
        conn = self._conn_factory()
        try:
            row = conn.execute(
                "SELECT signal_id FROM live_candidates WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
            if row is None:
                raise LiveMemoryError("candidate_not_found")
            if row[0] is None or str(row[0]) != str(signal_id):
                raise LiveMemoryError("signal_identity_conflict")
            conn.execute(
                "UPDATE live_candidates SET executed=1 WHERE candidate_id=?",
                (candidate_id,),
            )
            conn.commit()
        finally:
            conn.close()

    def candidate_metadata(self, candidate_id: str) -> dict[str, Any]:
        conn = self._conn_factory()
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT strategy,symbol,direction,release_sha,executed
                     FROM live_candidates WHERE candidate_id=?""",
                (candidate_id,),
            ).fetchone()
            if row is None:
                raise LiveMemoryError("candidate_not_found")
            return dict(row)
        finally:
            conn.close()

    def record_outcome(
        self,
        outcome: TradeOutcome,
        *,
        candidate_id: str,
        execution_id: str,
        strategy: str,
        symbol: str,
        direction: str,
        release_sha: str,
        confirmed_position: bool,
        gross_r: float,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        if not confirmed_position:
            raise LiveMemoryError("unconfirmed_position_forbidden")
        if not candidate_id or not execution_id or not outcome.position_id:
            raise LiveMemoryError("trade_correlation_incomplete")
        metadata = dict(context or {})
        conn = self._conn_factory()
        try:
            candidate = conn.execute(
                "SELECT executed,release_sha FROM live_candidates WHERE candidate_id=?", (candidate_id,),
            ).fetchone()
            if candidate is None or not bool(candidate[0]):
                raise LiveMemoryError("executed_candidate_not_found")
            if str(candidate[1]) != release_sha:
                raise LiveMemoryError("release_identity_conflict")
            values = (
                outcome.outcome_id, outcome.position_id, str(strategy).upper(), symbol.upper(),
                str(direction).upper(), float(outcome.net_r), float(outcome.fees),
                float(outcome.funding), _payload(metadata.get("regime", {})),
                _payload(metadata), outcome.closed_at.isoformat(), candidate_id, execution_id,
                float(gross_r), metadata.get("mfe_r"), metadata.get("mae_r"),
                metadata.get("duration_seconds"), metadata.get("setup_type"),
                metadata.get("session"), metadata.get("volatility"),
                metadata.get("btc_state"), release_sha,
            )
            existing = conn.execute(
                "SELECT position_id,net_r,fees,funding FROM live_trade_outcomes WHERE outcome_id=?",
                (outcome.outcome_id,),
            ).fetchone()
            if existing and tuple(existing) != (
                outcome.position_id, float(outcome.net_r), float(outcome.fees), float(outcome.funding),
            ):
                raise LiveMemoryError("outcome_identity_conflict")
            conn.execute(
                """INSERT OR IGNORE INTO live_trade_outcomes(
                       outcome_id,position_id,strategy,symbol,direction,net_r,fees,funding,
                       regime_json,context_json,closed_at,candidate_id,execution_id,gross_r,
                       mfe_r,mae_r,duration_seconds,setup_type,session,volatility,btc_state,release_sha
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                values,
            )
            conn.commit()
        finally:
            conn.close()


__all__ = ["LiveMemoryError", "LiveMemoryRepository"]
