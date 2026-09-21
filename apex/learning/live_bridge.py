"""Bridge the compatibility runtime to V3 live-only learning.

This module never submits orders and never infers fills from candles.  It may
create the immutable correlation chain, but it marks a candidate executed only
after the compatibility execution layer reports an exchange-protected live
position.  Closed outcomes require complete confirmed-fill accounting,
including an explicitly resolved funding value.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable, Mapping
from apex.db.connection import connect_compatibility

from apex.config.settings import ApexConfig
from apex.config.versions import (
    GROQ_PROMPT_VERSION,
    MANAGER_VERSION,
    REGIME_VERSION,
    RISK_VERSION,
    SCHEMA_VERSION,
)
from apex.db.repositories.correlation import TradeCorrelationRepository
from apex.db.repositories.executions import ExecutionRepository
from apex.domain.enums import Direction, Strategy
from apex.domain.ids import derived_id, is_id
from apex.domain.models import Candidate, TradeOutcome, VersionManifest
from apex.learning.live_memory import LiveMemoryError, LiveMemoryRepository


class LiveBridgeError(RuntimeError):
    pass


def _parse_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            raise LiveBridgeError("candidate_created_at_missing")
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _candidate_id(release_sha: str, signal_id: int) -> str:
    return derived_id("candidate", release_sha, signal_id)


def _stable_id(prefix: str, *parts: object) -> str:
    entities = {
        "cand": "candidate", "sig": "signal", "exec": "execution",
        "pos": "position", "mgr": "manager_event", "out": "outcome",
    }
    try:
        entity = entities[prefix]
    except KeyError as exc:
        raise LiveBridgeError(f"unknown_correlation_id_prefix:{prefix}") from exc
    return derived_id(entity, *parts)


class LiveLearningBridge:
    """Idempotent live correlation and learning adapter."""

    _POSITION_STATUSES = frozenset({"PROTECTED", "PROTECTED_NO_TP"})

    def __init__(
        self,
        config: ApexConfig,
        *,
        compatibility_db_path: str,
        state_factory: Callable[[], sqlite3.Connection],
        memory_factory: Callable[[], sqlite3.Connection],
    ) -> None:
        self.config = config
        self.compatibility_db_path = compatibility_db_path
        self.correlations = TradeCorrelationRepository(state_factory)
        self.executions = ExecutionRepository(state_factory)
        self.memory = LiveMemoryRepository(memory_factory)

    @property
    def release_sha(self) -> str:
        return str(self.config.runtime.release_sha).strip().lower()

    def _versions(self, strategy: Strategy) -> VersionManifest:
        return VersionManifest(
            release_sha=self.release_sha,
            strategy_version=self.config.strategies.version_for(strategy.value),
            manager_version=MANAGER_VERSION,
            risk_version=RISK_VERSION,
            regime_version=REGIME_VERSION,
            groq_prompt_version=GROQ_PROMPT_VERSION,
            schema_version=SCHEMA_VERSION,
        )

    def _signal(self, signal_id: int) -> Mapping[str, Any]:
        conn = connect_compatibility(self.compatibility_db_path, timeout=20)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("SELECT * FROM signals WHERE id=?", (int(signal_id),)).fetchone()
            if row is None:
                raise LiveBridgeError("signal_not_found")
            return dict(row)
        finally:
            conn.close()

    def register_signal(
        self,
        signal_id: int,
        *,
        groq: Mapping[str, Any] | None = None,
        risk: Mapping[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> str | None:
        """Register one delivered production signal; unsupported legacy types are ignored."""
        row = self._signal(signal_id)
        try:
            strategy = Strategy(str(row.get("signal_type") or row.get("grade") or "").upper())
        except ValueError:
            return None
        direction = Direction.normalize(row.get("direction"))
        entry = float(row.get("entry") or 0)
        stop = float(row.get("sl") or 0)
        tp1 = float(row.get("tp1") or 0)
        tp2 = float(row.get("tp2") or tp1)
        tp3_raw = row.get("tp3")
        tp3 = float(tp3_raw) if tp3_raw not in (None, "") else None
        terminal = tp3 if tp3 is not None else tp2
        risk_distance = abs(entry - stop)
        if not entry or not stop or not tp1 or risk_distance <= 0:
            raise LiveBridgeError("invalid_candidate_geometry")
        reward = (terminal - entry) if direction is Direction.LONG else (entry - terminal)
        candidate_id = _candidate_id(self.release_sha, signal_id)
        versions = self._versions(strategy)
        canonical_snapshot_id = (
            str(snapshot_id) if is_id(snapshot_id, "snapshot")
            else derived_id("snapshot", self.release_sha, snapshot_id or signal_id)
        )
        candidate = Candidate(
            candidate_id=candidate_id,
            symbol=str(row.get("symbol") or "").upper(),
            strategy=strategy,
            direction=direction,
            entry=entry,
            initial_sl=stop,
            tp1=tp1,
            tp2=tp2,
            tp3=tp3,
            rr=reward / risk_distance,
            snapshot_id=canonical_snapshot_id,
            created_at=_parse_utc(row.get("created_at")),
            versions=versions,
        )
        self.correlations.create_candidate(
            candidate_id, release_sha=self.release_sha, versions=versions,
        )
        canonical_signal_id = _stable_id("sig", self.release_sha, signal_id)
        self.correlations.attach(candidate_id, "signal_id", canonical_signal_id)
        self.memory.record_candidate(
            candidate,
            signal_id=canonical_signal_id,
            release_sha=self.release_sha,
            groq=groq,
            risk=risk,
        )
        return candidate_id

    def sync_execution(self, signal_id: int) -> dict[str, Any]:
        """Advance execution/position IDs without treating an entry order as a fill."""
        canonical_signal_id = _stable_id("sig", self.release_sha, signal_id)
        correlation = self.correlations.get_by_signal(canonical_signal_id)
        if correlation is None:
            raise LiveBridgeError("signal_correlation_missing")
        row: Mapping[str, Any] | None = self.executions.get(signal_id)
        if row is None:
            # Compatibility is recovery-only for executions created before the
            # canonical State repository existed.
            conn = connect_compatibility(self.compatibility_db_path, timeout=20)
            conn.row_factory = sqlite3.Row
            try:
                legacy = conn.execute(
                    "SELECT * FROM trade_executions WHERE signal_id=? AND mode='live'",
                    (int(signal_id),),
                ).fetchone()
                row = dict(legacy) if legacy is not None else None
            finally:
                conn.close()
        if row is None:
            return {"status": "NO_LIVE_EXECUTION", "candidate_id": correlation["candidate_id"]}
        execution_id = derived_id("execution", correlation["candidate_id"])
        self.correlations.attach(correlation["candidate_id"], "execution_id", execution_id)
        state_execution = self.executions.get(signal_id)
        if state_execution is not None:
            self.executions.bind_identity(
                signal_id, execution_id=execution_id,
                candidate_id=correlation["candidate_id"],
            )
        status = str(row["status"] or "").upper()
        result = {
            "status": status,
            "candidate_id": correlation["candidate_id"],
            "execution_id": execution_id,
            "position_confirmed": False,
        }
        if status not in self._POSITION_STATUSES:
            return result
        if not row["entry_order_id"] or not row["stop_order_id"] or float(row["quantity"] or 0) <= 0:
            raise LiveBridgeError("protected_position_evidence_incomplete")
        position_id = derived_id("position", execution_id, str(row["symbol"]).upper())
        self.correlations.attach(correlation["candidate_id"], "position_id", position_id)
        if state_execution is not None:
            self.executions.bind_identity(signal_id, position_id=position_id)
        self.memory.mark_executed(correlation["candidate_id"], signal_id=canonical_signal_id)
        result.update(position_confirmed=True, position_id=position_id)
        return result

    def sync_confirmed_positions(self) -> list[dict[str, Any]]:
        """Recover V3 correlations for already protected positions after restart."""
        state_rows = self.executions.requiring_reconciliation(tuple(self._POSITION_STATUSES))
        signal_ids = [int(row["signal_id"]) for row in state_rows]
        if not signal_ids:
            # One-way migration fallback for pre-State protected positions.
            conn = connect_compatibility(self.compatibility_db_path, timeout=20)
            try:
                table = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trade_executions'"
                ).fetchone()
                if table is None:
                    return []
                signal_ids = [
                    int(row[0]) for row in conn.execute(
                        """SELECT signal_id FROM trade_executions
                             WHERE mode='live' AND status IN ('PROTECTED','PROTECTED_NO_TP')"""
                    ).fetchall()
                ]
            finally:
                conn.close()
        results: list[dict[str, Any]] = []
        for signal_id in signal_ids:
            if self.correlations.get_by_signal(
                _stable_id("sig", self.release_sha, signal_id)
            ) is None:
                # Pre-V3 trades remain managed and reconciled, but are not
                # silently relabelled as V3 learning evidence.
                continue
            results.append(self.sync_execution(signal_id))
        return results

    def execution_accounting_snapshot(self, signal_id: int) -> dict[str, Any]:
        """Return immutable State geometry plus Live Memory strategy metadata."""
        execution = self.executions.get(signal_id)
        if execution is None or str(execution.get("mode") or "") != "live":
            raise LiveBridgeError("canonical_live_execution_missing")
        candidate_id = str(execution.get("candidate_id") or "")
        if not is_id(candidate_id, "candidate"):
            raise LiveBridgeError("execution_candidate_identity_missing")
        metadata = self.memory.candidate_metadata(candidate_id)
        return {
            **execution,
            "strategy": str(metadata["strategy"]),
            "initial_sl": execution.get("sl"),
            "terminal_tp": execution.get("tp3") or execution.get("tp2") or execution.get("tp1"),
        }

    def record_confirmed_outcome(
        self,
        signal_id: int,
        accounting: Mapping[str, Any],
    ) -> str:
        """Persist only complete Binance accounting; UNKNOWN funding is a hard stop."""
        correlation = self.correlations.get_by_signal(
            _stable_id("sig", self.release_sha, signal_id)
        )
        if correlation is None or not correlation.get("execution_id") or not correlation.get("position_id"):
            raise LiveBridgeError("trade_correlation_incomplete")
        if str(accounting.get("status") or "").upper() != "CLOSED":
            raise LiveBridgeError("confirmed_closed_fills_required")
        required = ("entry", "exit_price", "net_r", "gross_r", "fees_quote", "exit_time")
        if any(accounting.get(name) is None for name in required):
            raise LiveBridgeError("confirmed_accounting_incomplete")
        if accounting.get("funding_quote") is None:
            raise LiveBridgeError("funding_unresolved")
        candidate_metadata = self.memory.candidate_metadata(str(correlation["candidate_id"]))
        outcome_id = _stable_id("out", self.release_sha, correlation["execution_id"])
        closed_at = datetime.fromtimestamp(float(accounting["exit_time"]) / 1000, tz=timezone.utc)
        outcome = TradeOutcome(
            outcome_id=outcome_id,
            position_id=str(correlation["position_id"]),
            weighted_entry=float(accounting["entry"]),
            weighted_exit=float(accounting["exit_price"]),
            net_r=float(accounting["net_r"]),
            fees=float(accounting["fees_quote"]),
            funding=float(accounting["funding_quote"]),
            closed_at=closed_at,
        )
        self.memory.record_outcome(
            outcome,
            candidate_id=str(correlation["candidate_id"]),
            execution_id=str(correlation["execution_id"]),
            strategy=str(candidate_metadata["strategy"]),
            symbol=str(candidate_metadata["symbol"]),
            direction=Direction.normalize(candidate_metadata["direction"]).value,
            release_sha=self.release_sha,
            confirmed_position=True,
            gross_r=float(accounting["gross_r"]),
            context={
                key: accounting.get(key)
                for key in (
                    "mfe_r", "mae_r", "duration_seconds", "targets_reached",
                    "exit_reason", "accounting_basis",
                )
                if accounting.get(key) is not None
            },
        )
        self.correlations.attach(correlation["candidate_id"], "outcome_id", outcome_id)
        return outcome_id

    def sync_confirmed_outcomes(
        self,
        accounting_loader: Callable[[int], Mapping[str, Any]],
    ) -> list[str]:
        """Append fully accounted outcomes; leave incomplete records pending."""
        recorded: list[str] = []
        for correlation in self.correlations.pending_outcomes():
            try:
                state_execution = self.executions.get_by_candidate(correlation["candidate_id"])
                signal_id = (
                    int(state_execution["signal_id"]) if state_execution is not None
                    else self._legacy_signal_id(correlation["candidate_id"])
                )
                accounting = accounting_loader(signal_id)
                recorded.append(self.record_confirmed_outcome(signal_id, accounting))
            except (LiveBridgeError, LiveMemoryError, TypeError, ValueError):
                continue
        return recorded

    def _legacy_signal_id(self, candidate_id: str) -> int:
        """Resolve the compatibility row only at the migration boundary."""
        conn = connect_compatibility(self.compatibility_db_path, timeout=20)
        try:
            rows = conn.execute("SELECT id FROM signals ORDER BY id").fetchall()
        finally:
            conn.close()
        for row in rows:
            signal_id = int(row[0])
            if _candidate_id(self.release_sha, signal_id) == str(candidate_id):
                return signal_id
        raise LiveBridgeError("legacy_signal_mapping_missing")


__all__ = ["LiveBridgeError", "LiveLearningBridge"]
