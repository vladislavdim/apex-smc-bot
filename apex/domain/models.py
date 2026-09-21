"""Immutable V3 domain models.

The legacy system still passes dictionaries internally.  These models define
the canonical destination and are intentionally dependency-free.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
from typing import Any, Mapping

from .enums import Decision, Direction, Strategy
from .ids import is_id
from .reason_codes import validate_reason_code, validate_reason_codes


@dataclass(frozen=True)
class VersionManifest:
    release_sha: str
    strategy_version: str
    manager_version: str
    risk_version: str
    regime_version: str
    groq_prompt_version: str
    schema_version: str

    def __post_init__(self) -> None:
        if any(not str(value).strip() for value in (
            self.release_sha, self.strategy_version, self.manager_version,
            self.risk_version, self.regime_version, self.groq_prompt_version,
            self.schema_version,
        )):
            raise ValueError("all version manifest fields are required")


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    symbol: str
    strategy: Strategy
    direction: Direction
    entry: float
    initial_sl: float
    tp1: float
    tp2: float
    tp3: float | None
    rr: float
    snapshot_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    versions: VersionManifest | None = None

    def __post_init__(self) -> None:
        if not is_id(self.candidate_id, "candidate") or not is_id(self.snapshot_id, "snapshot"):
            raise ValueError("candidate_id and snapshot_id must be canonical")
        if not self.symbol:
            raise ValueError("symbol is required")
        values = (self.entry, self.initial_sl, self.tp1, self.tp2, self.rr)
        if any(not isinstance(value, (int, float)) or not math.isfinite(float(value)) for value in values):
            raise TypeError("candidate geometry must be numeric")
        if self.tp3 is not None:
            if not math.isfinite(float(self.tp3)):
                raise TypeError("candidate tp3 must be finite")
        if self.created_at.tzinfo is None:
            raise ValueError("candidate created_at must be timezone-aware")


@dataclass(frozen=True)
class StrategyCheck:
    check_id: str
    category: str
    actual_value: Any
    required_value: Any
    outcome: str
    reason_code: str

    def __post_init__(self) -> None:
        if self.reason_code:
            validate_reason_code(self.reason_code)


@dataclass(frozen=True)
class StrategyResult:
    outcome: str
    reason_codes: tuple[str, ...]
    checks: tuple[StrategyCheck, ...]
    candidate: Candidate | None = None
    context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_reason_codes(self.reason_codes)


@dataclass(frozen=True)
class MarketRegime:
    direction: str
    volatility: str
    phase: str


@dataclass(frozen=True)
class MarketSnapshot:
    snapshot_id: str
    symbol: str
    as_of: datetime
    candles: Mapping[str, tuple[Mapping[str, Any], ...]]
    structure: Mapping[str, Any]
    levels: tuple[Mapping[str, Any], ...]
    regime: MarketRegime
    volume: Mapping[str, Any]
    derivatives_context: Mapping[str, Any]
    microstructure_context: Mapping[str, Any]
    market_context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not is_id(self.snapshot_id, "snapshot"):
            raise ValueError("snapshot_id must be canonical")
        if self.as_of.tzinfo is None:
            raise ValueError("snapshot as_of must be timezone-aware")


@dataclass(frozen=True)
class SetupEvidence:
    complete: bool
    domains: Mapping[str, str]
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        validate_reason_codes(self.reason_codes)


@dataclass(frozen=True)
class GroqReview:
    decision: Decision
    confidence: float
    reason_codes: tuple[str, ...]
    short_summary: str

    def __post_init__(self) -> None:
        validate_reason_codes(self.reason_codes)


@dataclass(frozen=True)
class RiskDecision:
    decision: str
    base_risk_pct: float
    final_risk_pct: float
    quantity: float
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        validate_reason_codes(self.reason_codes)


@dataclass(frozen=True)
class ExecutionPlan:
    execution_id: str
    candidate_id: str
    symbol: str
    direction: Direction
    entry: float
    sl: float
    targets: tuple[float, ...]
    quantity: float

    def __post_init__(self) -> None:
        if not is_id(self.execution_id, "execution"):
            raise ValueError("execution_id must be canonical")
        if not is_id(self.candidate_id, "candidate"):
            raise ValueError("candidate_id must be canonical")


@dataclass(frozen=True)
class ExecutionFill:
    execution_id: str
    order_id: str
    price: float
    quantity: float
    commission: float
    filled_at: datetime

    def __post_init__(self) -> None:
        if not is_id(self.execution_id, "execution"):
            raise ValueError("execution_id must be canonical")
        if self.filled_at.tzinfo is None:
            raise ValueError("fill timestamp must be timezone-aware")


@dataclass(frozen=True)
class PositionState:
    position_id: str
    execution_id: str
    symbol: str
    direction: Direction
    quantity: float
    average_entry: float
    confirmed_stop: float
    status: str

    def __post_init__(self) -> None:
        if not is_id(self.position_id, "position"):
            raise ValueError("position_id must be canonical")
        if not is_id(self.execution_id, "execution"):
            raise ValueError("execution_id must be canonical")


@dataclass(frozen=True)
class ManagerDecision:
    manager_event_id: str
    position_id: str
    action: str
    eligible_actions: tuple[str, ...]
    proposed_stop: float | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not is_id(self.manager_event_id, "manager_event"):
            raise ValueError("manager_event_id must be canonical")
        if not is_id(self.position_id, "position"):
            raise ValueError("position_id must be canonical")
        validate_reason_codes(self.reason_codes)


@dataclass(frozen=True)
class TradeOutcome:
    outcome_id: str
    position_id: str
    weighted_entry: float
    weighted_exit: float
    net_r: float
    fees: float
    funding: float
    closed_at: datetime

    def __post_init__(self) -> None:
        if not is_id(self.outcome_id, "outcome"):
            raise ValueError("outcome_id must be canonical")
        if not is_id(self.position_id, "position"):
            raise ValueError("position_id must be canonical")
        if self.closed_at.tzinfo is None:
            raise ValueError("outcome closed_at must be timezone-aware")
