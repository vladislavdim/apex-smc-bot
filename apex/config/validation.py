"""Fail-closed validation of capital-affecting V3 configuration."""

from __future__ import annotations

from dataclasses import dataclass

from apex.domain.enums import Strategy

from .settings import ApexConfig


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]


def validate_config(config: ApexConfig, *, raise_on_error: bool = False) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    if config.execution.mode not in {"paper", "live"}:
        errors.append("EXECUTION_MODE_INVALID")
    if not 0 < config.risk.risk_pct <= config.risk.max_risk_pct:
        errors.append("RISK_OUTSIDE_HARD_CAP")
    if config.execution.leverage < 1 or config.execution.leverage > 5:
        errors.append("LEVERAGE_OUTSIDE_HARD_CAP")
    if not 0 <= config.execution.fee_bps <= 100:
        errors.append("EXECUTION_FEE_INVALID")
    if not 0.1 <= config.execution.tp1_fraction <= 0.9:
        errors.append("TP1_FRACTION_INVALID")
    if not 3 <= config.execution.timeout_seconds <= 10:
        errors.append("EXECUTION_TIMEOUT_INVALID")
    if not 1 <= config.execution.retries <= 4:
        errors.append("EXECUTION_RETRIES_INVALID")
    if not 0 <= config.execution.min_groq_confidence <= 1:
        errors.append("EXECUTION_GROQ_CONFIDENCE_INVALID")
    if config.risk.max_total_risk_pct < config.risk.risk_pct:
        errors.append("PORTFOLIO_RISK_BELOW_TRADE_RISK")
    if not 0 < config.risk.max_same_side_risk_pct <= config.risk.max_total_risk_pct:
        errors.append("SAME_SIDE_RISK_INVALID")
    if config.risk.max_daily_loss_pct <= 0:
        errors.append("DAILY_LOSS_LIMIT_INVALID")
    if config.risk.max_open_positions < 1:
        errors.append("MAX_OPEN_POSITIONS_INVALID")
    if config.execution.enabled and config.execution.mode == "live":
        if not config.execution.binance_api_key or not config.execution.binance_api_secret:
            errors.append("BINANCE_CREDENTIALS_MISSING")
        if config.execution.live_confirmation != "ENABLE_LIVE_BINANCE_FUTURES":
            errors.append("LIVE_CONFIRMATION_MISSING")
    for name, path in (
        ("STATE_DB_PATH", config.database.state_db_path),
        ("MEMORY_DB_PATH", config.database.memory_db_path),
    ):
        if not str(path).strip():
            errors.append(name + "_EMPTY")
    ratios = (
        config.operational.memory_watch_ratio,
        config.operational.memory_degraded_ratio,
        config.operational.memory_stop_ratio,
    )
    if not (0 < ratios[0] < ratios[1] < ratios[2] <= 1):
        errors.append("MEMORY_THRESHOLDS_INVALID")
    if config.operational.restart_limit_1h < 1:
        errors.append("RESTART_LIMIT_INVALID")
    if config.operational.event_loop_lag_sla_ms < 50:
        errors.append("EVENT_LOOP_SLA_INVALID")
    if not 0.1 <= config.operational.gate_timeout_seconds <= 60:
        errors.append("GATE_TIMEOUT_INVALID")
    if not 5 <= config.operational.execution_reconcile_seconds <= 300:
        errors.append("RECONCILE_INTERVAL_INVALID")
    if not 30 <= config.operational.runtime_lease_ttl_seconds <= 300:
        errors.append("LEASE_TTL_INVALID")
    if not 1 <= config.operational.fast_concurrency <= 8:
        errors.append("FAST_CONCURRENCY_INVALID")
    if not 7 <= config.operational.state_telemetry_retention_days <= 365:
        errors.append("STATE_RETENTION_INVALID")
    if not 30 <= config.operational.memory_context_retention_days <= 3650:
        errors.append("MEMORY_RETENTION_INVALID")
    if not 30 <= config.operational.resolved_incident_retention_days <= 3650:
        errors.append("INCIDENT_RETENTION_INVALID")
    if not config.gate_primary or not config.binance_execution_only:
        errors.append("SOURCE_AUTHORITY_INVALID")
    if not config.integrations.gate_api_base.startswith("https://"):
        errors.append("GATE_API_BASE_INVALID")
    if not 0 <= config.integrations.groq_min_approval_confidence <= 1:
        errors.append("GROQ_CONFIDENCE_INVALID")
    expected_strategies = {strategy.value for strategy in Strategy}
    configured_versions = dict(config.strategies.versions)
    if set(configured_versions) != expected_strategies or any(
        not str(version).strip() for version in configured_versions.values()
    ):
        errors.append("STRATEGY_VERSIONS_INCOMPLETE")
    if config.strategies.minimum_rr != 2.0:
        errors.append("MINIMUM_RR_PARITY_VIOLATION")
    if config.strategies.snapshot_activation_requested and (
        not config.strategies.parity_corpus_path
        or not config.strategies.parity_verdict_path
    ):
        errors.append("SNAPSHOT_STRATEGY_PROOF_PATHS_MISSING")
    if bool(config.integrations.stats_ingest_url) != bool(config.integrations.stats_ingest_token):
        errors.append("STATS_INGEST_CONFIG_INCOMPLETE")
    if config.integrations.runtime_lease_url and not config.integrations.stats_ingest_token:
        errors.append("RUNTIME_LEASE_TOKEN_MISSING")
    if not 1 <= config.runtime.port <= 65535:
        errors.append("PORT_INVALID")
    if not 0 <= config.runtime.rollout_settle_seconds <= 120:
        errors.append("ROLLOUT_SETTLE_INVALID")
    if config.execution.enabled and not config.integrations.telegram_token:
        warnings.append("TELEGRAM_TOKEN_MISSING")
    result = ValidationResult(not errors, tuple(errors), tuple(warnings))
    if errors and raise_on_error:
        raise ConfigError(";".join(errors))
    return result


__all__ = ["ConfigError", "ValidationResult", "validate_config"]
