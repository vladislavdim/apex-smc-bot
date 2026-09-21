"""Single environment parsing boundary for APEX V3."""

from __future__ import annotations

import os
import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Mapping

from .versions import STRATEGY_VERSIONS


_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


class ConfigParseError(ValueError):
    """Raised when an explicitly configured value cannot be parsed safely."""


def _bool(env: Mapping[str, str], key: str, default: bool = False) -> bool:
    value = env.get(key)
    if value is None or str(value).strip() == "":
        return default
    normalized = str(value).strip().lower()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    raise ConfigParseError(f"{key}_INVALID")


def _float(env: Mapping[str, str], key: str, default: float) -> float:
    value = env.get(key)
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ConfigParseError(f"{key}_INVALID") from None


def _int(env: Mapping[str, str], key: str, default: int) -> int:
    value = env.get(key)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ConfigParseError(f"{key}_INVALID") from None


def _csv_ints(env: Mapping[str, str], key: str) -> tuple[int, ...]:
    value = str(env.get(key, "")).strip()
    if not value:
        return ()
    try:
        return tuple(int(item.strip()) for item in value.split(",") if item.strip())
    except ValueError:
        raise ConfigParseError(f"{key}_INVALID") from None


def _csv_strings(env: Mapping[str, str], key: str, default: str = "") -> tuple[str, ...]:
    value = str(env.get(key, default)).strip()
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


@dataclass(frozen=True)
class ExecutionSettings:
    enabled: bool
    mode: str
    live_confirmation: str = field(repr=False)
    binance_api_key: str = field(repr=False)
    binance_api_secret: str = field(repr=False)
    leverage: int = 5
    kill_switch: bool = False
    paper_balance_usdt: float = 1000.0
    fee_bps: float = 10.0
    tp1_fraction: float = 0.5
    binance_base_url: str = "https://fapi.binance.com"
    timeout_seconds: float = 8.0
    retries: int = 3
    min_groq_confidence: float = 0.70


@dataclass(frozen=True)
class RiskSettings:
    risk_pct: float
    max_risk_pct: float
    max_total_risk_pct: float
    max_same_side_risk_pct: float
    max_daily_loss_pct: float
    max_open_positions: int


@dataclass(frozen=True)
class DatabaseSettings:
    compatibility_db_path: str
    state_db_path: str
    memory_db_path: str


@dataclass(frozen=True)
class OperationalSettings:
    restart_limit_1h: int
    memory_watch_ratio: float
    memory_degraded_ratio: float
    memory_stop_ratio: float
    event_loop_lag_sla_ms: int
    gate_timeout_seconds: float
    execution_reconcile_seconds: int
    runtime_lease_ttl_seconds: int
    fast_concurrency: int
    state_telemetry_retention_days: int
    memory_context_retention_days: int
    resolved_incident_retention_days: int
    active_pair_limit: int
    market_data_providers: tuple[str, ...]
    memory_limit_bytes: int


@dataclass(frozen=True)
class IntegrationSettings:
    gate_api_base: str
    gate_ws_enabled: bool
    groq_model: str
    groq_fallback_models: tuple[str, ...]
    groq_min_approval_confidence: float
    groq_api_key: str = field(repr=False)
    telegram_token: str = field(repr=False)
    telegram_admin_id: str = field(repr=False)
    telegram_admin_ids: tuple[int, ...]
    signal_channel_main: int
    signal_channel_swing: int
    swing_thread_id: int
    fast_thread_id: int
    groq_api_keys: tuple[str, ...] = field(repr=False)
    tavily_api_key: str = field(repr=False)
    twelvedata_api_key: str = field(repr=False)
    mobula_api_key: str = field(repr=False)
    coinalyze_api_key: str = field(repr=False)
    lunarcrush_api_key: str = field(repr=False)
    coinglass_api_key: str = field(repr=False)
    santiment_api_key: str = field(repr=False)
    coinalyze_symbol_map_json: str
    coinmetrics_asset_map_json: str
    crypto_monitor_api_url: str
    crypto_monitor_api_key: str = field(repr=False)
    oli_api_url: str
    oli_api_key: str = field(repr=False)
    oli_tracked_addresses_json: str = field(repr=False)
    asset_contract_map_json: str
    whale_tracker_api_url: str
    whale_tracker_api_key: str = field(repr=False)
    gate_depth_symbols: tuple[str, ...]
    macro_calendar_url: str
    external_source_plan_json: str
    legacy_strategy_groq: bool
    stats_ingest_url: str = ""
    stats_ingest_token: str = field(default="", repr=False)
    runtime_lease_url: str = ""
    stats_url: str = ""
    webhook_url: str = ""
    github_repo: str = ""
    github_token: str = field(default="", repr=False)
    backup_branch: str = "brain-backups"
    github_file: str = "bot.py"


@dataclass(frozen=True)
class RuntimeSettings:
    release_sha: str
    instance_id: str
    render: bool
    port: int
    rollout_settle_seconds: int
    deploy_id: str


@dataclass(frozen=True)
class StrategySettings:
    """Versioned production strategy boundary; values remain legacy parity."""

    minimum_rr: float
    versions: tuple[tuple[str, str], ...]
    snapshot_activation_requested: bool = False
    parity_corpus_path: str = ""
    parity_verdict_path: str = ""

    def version_for(self, strategy: str) -> str:
        return dict(self.versions)[str(strategy).upper()]

    def manifest_hash(self) -> str:
        # Import lazily so the configuration layer stays free of strategy
        # initialization side effects.
        from apex.strategies.data_contracts import CONTRACTS
        from apex.strategies.specifications import SPECIFICATIONS

        payload = {
            "minimum_rr": self.minimum_rr,
            "versions": dict(self.versions),
            "specifications": {
                strategy.value: {
                    **asdict(specification),
                    "strategy": strategy.value,
                }
                for strategy, specification in sorted(
                    SPECIFICATIONS.items(), key=lambda item: item[0].value
                )
            },
            "data_contracts": {
                strategy.value: {
                    name: relevance.value
                    for name, relevance in sorted(contract.fields.items())
                }
                for strategy, contract in sorted(
                    CONTRACTS.items(), key=lambda item: item[0].value
                )
            },
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ApexConfig:
    execution: ExecutionSettings
    risk: RiskSettings
    database: DatabaseSettings
    operational: OperationalSettings
    integrations: IntegrationSettings
    runtime: RuntimeSettings
    strategies: StrategySettings
    gate_primary: bool = True
    binance_execution_only: bool = True

    def safe_config_hash(self) -> str:
        """Hash non-secret production settings for the release manifest."""
        payload = asdict(self)
        integrations = payload["integrations"]
        for name in (
            "groq_api_key", "telegram_token", "telegram_admin_id",
            "telegram_admin_ids",
            "groq_api_keys", "tavily_api_key", "twelvedata_api_key",
            "mobula_api_key", "coinalyze_api_key", "lunarcrush_api_key",
            "coinglass_api_key", "santiment_api_key", "stats_ingest_token",
            "crypto_monitor_api_key", "oli_api_key", "oli_tracked_addresses_json",
            "whale_tracker_api_key", "github_token",
        ):
            integrations[name] = bool(integrations.get(name))
        execution = payload["execution"]
        for name in ("binance_api_key", "binance_api_secret", "live_confirmation"):
            execution[name] = bool(execution.get(name))
        # Instance identity is release evidence, not configuration. Keeping it
        # in this hash would make identical settings differ after every restart.
        payload["runtime"].pop("instance_id", None)
        payload["runtime"].pop("release_sha", None)
        payload["runtime"].pop("deploy_id", None)
        payload["strategy_manifest_hash"] = self.strategies.manifest_hash()
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ApexConfig":
        source = os.environ if env is None else env
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        legacy = str(source.get("APEX_COMPAT_DB_PATH") or os.path.join(root, "brain.db"))
        return cls(
            execution=ExecutionSettings(
                enabled=_bool(source, "AUTO_TRADING_ENABLED"),
                mode=str(source.get("AUTO_TRADING_MODE", "paper")).strip().lower(),
                live_confirmation=str(source.get("AUTO_TRADING_LIVE_CONFIRM", "")),
                binance_api_key=str(source.get("BINANCE_API_KEY", "")),
                binance_api_secret=str(source.get("BINANCE_API_SECRET", "")),
                leverage=_int(source, "AUTO_TRADING_LEVERAGE", 5),
                kill_switch=_bool(source, "AUTO_TRADING_KILL_SWITCH", False),
                paper_balance_usdt=_float(source, "AUTO_TRADING_PAPER_BALANCE_USDT", 1000.0),
                fee_bps=_float(source, "AUTO_TRADING_FEE_BPS", 10.0),
                tp1_fraction=_float(source, "AUTO_TRADING_TP1_FRACTION", 0.5),
                binance_base_url=str(source.get("BINANCE_FUTURES_API_URL", "https://fapi.binance.com")).strip().rstrip("/"),
                timeout_seconds=_float(source, "AUTO_TRADING_TIMEOUT_SECONDS", 8.0),
                retries=_int(source, "AUTO_TRADING_RETRIES", 3),
                min_groq_confidence=_float(
                    source, "AUTO_TRADING_MIN_GROQ_CONFIDENCE",
                    _float(source, "GROQ_MIN_APPROVAL_CONFIDENCE", 0.70),
                ),
            ),
            risk=RiskSettings(
                risk_pct=_float(source, "AUTO_TRADING_RISK_PCT", 0.5),
                max_risk_pct=_float(source, "APEX_MAX_RISK_PCT", 1.0),
                max_total_risk_pct=_float(source, "APEX_MAX_TOTAL_RISK_PCT", 3.0),
                max_same_side_risk_pct=_float(source, "APEX_MAX_SAME_SIDE_RISK_PCT", 2.0),
                max_daily_loss_pct=_float(source, "AUTO_TRADING_MAX_DAILY_LOSS_PCT", 2.0),
                max_open_positions=_int(source, "AUTO_TRADING_MAX_OPEN_POSITIONS", 3),
            ),
            database=DatabaseSettings(
                compatibility_db_path=legacy,
                state_db_path=str(source.get("APEX_STATE_DB_PATH") or os.path.join(root, "apex_state.db")),
                memory_db_path=str(source.get("APEX_MEMORY_DB_PATH") or os.path.join(root, "apex_memory.db")),
            ),
            operational=OperationalSettings(
                restart_limit_1h=_int(source, "APEX_RESTART_LIMIT_1H", 4),
                memory_watch_ratio=_float(source, "APEX_MEMORY_WATCH_RATIO", 0.65),
                memory_degraded_ratio=_float(source, "APEX_MEMORY_DEGRADED_RATIO", 0.75),
                memory_stop_ratio=_float(source, "APEX_MEMORY_STOP_RATIO", 0.85),
                event_loop_lag_sla_ms=_int(source, "APEX_EVENT_LOOP_LAG_SLA_MS", 500),
                gate_timeout_seconds=_float(source, "APEX_GATE_TIMEOUT_SECONDS", 8.0),
                execution_reconcile_seconds=_int(source, "AUTO_TRADING_RECONCILE_SECONDS", 30),
                runtime_lease_ttl_seconds=_int(source, "APEX_RUNTIME_LEASE_TTL_SECONDS", 60),
                fast_concurrency=_int(source, "APEX_FAST_CONCURRENCY", 6),
                state_telemetry_retention_days=_int(source, "APEX_STATE_TELEMETRY_RETENTION_DAYS", 30),
                memory_context_retention_days=_int(source, "APEX_MEMORY_CONTEXT_RETENTION_DAYS", 365),
                resolved_incident_retention_days=_int(source, "APEX_RESOLVED_INCIDENT_RETENTION_DAYS", 365),
                active_pair_limit=max(20, min(120, _int(source, "APEX_ACTIVE_PAIR_LIMIT", 80))),
                market_data_providers=tuple(
                    provider.strip().lower()
                    for provider in str(source.get("APEX_MARKET_DATA_PROVIDERS", "gate")).split(",")
                    if provider.strip()
                ),
                memory_limit_bytes=max(0, _int(source, "APEX_MEMORY_LIMIT_BYTES", 0)),
            ),
            integrations=IntegrationSettings(
                gate_api_base=str(source.get("APEX_GATE_API_BASE", "https://api.gateio.ws/api/v4")).strip(),
                gate_ws_enabled=_bool(source, "APEX_GATE_WS_ENABLED", True),
                groq_model=str(source.get("GROQ_MODEL", "")).strip(),
                groq_fallback_models=tuple(
                    model.strip() for model in str(source.get("GROQ_FALLBACK_MODELS", "")).split(",")
                    if model.strip()
                ),
                groq_min_approval_confidence=_float(source, "GROQ_MIN_APPROVAL_CONFIDENCE", 0.65),
                groq_api_key=str(source.get("GROQ_API_KEY", "")).strip(),
                telegram_token=str(source.get("TELEGRAM_TOKEN", "")).strip(),
                telegram_admin_id=str(source.get("ADMIN_ID", "")).strip(),
                telegram_admin_ids=_csv_ints(source, "ADMIN_ID"),
                signal_channel_main=_int(source, "SIGNAL_CHANNEL_MAIN", -1003614593530),
                signal_channel_swing=_int(source, "SIGNAL_CHANNEL_ID", -1003122576951),
                swing_thread_id=_int(source, "SWING_THREAD_ID", 262),
                fast_thread_id=_int(source, "FAST_DEAL_THREAD_ID", 264),
                groq_api_keys=tuple(
                    key for key in (
                        str(source.get("GROQ_API_KEY", "")).strip(),
                        *(str(source.get(f"GROQ_API_KEY_{index}", "")).strip()
                          for index in range(2, 20)),
                    ) if key
                ),
                tavily_api_key=str(source.get("TAVILY_API_KEY", "")).strip(),
                twelvedata_api_key=str(source.get("TWELVEDATA_API_KEY", "")).strip(),
                mobula_api_key=str(source.get("MOBULA_API_KEY", "")).strip(),
                coinalyze_api_key=str(source.get("COINALYZE_API_KEY", "")).strip(),
                lunarcrush_api_key=str(source.get("LUNARCRUSH_API_KEY", "")).strip(),
                coinglass_api_key=str(source.get("COINGLASS_API_KEY", "")).strip(),
                santiment_api_key=str(source.get("SANTIMENT_API_KEY", "")).strip(),
                coinalyze_symbol_map_json=str(source.get("COINALYZE_SYMBOL_MAP_JSON", "{}")),
                coinmetrics_asset_map_json=str(source.get("COINMETRICS_ASSET_MAP_JSON", "{}")),
                crypto_monitor_api_url=str(source.get("CRYPTO_MONITOR_API_URL", "")).strip().rstrip("/"),
                crypto_monitor_api_key=str(source.get("CRYPTO_MONITOR_API_KEY", "")).strip(),
                oli_api_url=str(source.get("OLI_API_URL", "https://api.openlabelsinitiative.org")).strip().rstrip("/"),
                oli_api_key=str(source.get("OLI_API_KEY", "")).strip(),
                oli_tracked_addresses_json=str(source.get("OLI_TRACKED_ADDRESSES_JSON", "{}")),
                asset_contract_map_json=str(source.get("ASSET_CONTRACT_MAP_JSON", "{}")),
                whale_tracker_api_url=str(source.get("WHALE_TRACKER_API_URL", "")).strip().rstrip("/"),
                whale_tracker_api_key=str(source.get("WHALE_TRACKER_API_KEY", "")).strip(),
                gate_depth_symbols=_csv_strings(source, "APEX_GATE_DEPTH_SYMBOLS", "BTCUSDT"),
                macro_calendar_url=str(source.get(
                    "MACRO_CALENDAR_URL",
                    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                )).strip(),
                external_source_plan_json=str(source.get("APEX_EXTERNAL_SOURCE_PLAN_JSON", "")).strip(),
                legacy_strategy_groq=_bool(source, "LEGACY_STRATEGY_GROQ", False),
                stats_ingest_url=str(source.get("APEX_STATS_INGEST_URL", "")).strip(),
                stats_ingest_token=str(source.get("APEX_STATS_INGEST_TOKEN", "")).strip(),
                runtime_lease_url=str(source.get("APEX_RUNTIME_LEASE_URL", "")).strip(),
                stats_url=str(source.get("APEX_STATS_URL", "")).strip(),
                webhook_url=str(source.get("WEBHOOK_URL", "")).strip(),
                github_repo=str(source.get("GITHUB_REPO", "")).strip(),
                github_token=str(source.get("GITHUB_TOKEN", "")).strip(),
                backup_branch=str(source.get("BRAIN_BACKUP_BRANCH", "brain-backups")).strip(),
                github_file=str(source.get("GITHUB_FILE", "bot.py")).strip(),
            ),
            runtime=RuntimeSettings(
                release_sha=str(
                    source.get("RENDER_GIT_COMMIT") or source.get("GIT_COMMIT") or "unknown"
                ).strip() or "unknown",
                instance_id=str(
                    source.get("RENDER_INSTANCE_ID") or source.get("HOSTNAME") or "unknown"
                ).strip(),
                render=_bool(source, "RENDER", bool(source.get("RENDER_INSTANCE_ID"))),
                port=_int(source, "PORT", 10000),
                rollout_settle_seconds=_int(
                    source, "BRAIN_ROLLOUT_SETTLE_SECONDS",
                    65 if (source.get("RENDER") or source.get("RENDER_INSTANCE_ID")) else 0,
                ),
                deploy_id=str(
                    source.get("RENDER_DEPLOY_ID") or source.get("DEPLOY_ID")
                    or source.get("RENDER_GIT_COMMIT") or source.get("GIT_COMMIT") or "unknown"
                ).strip(),
            ),
            strategies=StrategySettings(
                minimum_rr=_float(source, "APEX_MINIMUM_RR", 2.0),
                versions=tuple(sorted(STRATEGY_VERSIONS.items())),
                snapshot_activation_requested=_bool(
                    source, "APEX_SNAPSHOT_STRATEGIES_ENABLED", False,
                ),
                parity_corpus_path=str(source.get(
                    "APEX_STRATEGY_PARITY_CORPUS",
                    os.path.join(root, "tests", "fixtures", "apex_v3_strategy_parity"),
                )).strip(),
                parity_verdict_path=str(source.get(
                    "APEX_STRATEGY_PARITY_VERDICT",
                    os.path.join(root, "tests", "fixtures", "apex_v3_strategy_parity_verdict.json"),
                )).strip(),
            ),
        )


__all__ = [
    "ApexConfig", "ConfigParseError", "DatabaseSettings", "ExecutionSettings",
    "IntegrationSettings", "OperationalSettings", "RiskSettings", "RuntimeSettings",
    "StrategySettings",
]
