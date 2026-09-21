"""Central production runtime authority for readiness and entry admission."""

from __future__ import annotations

import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from apex.domain.enums import ComponentState, RuntimeStatus
from apex.config.settings import ApexConfig


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class ComponentHealth:
    state: ComponentState
    updated_at: str
    detail: str = ""
    required: bool = True


class RuntimeSupervisor:
    """One answer to whether APEX may accept new capital risk.

    The supervisor is activated by the production launcher.  Imports and unit
    tests that call legacy functions directly remain compatible until their
    module is migrated explicitly.
    """

    REQUIRED_COMPONENTS = frozenset({
        "config",
        "state_db",
        "binance_reconciliation",
        "manager_reconciliation",
        "scheduler",
        "gate",
        "market_data",
        "telegram",
        "backup",
        "instance_fencing",
    })
    COMPONENTS = frozenset({
        "worker", "event_loop", "scheduler", "gate", "market_data",
        "scanner_fast", "scanner_mtf", "scanner_zone", "scanner_swing",
        "scanner_wyckoff", "state_db", "memory_db", "binance_reconciliation",
        "risk_engine", "manager", "manager_reconciliation", "groq", "telegram",
        "backup", "dashboard_telemetry", "instance_fencing", "restart_guard",
        "config", "cpu", "memory", "strategy_activation",
    })

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._active = False
        self._status = RuntimeStatus.STARTING
        self._started_at = _utc_now()
        self._release_sha = "unknown"
        self._instance_id = "unknown"
        self._reason_codes: set[str] = set()
        self._components: dict[str, ComponentHealth] = {}
        self._lease_generation: int | None = None
        self._lease_expires_at: datetime | None = None

    def activate(self, *, release_sha: str = "", instance_id: str = "") -> None:
        config = ApexConfig.from_env()
        with self._lock:
            self._active = True
            self._status = RuntimeStatus.STARTING
            self._started_at = _utc_now()
            self._release_sha = (release_sha or config.runtime.release_sha).strip()
            self._instance_id = (instance_id or config.runtime.instance_id).strip()
            self._reason_codes.clear()
            self._components = {
                name: ComponentHealth(
                    ComponentState.UNKNOWN, self._started_at, "",
                    name in self.REQUIRED_COMPONENTS,
                )
                for name in self.COMPONENTS
            }
            self._lease_generation = None
            self._lease_expires_at = None

    def deactivate(self) -> None:
        """Disable the compatibility fence (used by isolated legacy tests)."""
        with self._lock:
            self._active = False
            self._status = RuntimeStatus.STARTING
            self._reason_codes.clear()
            self._components.clear()

    @property
    def active(self) -> bool:
        with self._lock:
            return self._active

    @property
    def allows_new_entries(self) -> bool:
        with self._lock:
            lease_valid = (
                self._lease_generation is not None
                and self._lease_expires_at is not None
                and self._lease_expires_at > datetime.now(timezone.utc)
            )
            return (not self._active) or (
                self._status is RuntimeStatus.READY and not self._reason_codes and lease_valid
            )

    def set_instance_lease(self, generation: int, expires_at: str) -> None:
        parsed = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        with self._lock:
            self._lease_generation = int(generation)
            self._lease_expires_at = parsed.astimezone(timezone.utc)

    def clear_instance_lease(self) -> None:
        with self._lock:
            self._lease_generation = None
            self._lease_expires_at = None

    def transition(self, status: RuntimeStatus | str, reason_code: str = "") -> None:
        target = status if isinstance(status, RuntimeStatus) else RuntimeStatus(str(status))
        with self._lock:
            self._status = target
            if reason_code:
                self._reason_codes.add(str(reason_code))

    def mark_component(
        self,
        component: str,
        state: ComponentState | str,
        detail: str = "",
        *,
        required: bool | None = None,
    ) -> None:
        name = str(component).strip().lower()
        value = state if isinstance(state, ComponentState) else ComponentState(str(state))
        with self._lock:
            is_required = name in self.REQUIRED_COMPONENTS if required is None else bool(required)
            self._components[name] = ComponentHealth(value, _utc_now(), str(detail)[:500], is_required)

    def inhibit_entries(self, reason_code: str, *, failed: bool = False) -> None:
        with self._lock:
            self._reason_codes.add(str(reason_code))
            self._status = RuntimeStatus.FAILED if failed else RuntimeStatus.NEW_ENTRIES_OFF

    def clear_inhibit(self, reason_code: str) -> None:
        with self._lock:
            self._reason_codes.discard(str(reason_code))

    def evaluate_readiness(self) -> bool:
        with self._lock:
            missing = []
            unhealthy = []
            acceptable = {ComponentState.READY, ComponentState.FRESH}
            for name in sorted(self.REQUIRED_COMPONENTS):
                health = self._components.get(name)
                if health is None:
                    missing.append(name)
                elif health.state not in acceptable:
                    unhealthy.append(f"{name}:{health.state.value}")
            self._reason_codes = {
                code for code in self._reason_codes
                if not code.startswith("READINESS_")
            }
            if missing:
                self._reason_codes.add("READINESS_MISSING:" + ",".join(missing))
            if unhealthy:
                self._reason_codes.add("READINESS_UNHEALTHY:" + ",".join(unhealthy))
            if missing or unhealthy or self._reason_codes:
                if self._status is not RuntimeStatus.FAILED:
                    self._status = RuntimeStatus.DEGRADED
                return False
            self._status = RuntimeStatus.READY
            return True

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            required_bad = any(
                health.required and health.state in {
                    ComponentState.FAILED, ComponentState.UNAVAILABLE,
                    ComponentState.STALE, ComponentState.DEGRADED,
                }
                for health in self._components.values()
            )
            any_bad = any(
                health.state in {
                    ComponentState.FAILED, ComponentState.UNAVAILABLE,
                    ComponentState.STALE, ComponentState.DEGRADED,
                }
                for health in self._components.values()
            )
            if required_bad or self._status is RuntimeStatus.FAILED:
                health_status = "FAILED"
            elif any_bad or self._reason_codes:
                health_status = "DEGRADED"
            elif self._status is RuntimeStatus.READY:
                health_status = "HEALTHY"
            else:
                health_status = "STARTING"
            return {
                "active": self._active,
                "alive": True,
                "ready": self.allows_new_entries,
                "status": self._status.value,
                "health": health_status,
                "new_entries": "ON" if self.allows_new_entries else "OFF",
                "release_sha": self._release_sha,
                "instance_id": self._instance_id,
                "started_at": self._started_at,
                "reason_codes": sorted(self._reason_codes),
                "fencing_generation": self._lease_generation,
                "fencing_expires_at": self._lease_expires_at.isoformat() if self._lease_expires_at else None,
                "components": {
                    name: {**asdict(health), "state": health.state.value}
                    for name, health in sorted(self._components.items())
                },
            }

    def public_snapshot(self) -> dict[str, Any]:
        """Secret-free health view suitable for an unauthenticated endpoint."""
        full = self.snapshot()
        return {
            "alive": True,
            "ready": full["ready"],
            "status": full["status"],
            "health": full["health"],
            "new_entries": full["new_entries"],
            "release_sha": str(full["release_sha"])[:12],
            "started_at": full["started_at"],
            "reason_codes": full["reason_codes"],
            "fencing_generation": full["fencing_generation"],
            "fencing_expires_at": full["fencing_expires_at"],
            "components": {
                name: {"state": value["state"], "updated_at": value["updated_at"]}
                for name, value in full["components"].items()
            },
        }


runtime_supervisor = RuntimeSupervisor()


def entry_admission() -> tuple[bool, str]:
    """Stable boundary consumed by the legacy execution module."""
    if runtime_supervisor.allows_new_entries:
        return True, ""
    snapshot = runtime_supervisor.snapshot()
    reasons = snapshot.get("reason_codes") or [snapshot.get("status", "NOT_READY")]
    return False, ";".join(str(item) for item in reasons)[:500]


__all__ = ["ComponentHealth", "RuntimeSupervisor", "entry_admission", "runtime_supervisor"]
