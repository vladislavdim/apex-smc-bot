"""Durable, append-only recovery for exchange mutations not yet in State.

This journal is not a second source of truth. It exists only when Binance has
already accepted an operation and the canonical State transaction failed. All
records are idempotently replayed into State before the journal is removed.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from apex.db.repositories.executions import ExecutionRepository


_LOCK = threading.RLock()


def recovery_path(compatibility_path: str) -> Path:
    path = Path(str(compatibility_path)).expanduser().resolve()
    return path.with_name("apex_execution_recovery.jsonl")


def append_recovery(compatibility_path: str, event: Mapping[str, Any]) -> Path:
    """Append one complete JSON record and force it to stable local storage."""
    target = recovery_path(compatibility_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "recorded_at": time.time(),
        **dict(event),
    }
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str) + "\n").encode()
    with _LOCK:
        descriptor = os.open(target, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return target


def _apply(repository: ExecutionRepository, event: Mapping[str, Any]) -> None:
    kind = str(event.get("kind") or "")
    if kind == "execution_snapshot":
        values = dict(event.get("values") or {})
        signal_id = int(values["signal_id"])
        if repository.get(signal_id) is None:
            repository.register(values)
        repository.update_exchange_state(
            signal_id,
            **{
                key: values.get(key) for key in (
                    "status", "quantity", "entry_order_id", "stop_order_id",
                    "tp1_order_id", "tp2_order_id", "active_stop_price",
                    "pending_stop_order_id", "previous_stop_order_id", "last_error",
                ) if key in values
            },
        )
        return
    if kind == "execution_update":
        updated = repository.update_exchange_state(
            int(event["signal_id"]),
            status=str(event["status"]),
            **dict(event.get("changes") or {}),
        )
        if not updated:
            raise RuntimeError("execution_recovery_parent_missing")
        return
    if kind == "manager_action_finish":
        updated = repository.finish_action(
            str(event["action_key"]), str(event["status"]),
            order_id=str(event.get("order_id") or ""),
            error=str(event.get("error") or ""),
        )
        if not updated:
            raise RuntimeError("execution_recovery_action_missing")
        return
    raise ValueError(f"unknown_execution_recovery_kind:{kind}")


def replay_recovery(
    compatibility_path: str, connection_factory: Callable[[], Any],
) -> int:
    """Replay the whole journal atomically-by-idempotency, deleting only on success."""
    target = recovery_path(compatibility_path)
    with _LOCK:
        if not target.exists():
            return 0
        lines = target.read_text(encoding="utf-8").splitlines()
        events = [json.loads(line) for line in lines if line.strip()]
        repository = ExecutionRepository(connection_factory)
        for event in events:
            _apply(repository, event)
        target.unlink()
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return len(events)
