"""Runtime health projection."""
from apex.app.runtime import runtime_supervisor


def health_snapshot() -> dict:
    return runtime_supervisor.snapshot()


__all__ = ["health_snapshot"]
