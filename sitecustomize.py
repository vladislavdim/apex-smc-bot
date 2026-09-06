"""APEX passive observability bootstrap.

Python imports sitecustomize automatically on normal startup. The installer is
fail-open and only augments telemetry / Strategy Lab cohort presentation.
"""
try:
    from core.runtime_observability import install
    install()
except Exception:
    pass
