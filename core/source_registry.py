"""Compatibility façade for the canonical V3 source registry."""

from apex.market.source_registry import (
    REGISTRY,
    SOURCE_ALIASES,
    SOURCES,
    SourcePolicyError,
    SourceSpec,
    authorize,
    get_source,
    registry_snapshot,
    source_contract,
    source_spec,
    validate_source_usage,
)

__all__ = [
    "REGISTRY", "SOURCES", "SOURCE_ALIASES", "SourcePolicyError", "SourceSpec",
    "authorize", "get_source", "registry_snapshot", "source_contract", "source_spec",
    "validate_source_usage",
]
