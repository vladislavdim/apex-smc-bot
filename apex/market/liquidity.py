"""Canonical liquidity context boundary for V3 market snapshots."""
from __future__ import annotations
from typing import Any,Mapping

def liquidity_context(*,sweeps:Mapping[str,Any]|None=None,heatmap:Mapping[str,Any]|None=None)->dict[str,Any]:
    return {"sweeps":dict(sweeps or {}),"heatmap":dict(heatmap or {})}
__all__=["liquidity_context"]
