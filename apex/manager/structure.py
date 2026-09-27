"""Manager structural-analysis boundary.
Consumes canonical market structure facts; it does not own market-data analysis.
"""
from __future__ import annotations
from typing import Any,Mapping

def structural_facts(structure:Mapping[str,Any]|None)->dict[str,Any]:
    value=dict(structure or {})
    event=value.get("event") if isinstance(value.get("event"),dict) else {}
    return {"direction":value.get("direction"),"trend_direction":value.get("trend_direction"),"event_type":event.get("type"),"event_direction":event.get("direction"),"event_level":event.get("level")}
__all__=["structural_facts"]
