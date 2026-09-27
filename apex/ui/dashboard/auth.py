"""Constant-time authentication primitives for the read-only Dashboard."""
from __future__ import annotations
import hmac

def authorized(provided:str|None,expected:str|None)->bool:
    return bool(provided and expected and hmac.compare_digest(str(provided),str(expected)))

__all__=["authorized"]
