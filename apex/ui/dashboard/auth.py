"""Dashboard authentication primitive."""
from __future__ import annotations
import hmac

def authorized(provided:str|None,expected:str)->bool:
    return bool(provided and expected and hmac.compare_digest(str(provided),str(expected)))
__all__=["authorized"]
