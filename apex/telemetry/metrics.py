"""Small dependency-free metric primitives for APEX V3."""
from __future__ import annotations
from dataclasses import dataclass
@dataclass
class Counter:
    value:int=0
    def inc(self,amount:int=1)->int:
        self.value+=int(amount); return self.value
__all__=["Counter"]
