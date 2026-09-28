"""Portfolio risk exposure model for APEX V3."""
from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class PortfolioExposure:
    total_risk_pct: float=0.0
    long_risk_pct: float=0.0
    short_risk_pct: float=0.0
    cluster_risk_pct: float=0.0
    open_positions: int=0
    def same_side(self,direction:str)->float:
        return self.long_risk_pct if str(direction).upper() in {"LONG","BULLISH","BUY"} else self.short_risk_pct
__all__=["PortfolioExposure"]
