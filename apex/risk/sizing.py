"""Pure position sizing for APEX V3."""
from __future__ import annotations

def quantity_for_risk(equity_quote:float,risk_pct:float,entry:float,stop:float)->float:
    distance=abs(float(entry)-float(stop))
    if equity_quote<=0 or risk_pct<=0 or distance<=0: return 0.0
    return (float(equity_quote)*(float(risk_pct)/100.0))/distance
__all__=["quantity_for_risk"]
