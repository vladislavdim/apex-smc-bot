"""Learning Telegram presentation boundary; read-only advisory output."""
from __future__ import annotations

def learning_line(samples:int,confidence:float|None=None)->str:
    suffix="UNKNOWN" if confidence is None else f"{float(confidence):.2f}"
    return f"Learning · real outcomes {int(samples)} · confidence {suffix}"
__all__=["learning_line"]
