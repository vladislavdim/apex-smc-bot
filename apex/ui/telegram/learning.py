"""Learning Telegram projection; presentation only."""
def learning_line(real_outcomes:int,confidence=None):
    suffix="UNKNOWN" if confidence is None else f"{float(confidence):.2f}"
    return f"Learning · real outcomes {int(real_outcomes)} · confidence {suffix}"
__all__=["learning_line"]
