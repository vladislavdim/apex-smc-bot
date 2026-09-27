"""Learning Telegram presentation boundary."""
def learning_line(real_outcomes=0,confidence=None):
    c="UNKNOWN" if confidence is None else f"{float(confidence):.2f}"
    return f"Learning · real outcomes {int(real_outcomes)} · confidence {c}"
__all__=["learning_line"]
