"""Learning Telegram presentation; advisory and read-only."""
def learning_line(samples=0,confidence=None):
    c="UNKNOWN" if confidence is None else f"{float(confidence):.2f}"
    return f"Learning · real outcomes {int(samples)} · confidence {c}"
__all__=["learning_line"]
