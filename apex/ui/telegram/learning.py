"""Learning Telegram presentation; read-only advisory output."""
def learning_line(outcomes=0,confidence=None):
    c="UNKNOWN" if confidence is None else f"{float(confidence):.2f}"
    return f"Learning · real outcomes {int(outcomes)} · confidence {c}"
__all__=["learning_line"]
