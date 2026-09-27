"""Learning Telegram presentation; advisory facts only."""
def learning_line(outcomes=0,confidence=None):
    conf="UNKNOWN" if confidence is None else f"{float(confidence):.2f}"
    return f"Learning · real outcomes {int(outcomes)} · confidence {conf}"
__all__=["learning_line"]
