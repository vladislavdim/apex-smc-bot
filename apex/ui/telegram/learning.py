"""Learning Telegram presentation; advisory only."""
def learning_line(status,samples=None):
    suffix="" if samples is None else f" · samples {int(samples)}"
    return f"Learning {status}{suffix}"
__all__=["learning_line"]
