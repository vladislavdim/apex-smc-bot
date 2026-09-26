"""Telegram presentation-only formatters."""
def money(value,quote="USDT"):
    return "UNKNOWN" if value is None else f"{float(value):.2f} {quote}"
__all__=["money"]
