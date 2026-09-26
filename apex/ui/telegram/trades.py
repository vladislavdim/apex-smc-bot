"""Trade Telegram presentation boundary."""
def trade_line(symbol,direction,status): return f"{symbol} · {direction} · {status}"
__all__=["trade_line"]
