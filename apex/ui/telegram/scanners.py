"""Scanner Telegram presentation."""
def scanner_line(name,status,candidates=0): return f"{name} · {status} · candidates {int(candidates)}"
__all__=["scanner_line"]
