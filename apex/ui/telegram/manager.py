"""Manager Telegram presentation boundary."""
def manager_line(state=None,position_id=None): return f"Manager {state or 'UNKNOWN'} · {position_id or 'no position'}"
__all__=["manager_line"]
