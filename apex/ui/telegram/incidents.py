"""Incident Telegram presentation helpers."""
def incident_line(code:str,status:str,detail:str="")->str:
    suffix=f" · {detail}" if detail else ""
    return f"{status.upper()} · {code}{suffix}"
__all__=["incident_line"]
