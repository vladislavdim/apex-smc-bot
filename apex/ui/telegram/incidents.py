"""Incident Telegram presentation helpers."""
def incident_line(code,status,detail=""):
    tail=f" · {detail}" if detail else ""
    return f"{status} · {code}{tail}"
__all__=["incident_line"]
