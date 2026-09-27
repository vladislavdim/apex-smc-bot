"""Incident Telegram presentation boundary."""
def incident_line(code,status,detail=""):
    suffix=f" · {detail}" if detail else ""
    return f"{code} · {status}{suffix}"
__all__=["incident_line"]
