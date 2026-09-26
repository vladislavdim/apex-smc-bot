"""Incident Telegram presentation helper."""
def incident_line(code,status): return f"{code} · {status}"
__all__=["incident_line"]
