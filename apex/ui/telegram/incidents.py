"""Incident Telegram presentation helpers."""
def incident_line(code,severity,status): return f"{severity} · {code} · {status}"
__all__=["incident_line"]
