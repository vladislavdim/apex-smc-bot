"""System Telegram presentation boundary."""
from apex.ui.telegram.execution import account_line

def system_line(status,*,entries_allowed,release_sha=None):
    release=(release_sha or "UNKNOWN")[:12]
    return f"System {status} · entries {'ON' if entries_allowed else 'OFF'} · {release}"
__all__=["account_line","system_line"]
