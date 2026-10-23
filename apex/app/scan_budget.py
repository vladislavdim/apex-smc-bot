"""Deadline accounting shared by the scan task and durable candidate handling."""
from contextvars import ContextVar
import time

class ScanBudget:
    def __init__(self, seconds):
        self.deadline = time.monotonic() + seconds

    def cancel(self):
        self.deadline = time.monotonic()


scan_deadline = ContextVar("scan_deadline", default=None)


def scan_has_budget(seconds=90):
    deadline = scan_deadline.get()
    boundary = deadline.deadline if isinstance(deadline, ScanBudget) else deadline
    return boundary is None or time.monotonic() + seconds < boundary


class ScanDeadlineExceeded(TimeoutError):
    def __init__(self, seconds):
        self.timeout_seconds = seconds
        super().__init__(f"scan deadline exceeded: {seconds}s")
