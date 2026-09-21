"""Fallback HTTP health server for polling-mode compatibility."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            from apex.app.runtime import runtime_supervisor
            snapshot = runtime_supervisor.public_snapshot()
        except Exception as exc:
            snapshot = {
                "alive": True, "ready": False, "status": "DEGRADED",
                "reason_codes": [f"RUNTIME_STATUS_ERROR:{type(exc).__name__}"],
            }
        if self.path == "/health/ready":
            code = 200 if snapshot.get("ready") else 503
            payload = {
                "ready": bool(snapshot.get("ready")),
                "status": snapshot.get("status"),
                "health": snapshot.get("health"),
            }
        elif self.path == "/health/system":
            code, payload = 200, snapshot
        elif self.path in {"/", "/health", "/health/live"}:
            code, payload = 200, {
                "alive": True, "status": snapshot.get("status"),
                "health": snapshot.get("health"),
            }
        else:
            code, payload = 404, {"error": "not_found"}
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass


def run_server() -> None:
    HTTPServer(("0.0.0.0", 10000), HealthHandler).serve_forever()


__all__ = ["HealthHandler", "run_server"]
