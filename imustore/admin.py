"""A small admin HTTP endpoint for observability.

Serves three routes from the standard library only, so any monitoring stack can
scrape them:

- ``GET /metrics`` -- Prometheus text exposition of the registry.
- ``GET /healthz`` -- liveness probe (200 ``ok``).
- ``GET /stats``   -- JSON from an injected callable (e.g. storage stats).

It runs on a daemon thread so it can sit alongside the async server without
blocking the event loop.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .metrics import REGISTRY


class AdminServer:
    def __init__(self, registry=None, *, stats=None, host="127.0.0.1", port=0):
        self._registry = registry or REGISTRY
        self._stats = stats
        self._httpd = ThreadingHTTPServer((host, port), self._make_handler())
        self._thread = None

    @property
    def address(self):
        return self._httpd.server_address

    def start(self):
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        return self.address

    def stop(self):
        self._httpd.shutdown()
        if self._thread is not None:
            self._thread.join()
        self._httpd.server_close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.stop()

    def _make_handler(server):  # noqa: N805 - closure over the AdminServer
        registry = server._registry
        stats = server._stats

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass  # keep the admin endpoint quiet

            def _respond(self, status, body, content_type="text/plain; charset=utf-8"):
                payload = body.encode("utf-8") if isinstance(body, str) else body
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):
                try:
                    if self.path == "/metrics":
                        self._respond(200, registry.render())
                    elif self.path in ("/healthz", "/health"):
                        self._respond(200, "ok\n")
                    elif self.path == "/stats":
                        data = stats() if stats is not None else {}
                        self._respond(200, json.dumps(data) + "\n", "application/json")
                    else:
                        self._respond(404, "not found\n")
                except Exception as exc:  # a broken stats callable must not drop the socket
                    self._respond(500, f"internal error: {exc}\n")

        return Handler
