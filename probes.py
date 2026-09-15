"""A minimal HTTP server for the worker and scheduler processes.

The API gets its probe endpoints from FastAPI. The worker and the scheduler
have no web framework and do not need one, but Kubernetes still has to be able
to ask them whether they are alive, and Prometheus still has to scrape them.
Anything that cannot be probed is a pod Kubernetes will happily keep sending
work to long after it has stopped doing any.

Uses only the standard library, so the worker image carries no extra
dependency for it.
"""

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable

import metrics


def _handler_for(readiness: Callable[[], bool]):
    class Handler(BaseHTTPRequestHandler):
        # Liveness: the process is running and its HTTP thread is responsive.
        # Deliberately does not touch Redis — a liveness probe that fails on a
        # dependency outage restarts every pod at once, turning a recoverable
        # problem into an outage.
        def _live(self):
            self._send(200, b"ok\n", "text/plain")

        # Readiness: this process can actually do its job right now.
        def _ready(self):
            try:
                ok = readiness()
            except Exception:
                ok = False
            metrics.redis_up.set(1 if ok else 0)
            if ok:
                self._send(200, b"ready\n", "text/plain")
            else:
                self._send(503, b"not ready\n", "text/plain")

        def _metrics(self):
            body, content_type = metrics.render()
            self._send(200, body, content_type)

        def _send(self, status: int, body: bytes, content_type: str):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802 - name fixed by BaseHTTPRequestHandler
            route = {
                "/health": self._live,
                "/healthz": self._live,
                "/ready": self._ready,
                "/readyz": self._ready,
                "/metrics": self._metrics,
            }.get(self.path.split("?")[0])

            if route is None:
                self._send(404, b"not found\n", "text/plain")
            else:
                route()

        # The default handler logs every request to stderr, which would bury
        # real worker output under one line per probe every few seconds.
        def log_message(self, fmt, *args):
            pass

    return Handler


def serve(host: str, port: int, readiness: Callable[[], bool]) -> HTTPServer:
    """Start the probe server on a daemon thread and return it."""
    server = HTTPServer((host, port), _handler_for(readiness))
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="probes")
    thread.start()
    return server
