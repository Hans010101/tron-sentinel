"""
entrypoint.py
~~~~~~~~~~~~~
Cloud Run entry point for TRON Sentinel.

Serves two HTTP routes on $PORT (default 8080):
    GET /health      → 200 "OK"  (Cloud Run liveness probe)
    GET /data.json   → 200 JSON  (latest dashboard data)
                       Triggers a pipeline run on first request if
                       dashboard/data.json does not yet exist.

A background daemon thread runs the APScheduler pipeline every 30 min
so the data file stays fresh without any external cron job.
"""

import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sentinel.entrypoint")

# Absolute path to the generated data file inside the container (/app/…)
DATA_JSON = Path(__file__).parent / "dashboard" / "data.json"


# ── HTTP handler ──────────────────────────────────────────────────────────────


class _Handler(BaseHTTPRequestHandler):
    """Route GET requests to health-check or data.json endpoints."""

    def do_GET(self) -> None:
        path = self.path.split("?")[0].rstrip("/") or "/"
        if path in ("/health", "/"):
            self._send(200, b"OK", "text/plain; charset=utf-8")
        elif path == "/data.json":
            self._serve_data_json()
        else:
            self._send(404, b"Not Found", "text/plain; charset=utf-8")

    # ------------------------------------------------------------------

    def _serve_data_json(self) -> None:
        """Return dashboard/data.json, generating it first if absent."""
        if not DATA_JSON.exists():
            logger.info(
                "/data.json requested but file missing – running pipeline…"
            )
            try:
                import main as pipeline          # noqa: PLC0415
                pipeline.main()
            except Exception as exc:
                logger.exception("On-demand pipeline failed: %s", exc)
                self._send(
                    503,
                    b"Pipeline error – data not yet available",
                    "text/plain; charset=utf-8",
                )
                return

        if not DATA_JSON.exists():
            self._send(
                503,
                b"data.json not found after pipeline run",
                "text/plain; charset=utf-8",
            )
            return

        try:
            body = DATA_JSON.read_bytes()
        except OSError as exc:
            logger.error("Cannot read data.json: %s", exc)
            self._send(500, b"Internal error", "text/plain; charset=utf-8")
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args) -> None:  # silence noisy access logs
        pass


# ── Scheduler thread ──────────────────────────────────────────────────────────


def _run_scheduler() -> None:
    """Run APScheduler in a daemon thread (30-minute pipeline interval)."""
    try:
        from scheduler import main as sched_main  # noqa: PLC0415
        sched_main()
    except Exception:
        logger.exception("Scheduler crashed – container will restart.")
        sys.exit(1)


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    t = threading.Thread(target=_run_scheduler, name="scheduler", daemon=True)
    t.start()

    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), _Handler)
    logger.info("Serving on 0.0.0.0:%d  (health + /data.json)", port)
    try:
        server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Entrypoint received shutdown signal – exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()
