"""
entrypoint.py
~~~~~~~~~~~~~
Cloud Run entry point for TRON Sentinel.

Cloud Run requires every container to listen on $PORT (default 8080) and
return HTTP 2xx within the startup timeout.  This module satisfies that
requirement by running a tiny health-check HTTP server in a daemon thread
while the blocking APScheduler pipeline runs in the main thread.

Architecture inside the container:
    ┌─────────────────────────────────────────────────────────────┐
    │  Thread 1 (daemon)  –  HTTP health server on $PORT          │
    │    GET /         → 200 OK  "TRON Sentinel – healthy"        │
    │    GET /health   → 200 OK  "TRON Sentinel – healthy"        │
    │                                                             │
    │  Thread 2 (main)   –  APScheduler (scheduler.main())        │
    │    • runs pipeline immediately on start                     │
    │    • repeats every 30 minutes                               │
    │    • main.py writes data.json locally AND to GCS_BUCKET     │
    └─────────────────────────────────────────────────────────────┘

Environment variables consumed here:
    PORT         – HTTP port for Cloud Run health checks (default: 8080)
    GCS_BUCKET   – set by deploy.sh; forwarded to main.py for GCS upload
"""

import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt= "%H:%M:%S",
)
logger = logging.getLogger("sentinel.entrypoint")

# ── Health-check HTTP server ───────────────────────────────────────────────────


class _HealthHandler(BaseHTTPRequestHandler):
    """Return 200 OK for any GET request; silences access logs."""

    _BODY = b"TRON Sentinel - healthy\n"

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(self._BODY)))
        self.end_headers()
        self.wfile.write(self._BODY)

    def log_message(self, fmt, *args) -> None:  # silence noisy access logs
        pass


def _start_health_server() -> None:
    port = int(os.environ.get("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    logger.info("Health-check server listening on 0.0.0.0:%d", port)
    server.serve_forever()


# ── Entry point ────────────────────────────────────────────────────────────────


def main() -> None:
    # 1. Start health-check HTTP server as a daemon thread.
    #    daemon=True means the thread is killed automatically when main() returns.
    health_thread = threading.Thread(
        target=_start_health_server,
        name="health-server",
        daemon=True,
    )
    health_thread.start()

    # 2. Import and run the scheduler in the main (foreground) thread.
    #    scheduler.main() calls APScheduler's BlockingScheduler.start()
    #    which runs forever (until Ctrl-C / SIGTERM).
    try:
        import scheduler  # noqa: PLC0415
        scheduler.main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Entrypoint received shutdown signal – exiting.")
        sys.exit(0)
    except Exception:
        logger.exception("Scheduler crashed – container will restart.")
        sys.exit(1)


if __name__ == "__main__":
    main()
