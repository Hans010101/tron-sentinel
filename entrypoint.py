"""
entrypoint.py
~~~~~~~~~~~~~
Cloud Run entry point for TRON Sentinel.

Serves HTTP routes on $PORT (default 8080):
    GET /health      → 200 JSON  health report (scripts/health_check.py)
    GET /diagnose    → 200 text  full system diagnosis (scripts/diagnose.py)
    GET /logs        → 200 text  last 200 lines of data/pipeline.log
    GET /            → 200 HTML  dashboard (dashboard/index.html)
    GET /data.json   → 200 JSON  latest dashboard data
                       Triggers a pipeline run on first request if
                       dashboard/data.json does not yet exist.

A background daemon thread runs the APScheduler pipeline every 4 hours
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
INDEX_HTML = Path(__file__).parent / "dashboard" / "index.html"


# ── HTTP handler ──────────────────────────────────────────────────────────────


class _Handler(BaseHTTPRequestHandler):
    """Route GET requests to health-check or data.json endpoints."""

    def do_GET(self) -> None:
        path = self.path.split("?")[0].rstrip("/") or "/"
        if path == "/health":
            self._serve_health()
        elif path == "/diagnose":
            self._serve_diagnose()
        elif path == "/logs":
            self._serve_logs()
        elif path == "/":
            self._serve_index()
        elif path == "/data.json":
            self._serve_data_json()
        else:
            self._send(404, b"Not Found", "text/plain; charset=utf-8")

    # ------------------------------------------------------------------

    def _serve_health(self) -> None:
        """Return a JSON health report from scripts/health_check.py."""
        import json  # noqa: PLC0415
        try:
            sys.path.insert(0, str(Path(__file__).parent / "scripts"))
            from health_check import run_health_check  # noqa: PLC0415
            report = run_health_check()
            body   = json.dumps(report, ensure_ascii=False).encode("utf-8")
            status = 200 if report.get("status") in ("healthy", "degraded") else 503
        except Exception as exc:
            logger.warning("Health check failed: %s", exc)
            body   = json.dumps({"status": "critical", "error": str(exc)}).encode("utf-8")
            status = 503
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------

    def _serve_diagnose(self) -> None:
        """Run scripts/diagnose.py and return the report as plain text."""
        try:
            sys.path.insert(0, str(Path(__file__).parent / "scripts"))
            from diagnose import run_diagnosis  # noqa: PLC0415
            report = run_diagnosis()
            body   = report.encode("utf-8")
            status = 200
        except Exception as exc:
            logger.warning("Diagnose failed: %s", exc)
            body   = f"Diagnosis error: {exc}".encode("utf-8")
            status = 500
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------

    def _serve_logs(self) -> None:
        """Return the last 200 lines of data/pipeline.log as plain text."""
        log_path = Path(__file__).parent / "data" / "pipeline.log"
        if not log_path.exists():
            body = (
                "pipeline.log 尚不存在 – 流水线还未运行过。\n"
                "首次运行后此文件将自动创建。"
            ).encode("utf-8")
        else:
            try:
                lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
                body = "\n".join(lines[-200:]).encode("utf-8")
            except Exception as exc:
                logger.warning("Cannot read pipeline.log: %s", exc)
                body = f"读取日志失败: {exc}".encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------

    def _serve_index(self) -> None:
        """Return dashboard/index.html."""
        try:
            body = INDEX_HTML.read_bytes()
        except OSError:
            self._send(500, b"index.html not found", "text/plain; charset=utf-8")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------------

    def _serve_data_json(self) -> None:
        """
        Return dashboard/data.json.

        Triggers a pipeline rerun when:
          - the file does not exist yet (first boot), OR
          - the file exists but was generated by an older version of the
            pipeline that did not include the 'all_articles' key (stale cache).
        """
        import json as _json  # noqa: PLC0415

        def _needs_pipeline() -> bool:
            """Return True if data.json is missing or missing all_articles."""
            if not DATA_JSON.exists():
                logger.info("/data.json missing – will run pipeline")
                return True
            try:
                parsed = _json.loads(DATA_JSON.read_bytes())
                count  = len(parsed.get("all_articles", []))
                logger.info(
                    "data.json keys=%s  all_articles=%d",
                    list(parsed.keys()), count,
                )
                if "all_articles" not in parsed:
                    logger.info(
                        "data.json lacks all_articles – re-running pipeline "
                        "to regenerate with full article list"
                    )
                    return True
            except Exception as exc:
                logger.warning("Cannot inspect data.json: %s", exc)
            return False

        if _needs_pipeline():
            try:
                import main as pipeline          # noqa: PLC0415
                pipeline.main()
            except Exception as exc:
                logger.exception("On-demand pipeline failed: %s", exc)
                self._send(
                    503,
                    b"Pipeline error - data not yet available",
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
            # Debug: log all_articles count from the file we are about to serve
            try:
                parsed = _json.loads(body)
                logger.info(
                    "Serving data.json: all_articles=%d  total_keys=%d",
                    len(parsed.get("all_articles", [])), len(parsed),
                )
            except Exception:
                pass
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


def _sync_db_from_gcs() -> None:
    """If GCS_BUCKET is configured, pull the latest DB before serving traffic."""
    bucket = os.environ.get("GCS_BUCKET", "").strip()
    if not bucket:
        return
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        from utils.gcs_storage import download_db  # noqa: PLC0415
        db_path = Path(__file__).parent / "data" / "sentinel.db"
        ok = download_db(bucket, db_path)
        if ok:
            logger.info("Startup GCS sync: DB downloaded from gs://%s/data/sentinel.db", bucket)
        else:
            logger.info("Startup GCS sync: no existing DB in GCS, will create fresh")
    except Exception as exc:
        logger.warning("Startup GCS sync failed: %s", exc)


def main() -> None:
    _sync_db_from_gcs()

    t = threading.Thread(target=_run_scheduler, name="scheduler", daemon=True)
    t.start()

    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), _Handler)
    logger.info("Serving on 0.0.0.0:%d  (health + /data.json + /diagnose + /logs)", port)
    try:
        server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Entrypoint received shutdown signal – exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()
