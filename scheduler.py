"""
scheduler.py
~~~~~~~~~~~~
TRON Sentinel – automated pipeline scheduler.

Runs the full three-step pipeline (RSS collection → sentiment analysis →
Telegram alerting) on a fixed 30-minute interval using APScheduler.

Usage:
    python scheduler.py          # run forever (Ctrl-C to stop)
    python scheduler.py --once   # run once immediately, then exit
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt= "%H:%M:%S",
)
logger = logging.getLogger("sentinel.scheduler")

# ── Configuration ──────────────────────────────────────────────────────────────

INTERVAL_MINUTES = 30

_SEP  = "─" * 58
_SEP2 = "═" * 58

# ── Job ───────────────────────────────────────────────────────────────────────


def run_pipeline() -> None:
    """
    Execute the full pipeline via main.main() and log the outcome.

    Any uncaught exception is logged without crashing the scheduler,
    so the next scheduled run will still be attempted.
    """
    started_at = datetime.now(timezone.utc)
    wall_start  = time.perf_counter()

    print(f"\n{_SEP2}")
    print(f"  [Scheduler]  定时触发  ·  {started_at.strftime('%Y-%m-%d  %H:%M:%S UTC')}")
    print(f"{_SEP2}")

    try:
        import main as pipeline  # noqa: PLC0415  (lazy import keeps top-level fast)
        pipeline.main()
        elapsed = time.perf_counter() - wall_start
        logger.info("Pipeline completed in %.1fs", elapsed)
    except Exception:
        elapsed = time.perf_counter() - wall_start
        logger.exception("Pipeline raised an unhandled exception after %.1fs", elapsed)


# ── Entry point ────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TRON Sentinel scheduler – runs the pipeline every 30 minutes.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline once immediately and exit.",
    )
    args = parser.parse_args()

    if args.once:
        logger.info("--once flag set: running pipeline once and exiting.")
        run_pipeline()
        return

    scheduler = BlockingScheduler(timezone="UTC")
    trigger   = IntervalTrigger(minutes=INTERVAL_MINUTES)

    scheduler.add_job(
        func    = run_pipeline,
        trigger = trigger,
        id      = "sentinel_pipeline",
        name    = "TRON Sentinel Pipeline",
        # Run once immediately on start, then every INTERVAL_MINUTES.
        next_run_time = datetime.now(timezone.utc),
    )

    print(f"\n{_SEP2}")
    print("  TRON Sentinel Scheduler")
    print(f"  间隔：每 {INTERVAL_MINUTES} 分钟  ·  首次执行：立即")
    print("  按 Ctrl-C 停止")
    print(f"{_SEP2}\n")

    logger.info(
        "Scheduler started – pipeline will run every %d minutes (UTC).",
        INTERVAL_MINUTES,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user.")
        print("\n  调度器已停止。\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
