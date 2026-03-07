"""
scheduler.py
~~~~~~~~~~~~
APScheduler-based periodic pipeline runner for TRON Sentinel.

Runs the full data pipeline (main.main) every 4 hours.
Called from entrypoint.py as a daemon thread in Cloud Run.

The first run happens immediately on startup, then repeats
at the configured interval.
"""

import logging
import time

logger = logging.getLogger("sentinel.scheduler")

INTERVAL_HOURS = 4


def main() -> None:
    """Run the pipeline once immediately, then every INTERVAL_HOURS."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.warning(
            "APScheduler not installed; falling back to simple loop "
            "(every %d hours)", INTERVAL_HOURS,
        )
        _simple_loop()
        return

    scheduler = BlockingScheduler()
    scheduler.add_job(
        _run_pipeline,
        "interval",
        hours=INTERVAL_HOURS,
        id="sentinel_pipeline",
        name="TRON Sentinel data pipeline",
        next_run_time=None,  # don't double-run on startup
    )

    # Run once immediately on startup
    logger.info("Running initial pipeline on startup...")
    _run_pipeline()

    logger.info(
        "Scheduler started: pipeline runs every %d hours", INTERVAL_HOURS,
    )
    scheduler.start()


def _run_pipeline() -> None:
    """Execute the full pipeline, catching all exceptions."""
    try:
        import main as pipeline
        pipeline.main()
    except Exception:
        logger.exception("Pipeline run failed")


def _simple_loop() -> None:
    """Fallback loop when APScheduler is unavailable."""
    while True:
        _run_pipeline()
        logger.info("Sleeping %d hours until next run...", INTERVAL_HOURS)
        time.sleep(INTERVAL_HOURS * 3600)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    main()
