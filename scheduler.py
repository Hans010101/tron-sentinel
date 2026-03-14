"""
scheduler.py
~~~~~~~~~~~~
APScheduler-based periodic pipeline runner for TRON Sentinel.

Two scheduled tasks:
    1. Data pipeline (main.main) — every 4 hours
    2. Daily report (reporters.daily_report) — every day at 09:00 UTC+8

Called from entrypoint.py as a daemon thread in Cloud Run.
The first pipeline run happens immediately on startup.
"""

import logging
import time

logger = logging.getLogger("sentinel.scheduler")

INTERVAL_HOURS = 4


def main() -> None:
    """Run the pipeline once immediately, then schedule recurring jobs."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.warning(
            "APScheduler not installed; falling back to simple loop "
            "(every %d hours)", INTERVAL_HOURS,
        )
        _simple_loop()
        return

    scheduler = BlockingScheduler()

    # Job 1: Data pipeline every INTERVAL_HOURS
    scheduler.add_job(
        _run_pipeline,
        "interval",
        hours=INTERVAL_HOURS,
        id="sentinel_pipeline",
        name="TRON Sentinel data pipeline",
        next_run_time=None,  # don't double-run on startup
    )

    # Job 2: Daily report at 09:00 UTC+8 (= 01:00 UTC)
    scheduler.add_job(
        _run_daily_report,
        CronTrigger(hour=1, minute=0, timezone="UTC"),  # 09:00 UTC+8
        id="sentinel_daily_report",
        name="TRON Sentinel daily report (09:00 UTC+8)",
    )

    # Run pipeline once immediately on startup
    logger.info("Running initial pipeline on startup...")
    _run_pipeline()

    logger.info(
        "Scheduler started: pipeline every %d hours, "
        "daily report at 09:00 UTC+8",
        INTERVAL_HOURS,
    )
    scheduler.start()


def _run_pipeline() -> None:
    """Execute the full pipeline, catching all exceptions.

    GCS upload is handled inside main.main() at the end of the pipeline,
    so it is automatically included in every scheduled run.
    """
    try:
        import main as pipeline
        pipeline.main()
    except Exception:
        logger.exception("Pipeline run failed")


def _run_daily_report() -> None:
    """Generate and send daily reports via Feishu webhook."""
    try:
        from reporters.daily_report import generate_and_send_all_reports
        sent = generate_and_send_all_reports()
        logger.info("Daily report job done: %d/3 reports sent.", sent)
    except Exception:
        logger.exception("Daily report job failed")


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
