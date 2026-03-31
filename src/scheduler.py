import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from src.config import get_settings
from src.scanner.engine import run_scan

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def _safe_run_scan() -> None:
    """Run a scan, catching exceptions so the scheduler stays alive."""
    try:
        await run_scan()
    except Exception:
        logger.exception("Scheduled scan failed")


async def _safe_run_dub_lookup() -> None:
    """Run dub availability lookup, catching exceptions."""
    try:
        from src.scanner.dub_lookup import run_dub_lookup
        await run_dub_lookup()
    except Exception:
        logger.exception("Scheduled dub lookup failed")


def start_scheduler() -> None:
    settings = get_settings()

    scheduler.add_job(
        _safe_run_scan,
        trigger="interval",
        hours=settings.SCAN_INTERVAL_HOURS,
        id="periodic_scan",
        replace_existing=True,
        jitter=60,
    )

    # Dub availability lookup — daily
    scheduler.add_job(
        _safe_run_dub_lookup,
        trigger="interval",
        hours=24,
        id="dub_lookup",
        replace_existing=True,
        jitter=300,
    )

    # Run an initial scan 10 seconds after startup.
    scheduler.add_job(
        _safe_run_scan,
        trigger="date",
        run_date=datetime.now(timezone.utc) + timedelta(seconds=10),
        id="initial_scan",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started - scans every %sh, dub lookup daily", settings.SCAN_INTERVAL_HOURS)


def stop_scheduler() -> None:
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


def get_next_run_time() -> str | None:
    job = scheduler.get_job("periodic_scan")
    if job and job.next_run_time:
        return job.next_run_time.isoformat()
    return None
