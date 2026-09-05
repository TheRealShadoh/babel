import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.config import get_effective_settings, get_settings
from src.scanner.engine import run_scan

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


# A run that starts late (the loop was busy, the container was paused) is
# still wanted. APScheduler's default grace of 1s would silently drop it.
_MISFIRE_GRACE = 300


def clamp_interval_hours(value) -> int:
    """SCAN_INTERVAL_HOURS as a usable interval — never less than an hour.

    APScheduler rewrites a zero interval to one second, which would run
    scans back to back forever; there is no client-side check we can rely on.
    """
    try:
        hours = int(value)
    except (TypeError, ValueError):
        return get_settings().SCAN_INTERVAL_HOURS
    return max(1, hours)


async def _safe_run_scan() -> None:
    """Run a scan, catching exceptions so the scheduler stays alive."""
    try:
        await run_scan()
    except Exception:
        logger.exception("Scheduled scan failed")


# The first scan fires shortly after startup, which is also when Sonarr and
# the media server are most likely to still be coming up (a host reboot
# starts every container at once). A pass that fails because a service is
# unreachable is retried with backoff instead of being written off until
# the next interval.
_INITIAL_RETRY_DELAYS = (30, 60, 120, 300, 600)


async def _initial_scan(attempt: int = 0) -> None:
    try:
        result = await run_scan()
    except Exception:
        logger.exception("Initial scan failed")
        return

    if not (isinstance(result, dict) and result.get("retryable")):
        return
    if attempt >= len(_INITIAL_RETRY_DELAYS):
        logger.warning(
            "Initial scan gave up after %d attempts; the periodic scan will try again",
            attempt + 1,
        )
        return
    delay = _INITIAL_RETRY_DELAYS[attempt]
    logger.info("Initial scan could not run yet — retrying in %ds", delay)
    scheduler.add_job(
        _initial_scan,
        trigger="date",
        run_date=datetime.now(timezone.utc) + timedelta(seconds=delay),
        id="initial_scan",
        replace_existing=True,
        misfire_grace_time=_MISFIRE_GRACE,
        kwargs={"attempt": attempt + 1},
    )


async def _safe_run_dub_lookup() -> None:
    """Run dub availability lookup, catching exceptions."""
    try:
        from src.scanner.dub_lookup import run_dub_lookup
        await run_dub_lookup()
    except Exception:
        logger.exception("Scheduled dub lookup failed")


async def start_scheduler() -> None:
    # Use effective settings (env + DB overrides) so a scan interval configured
    # via the web UI takes effect without requiring a container restart.
    cfg = await get_effective_settings()
    interval_hours = clamp_interval_hours(
        cfg.get("SCAN_INTERVAL_HOURS", get_settings().SCAN_INTERVAL_HOURS)
    )

    scheduler.add_job(
        _safe_run_scan,
        trigger="interval",
        hours=interval_hours,
        id="periodic_scan",
        replace_existing=True,
        jitter=60,
        misfire_grace_time=_MISFIRE_GRACE,
        coalesce=True,
    )

    # Dub availability lookup — daily. An interval trigger's first run is one
    # interval after start, so on a container that restarts more often than
    # daily it would never fire; start it an hour in instead.
    scheduler.add_job(
        _safe_run_dub_lookup,
        trigger="interval",
        hours=24,
        id="dub_lookup",
        replace_existing=True,
        jitter=300,
        misfire_grace_time=_MISFIRE_GRACE,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    # Run an initial scan shortly after startup, retrying if the services it
    # needs are not up yet.
    scheduler.add_job(
        _initial_scan,
        trigger="date",
        run_date=datetime.now(timezone.utc) + timedelta(seconds=10),
        id="initial_scan",
        replace_existing=True,
        misfire_grace_time=_MISFIRE_GRACE,
    )

    scheduler.start()
    logger.info("Scheduler started - scans every %sh, dub lookup daily", interval_hours)


def stop_scheduler() -> None:
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


def reschedule_scan(interval_hours: int) -> None:
    """Update the periodic scan job's interval without restarting the process.

    Called when SCAN_INTERVAL_HOURS is changed via the Settings page.
    """
    interval_hours = clamp_interval_hours(interval_hours)
    if not scheduler.running:
        return
    job = scheduler.get_job("periodic_scan")
    if job is None:
        return
    scheduler.reschedule_job(
        "periodic_scan", trigger=IntervalTrigger(hours=interval_hours, jitter=60)
    )
    logger.info("Scan interval updated to every %sh", interval_hours)


def get_next_run_time() -> str | None:
    job = scheduler.get_job("periodic_scan")
    if job and job.next_run_time:
        return job.next_run_time.isoformat()
    return None
