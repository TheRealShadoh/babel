"""Tests for the event-loop stall watchdog and the health endpoint it feeds.

The production incident had the container sitting `unhealthy` for two and a
half days with nothing acting on it: Docker's restart policy reacts to the
container *exiting*, never to it being unhealthy.
"""

import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from src import watchdog


@pytest.fixture(autouse=True)
def _reset():
    yield
    watchdog._last_beat = time.monotonic()


async def test_heartbeat_keeps_lag_near_zero():
    await watchdog.start_watchdog(interval=0.05, unhealthy_lag=5, abort_lag=0)
    try:
        await asyncio.sleep(0.3)
        assert watchdog.loop_lag_seconds() < 1.0
        assert watchdog.is_loop_healthy()
    finally:
        await watchdog.stop_watchdog()


async def test_blocked_loop_shows_up_as_lag():
    """Block the loop thread the way a stat() against a hung mount would."""
    await watchdog.start_watchdog(interval=0.05, unhealthy_lag=0.5, abort_lag=0)
    try:
        await asyncio.sleep(0.2)
        time.sleep(1.2)  # synchronous — the loop cannot turn
        assert watchdog.loop_lag_seconds() > 1.0
        assert not watchdog.is_loop_healthy()

        await asyncio.sleep(0.2)  # loop turns again
        assert watchdog.is_loop_healthy()
    finally:
        await watchdog.stop_watchdog()


def test_health_endpoint_reports_lag_and_flips_to_503():
    from src.main import app

    with TestClient(app) as client:
        ok = client.get("/api/health")
        assert ok.status_code == 200
        assert ok.json()["status"] == "ok"
        assert "loopLagSeconds" in ok.json()

        # Simulate a loop that has been stalled longer than the threshold.
        watchdog._last_beat = time.monotonic() - 10_000
        degraded = client.get("/api/health")
        assert degraded.status_code == 503
        assert degraded.json()["status"] == "degraded"
        assert degraded.json()["loopLagSeconds"] > 1000
