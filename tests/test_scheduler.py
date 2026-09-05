"""Scheduler hardening: bounded intervals and a retrying initial scan."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from src import scheduler
from src.db import models
from src.db.database import get_db, init_db
from src.web import auth
from src.web.routes import router


def test_interval_is_never_below_an_hour():
    # APScheduler rewrites a zero interval to one second — scans back to back.
    assert scheduler.clamp_interval_hours(0) == 1
    assert scheduler.clamp_interval_hours("0") == 1
    assert scheduler.clamp_interval_hours(-5) == 1
    assert scheduler.clamp_interval_hours("12") == 12
    assert scheduler.clamp_interval_hours("garbage") == 6  # the shipped default


@pytest.fixture
def recorded_jobs(monkeypatch):
    calls = []

    def fake_add_job(func, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(scheduler.scheduler, "add_job", fake_add_job)
    return calls


@pytest.mark.asyncio
async def test_initial_scan_retries_when_a_service_is_not_up_yet(monkeypatch, recorded_jobs):
    async def unreachable():
        return {"status": "failed", "error": "Sonarr is configured but unreachable", "retryable": True}

    monkeypatch.setattr(scheduler, "run_scan", unreachable)
    before = datetime.now(timezone.utc)

    await scheduler._initial_scan(attempt=0)

    assert len(recorded_jobs) == 1
    job = recorded_jobs[0]
    assert job["id"] == "initial_scan"
    assert job["kwargs"] == {"attempt": 1}
    assert 25 <= (job["run_date"] - before).total_seconds() <= 35
    assert job["misfire_grace_time"] == scheduler._MISFIRE_GRACE


@pytest.mark.asyncio
async def test_initial_scan_backs_off_and_eventually_gives_up(monkeypatch, recorded_jobs):
    async def unreachable():
        return {"status": "failed", "retryable": True}

    monkeypatch.setattr(scheduler, "run_scan", unreachable)

    await scheduler._initial_scan(attempt=3)
    assert recorded_jobs[-1]["kwargs"] == {"attempt": 4}
    delay = (recorded_jobs[-1]["run_date"] - datetime.now(timezone.utc)).total_seconds()
    assert 290 <= delay <= 305  # the fourth retry waits five minutes

    recorded_jobs.clear()
    await scheduler._initial_scan(attempt=len(scheduler._INITIAL_RETRY_DELAYS))
    assert recorded_jobs == []  # gave up; the periodic scan takes over


@pytest.mark.asyncio
async def test_initial_scan_does_not_retry_a_completed_or_non_retryable_pass(monkeypatch, recorded_jobs):
    async def completed():
        return {"status": "completed"}

    monkeypatch.setattr(scheduler, "run_scan", completed)
    await scheduler._initial_scan()
    assert recorded_jobs == []

    async def broken():
        return {"status": "failed", "error": "listing failed"}

    monkeypatch.setattr(scheduler, "run_scan", broken)
    await scheduler._initial_scan()
    assert recorded_jobs == []


@pytest.mark.asyncio
async def test_initial_scan_survives_an_exception(monkeypatch, recorded_jobs):
    async def explode():
        raise RuntimeError("boom")

    monkeypatch.setattr(scheduler, "run_scan", explode)
    await scheduler._initial_scan()  # must not raise
    assert recorded_jobs == []


@pytest.fixture
def web(tmp_path, monkeypatch):
    db_path = str(tmp_path / "web.db")
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("SONARR_URL", "")
    asyncio.run(init_db(db_path))

    app = FastAPI()
    app.include_router(router)
    app.add_middleware(auth.BasicAuthMiddleware)
    app.add_middleware(auth.SameOriginMiddleware)
    app.state.templates = Jinja2Templates(directory=str(Path("src/web/templates")))
    with TestClient(app) as client:
        yield client, db_path


def test_saving_a_zero_interval_stores_one_hour(web):
    client, db_path = web

    resp = client.post(
        "/settings", data={"SCAN_INTERVAL_HOURS": "0"},
        headers={"Sec-Fetch-Site": "same-origin"}, follow_redirects=False,
    )
    assert resp.status_code == 303

    async def stored():
        db = await get_db(db_path)
        try:
            return await models.get_setting(db, "SCAN_INTERVAL_HOURS")
        finally:
            await db.close()

    assert asyncio.run(stored()) == "1"
