"""Timezone normalisation, auth, same-origin enforcement and settings casting."""

import asyncio
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from src.db import models
from src.db.database import init_db, get_db
from src.web import auth
from src.web.routes import _to_utc_display, router


def _set_settings(db_path, **values):
    async def _store():
        db = await get_db(db_path)
        try:
            for key, value in values.items():
                await models.set_setting(db, key, value)
        finally:
            await db.close()

    asyncio.run(_store())


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "web_test.db")
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("SONARR_URL", "")

    from src.config import get_settings, invalidate_effective_settings_cache
    get_settings.cache_clear()
    invalidate_effective_settings_cache()
    asyncio.run(init_db(db_path))

    app = FastAPI()
    app.include_router(router)
    app.add_middleware(auth.BasicAuthMiddleware)
    app.add_middleware(auth.SameOriginMiddleware)
    app.state.templates = Jinja2Templates(directory=str(Path("src/web/templates")))

    with TestClient(app) as c:
        yield c, db_path

    get_settings.cache_clear()
    invalidate_effective_settings_cache()


# --- F02: one timezone basis -------------------------------------------------

def test_offset_aware_timestamps_are_converted_to_utc():
    # APScheduler hands back local time with an offset while the database
    # stores naive UTC. Rendered side by side, a six-hour gap read as two.
    assert _to_utc_display("2026-09-03T01:25:02.906147-04:00") == "2026-09-03 05:25:02"


def test_naive_timestamps_pass_through_unchanged():
    assert _to_utc_display("2026-09-02 23:24:15") == "2026-09-02 23:24:15"


def test_unparseable_and_empty_timestamps_are_survivable():
    assert _to_utc_display("") is None
    assert _to_utc_display(None) is None
    assert _to_utc_display("not a date") == "not a date"


# --- F19: a bad credential is a 401, not a 500 -------------------------------

def test_non_ascii_credentials_are_rejected_cleanly(client, monkeypatch):
    monkeypatch.setenv("AUTH_USERNAME", "admin")
    monkeypatch.setenv("AUTH_PASSWORD", "hunter2")
    from src.config import get_settings, invalidate_effective_settings_cache
    get_settings.cache_clear()
    invalidate_effective_settings_cache()

    c, _ = client
    # hmac.compare_digest raises TypeError on a str holding non-ASCII, which
    # turned this into an unhandled 500 that skipped the denial entirely.
    resp = c.get("/", auth=("ådmin", "påssword"))
    assert resp.status_code == 401

    assert c.get("/", auth=("admin", "hunter2")).status_code == 200


def test_health_stays_reachable_behind_auth(client, monkeypatch):
    monkeypatch.setenv("AUTH_USERNAME", "admin")
    monkeypatch.setenv("AUTH_PASSWORD", "hunter2")
    from src.config import get_settings, invalidate_effective_settings_cache
    get_settings.cache_clear()
    invalidate_effective_settings_cache()

    c, _ = client
    assert c.get("/api/health").status_code == 200


# --- F18: a stored password works, and is stored hashed ----------------------

def test_password_hash_round_trip():
    stored = auth.hash_password("correct horse")
    assert "correct horse" not in stored
    assert stored.startswith("pbkdf2_sha256$")
    assert auth.verify_password("correct horse", stored)
    assert not auth.verify_password("wrong horse", stored)
    assert not auth.verify_password("correct horse", "garbage")


def test_settings_configured_auth_is_enforced(client):
    c, db_path = client
    _set_settings(db_path, AUTH_USERNAME="chris",
                  AUTH_PASSWORD_HASH=auth.hash_password("s3cret"))
    from src.config import invalidate_effective_settings_cache
    invalidate_effective_settings_cache()

    assert c.get("/", auth=("chris", "nope")).status_code == 401
    assert c.get("/", auth=("chris", "s3cret")).status_code == 200


# --- F16: cross-origin writes are refused ------------------------------------

def test_cross_site_post_is_blocked(client):
    c, _ = client
    resp = c.post(
        "/api/ignore-path", data={"pattern": "/evil"},
        headers={"Sec-Fetch-Site": "cross-site"},
    )
    assert resp.status_code == 403


def test_cross_origin_post_is_blocked(client):
    c, _ = client
    resp = c.post(
        "/api/ignore-path", data={"pattern": "/evil"},
        headers={"Origin": "http://evil.example"},
    )
    assert resp.status_code == 403


def test_same_origin_post_is_allowed(client):
    c, _ = client
    resp = c.post(
        "/api/ignore-path", data={"pattern": "/anime/kids"},
        headers={"Sec-Fetch-Site": "same-origin"},
    )
    assert resp.status_code == 200
    assert "/anime/kids" in resp.text


def test_the_sonarr_webhook_is_exempt(client):
    c, _ = client
    # Sonarr is not a browser and sends no Origin; it authenticates with its
    # own shared secret instead.
    resp = c.post(
        "/api/webhook/sonarr",
        json={"eventType": "Test"},
        headers={"Sec-Fetch-Site": "cross-site"},
    )
    assert resp.status_code == 200


def test_reads_are_never_blocked(client):
    c, _ = client
    assert c.get("/", headers={"Sec-Fetch-Site": "cross-site"}).status_code == 200
