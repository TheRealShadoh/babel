"""Regression tests for the non-data-safety findings of the 1.2.0 review."""

import asyncio
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from src import notifications
from src.config import translate_path
from src.db import models
from src.db.database import get_db, init_db
from src.scanner import dub_lookup, engine, ffprobe
from src.scanner.engine import RateLimiter, _scan_with_sonarr
from src.scanner.jellyfin import JellyfinClient
from src.web import auth
from src.web.routes import router

from tests.fakes import FakeSonarr, FakePlex, base_cfg, sonarr_episode, sonarr_file, plex_show, plex_episode


# ---------------------------------------------------------------------------
# Path prefixes match on path boundaries
# ---------------------------------------------------------------------------


def test_path_prefix_only_matches_whole_components():
    cfg = {"SONARR_PATH_PREFIX": "/tv", "LOCAL_PATH_PREFIX": "/media"}
    assert translate_path("/tv/Show/e1.mkv", "local", cfg) == "/media/Show/e1.mkv"
    assert translate_path("/tv", "local", cfg) == "/media"
    # "/tv-anime" is a different directory, not a longer spelling of "/tv".
    assert translate_path("/tv-anime/Show/e1.mkv", "local", cfg) == "/tv-anime/Show/e1.mkv"
    # Trailing slashes on either side do not produce "//".
    cfg = {"SONARR_PATH_PREFIX": "/tv/", "LOCAL_PATH_PREFIX": "/media/"}
    assert translate_path("/tv/Show/e1.mkv", "local", cfg) == "/media/Show/e1.mkv"


# ---------------------------------------------------------------------------
# ffprobe only ever sees absolute local paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ffprobe_refuses_urls_and_options(monkeypatch):
    async def must_not_spawn(*args, **kwargs):
        raise AssertionError(f"ffprobe was spawned with {args}")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", must_not_spawn)

    assert await ffprobe.get_audio_tracks("http://169.254.169.254/latest/meta-data") is None
    assert await ffprobe.get_audio_tracks("-loglevel") is None
    assert await ffprobe.get_audio_tracks("relative/file.mkv") is None


# ---------------------------------------------------------------------------
# Auth: hashing off the loop, environment wins
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_password_verification_is_cached_and_off_loop(monkeypatch):
    calls = []
    real_verify = auth.verify_password

    def counting_verify(password, stored):
        calls.append(password)
        return real_verify(password, stored)

    monkeypatch.setattr(auth, "verify_password", counting_verify)
    auth._verified.clear()
    stored = auth.hash_password("hunter2")

    assert await auth._verify_password_cached("hunter2", stored) is True
    assert await auth._verify_password_cached("hunter2", stored) is True
    assert calls == ["hunter2"]  # second check served from the cache

    assert await auth._verify_password_cached("wrong", stored) is False
    assert await auth._verify_password_cached("wrong", stored) is False
    assert calls == ["hunter2", "wrong", "wrong"]  # failures are never cached


@pytest.mark.asyncio
async def test_env_username_wins_over_a_cleared_settings_username(monkeypatch):
    monkeypatch.setenv("AUTH_USERNAME", "admin")
    monkeypatch.setenv("AUTH_PASSWORD", "pw")

    async def cleared_in_ui():
        return {"AUTH_USERNAME": "", "AUTH_PASSWORD": "pw", "AUTH_PASSWORD_HASH": ""}

    monkeypatch.setattr("src.config.get_effective_settings", cleared_in_ui)
    username, password, _ = await auth._credentials()
    assert (username, password) == ("admin", "pw")


# ---------------------------------------------------------------------------
# Web routes
# ---------------------------------------------------------------------------


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


SAME_ORIGIN = {"Sec-Fetch-Site": "same-origin"}


def _store(db_path, **values):
    async def run():
        db = await get_db(db_path)
        try:
            for k, v in values.items():
                await models.set_setting(db, k, v)
        finally:
            await db.close()
    asyncio.run(run())


def test_connection_test_will_not_send_the_stored_key_to_a_new_url(web, monkeypatch):
    client, db_path = web
    _store(db_path, SONARR_URL="http://sonarr:8989", SONARR_API_KEY="secret")

    contacted = []

    class Spy(engine.SonarrClient):
        def __init__(self, url, api_key):
            contacted.append((url, api_key))
            super().__init__(url, api_key)

        async def test_connection(self):
            return True, "Connected"

    monkeypatch.setattr("src.web.routes.SonarrClient", Spy)

    # New URL, blank key: refused before any request is made.
    resp = client.post("/api/test-sonarr", data={"SONARR_URL": "http://evil:8989"}, headers=SAME_ORIGIN)
    assert resp.status_code == 200
    assert "Enter the API key" in resp.text
    assert contacted == []

    # Same URL, blank key: the stored key may be used.
    resp = client.post("/api/test-sonarr", data={"SONARR_URL": "http://sonarr:8989/"}, headers=SAME_ORIGIN)
    assert "Connected" in resp.text
    assert contacted == [("http://sonarr:8989/", "secret")]


def test_per_episode_search_button_renders_a_status(web):
    client, _ = web
    resp = client.post("/api/search/101", headers=SAME_ORIGIN)
    assert resp.status_code == 200
    assert "Sonarr is not configured" in resp.text and "badge" in resp.text


def test_discord_webhook_is_treated_as_a_secret(web):
    client, db_path = web
    _store(db_path, DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/123/abc")
    page = client.get("/settings").text
    assert "webhooks/123/abc" not in page
    assert "Saved — leave blank to keep" in page


# ---------------------------------------------------------------------------
# Download status: one record per episode, history since the search only
# ---------------------------------------------------------------------------


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "t.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


@pytest.mark.asyncio
async def test_pending_upgrades_are_one_per_episode(db):
    await models.upsert_series(db, 1, "S", "/m")
    await models.upsert_episode(db, 101, 1, 1, 1, "E", "/m/e.mkv", 10)
    for _ in range(3):
        await models.create_upgrade_record(db, 101, 1, "S", 1, 1, 10)
    pending = await models.get_pending_upgrades(db)
    assert len(pending) == 1


@pytest.mark.asyncio
async def test_download_status_ignores_history_from_before_the_search(monkeypatch, tmp_path):
    db_path = str(tmp_path / "dl.db")
    await init_db(db_path)
    db = await get_db(db_path)
    try:
        await models.upsert_series(db, 1, "S", "/m")
        await models.upsert_episode(db, 101, 1, 1, 1, "E", "/m/e.mkv", 10)
        await models.create_upgrade_record(db, 101, 1, "S", 1, 1, 10)
        triggered = (await models.get_pending_upgrades(db))[0]["triggered_at"]
    finally:
        await db.close()

    class HistorySonarr:
        def __init__(self, url, key):
            pass

        async def get_queue(self):
            return []

        async def get_history_for_episode(self, episode_id, limit=5):
            # The episode's original import, long before Babel searched.
            return [{"eventType": "downloadFolderImported", "date": "2020-01-01T00:00:00Z"}]

        async def close(self):
            pass

    async def fake_cfg():
        return {"DB_PATH": db_path, "SONARR_URL": "http://s", "SONARR_API_KEY": "k"}

    monkeypatch.setattr(engine, "get_effective_settings", fake_cfg)
    monkeypatch.setattr(engine, "SonarrClient", HistorySonarr)

    summary = await engine.check_download_status()
    assert summary["imported"] == 0 and summary["no_results"] == 1

    # An import after the search does count.
    class LaterSonarr(HistorySonarr):
        async def get_history_for_episode(self, episode_id, limit=5):
            return [{"eventType": "downloadFolderImported", "date": "2099-01-01T00:00:00Z"}]

    monkeypatch.setattr(engine, "SonarrClient", LaterSonarr)
    summary = await engine.check_download_status()
    assert summary["imported"] == 1
    assert triggered  # sanity: the record carried a timestamp to compare against


# ---------------------------------------------------------------------------
# MyAnimeList matching never takes the first hit blindly
# ---------------------------------------------------------------------------


def _jikan(results):
    def handler(request):
        return httpx.Response(200, json={"data": results})
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _anime(mal_id, title, english=None, licensors=()):
    return {"mal_id": mal_id, "title": title, "title_english": english, "titles": [],
            "status": "Finished Airing", "aired": {}, "episodes": 12,
            "licensors": [{"name": n} for n in licensors]}


@pytest.mark.asyncio
async def test_unrelated_first_hit_is_not_used():
    client = _jikan([_anime(1, "Naruto: Shippuuden", "Naruto Shippuden", ["Viz Media"])])
    info = await dub_lookup.lookup_dub_info("Naruto", client=client)
    assert info["ok"] is True
    assert info["mal_id"] is None and info["dub_status"] == "unknown"


@pytest.mark.asyncio
async def test_close_title_still_matches():
    client = _jikan([_anime(41467, "Bleach: Sennen Kessen-hen", "Bleach: Thousand-Year Blood War", ["Viz Media"])])
    info = await dub_lookup.lookup_dub_info("Bleach - Thousand Year Blood War (2022)", client=client)
    assert info["mal_id"] == 41467


# ---------------------------------------------------------------------------
# Jellyfin collections are batched
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_jellyfin_collection_writes_are_batched():
    seen = []

    def handler(request):
        if request.url.path.startswith("/Collections/") and request.url.path.endswith("/Items"):
            seen.append(len(request.url.params["ids"].split(",")))
            return httpx.Response(204)
        return httpx.Response(200, json={"Items": [], "TotalRecordCount": 0})

    client = JellyfinClient("http://jf:8096", "k")
    client.client = httpx.AsyncClient(base_url="http://jf:8096", transport=httpx.MockTransport(handler))

    await client._collection_items("POST", "c1", [f"id{i}" for i in range(120)])
    assert seen == [50, 50, 20]


# ---------------------------------------------------------------------------
# A partial media-server read never prunes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_library_read_skips_orphan_cleanup(db):
    for key in (1, 2, 3):
        await models.upsert_series(db, -(key + 1000000), f"Show {key}", "/m")

    library = [plex_show(1, "Show 1", "/m", [plex_episode(11, 1, 1, "E", "/m/e1.mkv", 1,
                [{"language": "eng", "codec": "aac", "source": "plex"}], "DUBBED")])]
    plex = FakePlex({}, library_data=library)
    plex.partial = True  # e.g. a page of the Jellyfin listing timed out

    scan_id = await models.start_scan_log(db)
    await engine._scan_media_server_only(db, base_cfg(), plex, "eng", scan_id)

    assert len(await models.get_all_series(db)) == 3  # nothing pruned


# ---------------------------------------------------------------------------
# Discord embeds respect limits and surface rejections
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discord_description_is_truncated_and_rejections_logged(caplog):
    bodies = []

    def handler(request):
        bodies.append(request.read())
        return httpx.Response(400, json={"message": "Invalid Form Body"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with caplog.at_level("WARNING"):
        await notifications.send_discord_embed(
            "https://discord.com/api/webhooks/1/x", "t", "x" * 5000, client=client,
        )
    assert len(bodies) == 1
    assert b"x" * 4096 not in bodies[0]  # truncated below the 4096 cap
    assert "Discord rejected" in caplog.text


# ---------------------------------------------------------------------------
# Long series commit periodically so the write lock is not held for minutes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_long_series_commits_partway_through(db, monkeypatch):
    commits = []
    real_commit = db.commit

    async def counting_commit():
        commits.append(1)
        await real_commit()

    monkeypatch.setattr(db, "commit", counting_commit)

    n = engine._COMMIT_EVERY * 2 + 1
    series = [{"id": 1, "title": "Long", "path": "/m", "poster_url": None}]
    episodes = {1: [sonarr_episode(100 + i, 1, i, f"E{i}") for i in range(1, n + 1)]}
    files = {1: [sonarr_file(100 + i, f"/m/e{i}.mkv", 1) for i in range(1, n + 1)]}
    plex = FakePlex({f"/m/e{i}.mkv": [{"language": "eng", "codec": "aac"}] for i in range(1, n + 1)})

    scan_id = await models.start_scan_log(db)
    before = len(commits)
    await _scan_with_sonarr(db, base_cfg(), FakeSonarr(series, episodes, files), plex,
                            RateLimiter(1000), timedelta(days=7), "eng", scan_id)
    assert len(commits) - before >= 3  # two mid-series commits plus the series commit
