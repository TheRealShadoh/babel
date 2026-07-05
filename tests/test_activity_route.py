from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.db import models


QUEUE_ITEM = {
    "episodeId": 101,
    "seriesId": 1,
    "title": "Some.Release.Title.1080p",
    "series": {"title": "Show A"},
    "episode": {"seasonNumber": 1, "episodeNumber": 1},
    "size": 1000,
    "sizeleft": 500,
    "trackedDownloadState": "downloading",
    "trackedDownloadStatus": "ok",
    "statusMessages": [],
    "timeleft": "00:05:00",
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    """A router-only test app — skips src.main's lifespan (and its
    module-level scheduler singleton, which can't restart cleanly across
    multiple TestClient instances in the same test session) since these
    tests only exercise HTTP routes, not scheduling."""
    db_path = str(tmp_path / "activity_test.db")
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("SONARR_URL", "http://fake-sonarr:8989")
    monkeypatch.setenv("SONARR_API_KEY", "fake-key")

    from src.config import get_settings
    get_settings.cache_clear()

    import asyncio
    from pathlib import Path
    from fastapi import FastAPI
    from fastapi.templating import Jinja2Templates
    from src.db.database import init_db
    from src.web.routes import router

    asyncio.run(init_db(db_path))

    app = FastAPI()
    app.include_router(router)
    templates_dir = Path("src/web/templates")
    app.state.templates = Jinja2Templates(directory=str(templates_dir))

    with TestClient(app) as c:
        yield c

    get_settings.cache_clear()


def test_activity_html_renders_via_jinja_partial(client, tmp_path):
    async def seed():
        from src.db.database import get_db
        db = await get_db(str(tmp_path / "activity_test.db"))
        await models.upsert_series(db, 1, "Show A", "/media/show-a")
        await models.upsert_episode(db, 101, 1, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000)
        await models.create_upgrade_record(db, 101, 1, "Show A", 1, 1, 1000)
        await db.close()

    import asyncio
    asyncio.run(seed())

    with patch("src.web.routes.SonarrClient") as MockSonarr:
        instance = MockSonarr.return_value
        instance.get_queue = AsyncMock(return_value=[QUEUE_ITEM])
        instance.close = AsyncMock()

        resp = client.get("/api/activity/html")

    assert resp.status_code == 200
    html = resp.text
    assert "Show A" in html
    assert "S01E01" in html
    assert "1 Downloading" not in html  # stat-num renders separately from label
    assert 'class="stat-num"' in html
    assert "download-grid" in html


def test_activity_json_matches_html_data(client, tmp_path):
    async def seed():
        from src.db.database import get_db
        db = await get_db(str(tmp_path / "activity_test.db"))
        await models.upsert_series(db, 1, "Show A", "/media/show-a")
        await models.upsert_episode(db, 101, 1, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000)
        await models.create_upgrade_record(db, 101, 1, "Show A", 1, 1, 1000)
        await db.close()

    import asyncio
    asyncio.run(seed())

    with patch("src.web.routes.SonarrClient") as MockSonarr:
        instance = MockSonarr.return_value
        instance.get_queue = AsyncMock(return_value=[QUEUE_ITEM])
        instance.close = AsyncMock()

        resp = client.get("/api/activity")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data["queue"]) == 1
    assert data["queue"][0]["series"] == "Show A"
    assert data["queue"][0]["episode"] == "S01E01"
    assert data["stats"]["downloading"] == 1


def test_activity_html_escapes_untrusted_release_title(client, tmp_path):
    """Regression test: release titles come from Sonarr's queue (reflecting
    untrusted indexer data) and must be HTML-escaped, not interpolated raw."""
    async def seed():
        from src.db.database import get_db
        db = await get_db(str(tmp_path / "activity_test.db"))
        await models.upsert_series(db, 1, "Show A", "/media/show-a")
        await models.upsert_episode(db, 101, 1, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000)
        await models.create_upgrade_record(db, 101, 1, "Show A", 1, 1, 1000)
        await db.close()

    import asyncio
    asyncio.run(seed())

    malicious_item = dict(QUEUE_ITEM)
    malicious_item["title"] = "<script>alert(1)</script>"

    with patch("src.web.routes.SonarrClient") as MockSonarr:
        instance = MockSonarr.return_value
        instance.get_queue = AsyncMock(return_value=[malicious_item])
        instance.close = AsyncMock()

        resp = client.get("/api/activity/html")

    assert "<script>alert(1)</script>" not in resp.text
    assert "&lt;script&gt;" in resp.text
