from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.db import models


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Router-only test app — see test_activity_route.py for why this skips
    src.main's lifespan/scheduler."""
    db_path = str(tmp_path / "webhook_test.db")
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("SONARR_URL", "")

    from src.config import get_settings, invalidate_effective_settings_cache
    get_settings.cache_clear()
    invalidate_effective_settings_cache()

    import asyncio
    from pathlib import Path
    from fastapi import FastAPI
    from fastapi.templating import Jinja2Templates
    from src.db.database import init_db
    from src.web.routes import router

    asyncio.run(init_db(db_path))

    app = FastAPI()
    app.include_router(router)
    app.state.templates = Jinja2Templates(directory=str(Path("src/web/templates")))

    with TestClient(app) as c:
        yield c, db_path

    get_settings.cache_clear()
    invalidate_effective_settings_cache()


def _download_payload(series_id=1, episode_id=101, file_path="/media/show-a/e1.mkv", size=5000):
    return {
        "eventType": "Download",
        "series": {"id": series_id, "title": "Show A"},
        "episodes": [{"id": episode_id, "seasonNumber": 1, "episodeNumber": 1, "title": "Ep1"}],
        "episodeFile": {"path": file_path, "relativePath": "e1.mkv", "size": size},
    }


def test_webhook_unknown_event_is_ignored(client):
    c, _ = client
    resp = c.post("/api/webhook/sonarr", json={"eventType": "Test"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"


def test_webhook_series_not_tracked(client):
    c, _ = client
    resp = c.post("/api/webhook/sonarr", json=_download_payload(series_id=999))
    assert resp.status_code == 200
    assert resp.json()["status"] == "series not tracked"


def test_webhook_rejects_wrong_apikey(client, tmp_path):
    c, db_path = client

    async def seed():
        from src.db.database import get_db
        db = await get_db(db_path)
        await models.set_setting(db, "WEBHOOK_SECRET", "correct-secret")
        await db.close()

    import asyncio
    asyncio.run(seed())
    from src.config import invalidate_effective_settings_cache
    invalidate_effective_settings_cache()

    resp = c.post("/api/webhook/sonarr?apikey=wrong", json=_download_payload())
    assert resp.status_code == 401


def test_webhook_accepts_correct_apikey_and_processes_download(client, tmp_path):
    c, db_path = client

    async def seed():
        from src.db.database import get_db
        db = await get_db(db_path)
        await models.set_setting(db, "WEBHOOK_SECRET", "correct-secret")
        await models.upsert_series(db, 1, "Show A", "/media/show-a")
        await models.upsert_episode(db, 101, 1, 1, 1, "Ep1", "/media/show-a/old.mkv", 1000)
        await models.update_episode_status(db, 101, "SUB_ONLY")
        await models.create_upgrade_record(db, 101, 1, "Show A", 1, 1, 1000)
        await db.close()

    import asyncio
    asyncio.run(seed())
    from src.config import invalidate_effective_settings_cache
    invalidate_effective_settings_cache()

    with patch("src.scanner.ffprobe.get_audio_tracks", new=AsyncMock(return_value=[
        {"language": "eng", "codec": "aac"}
    ])):
        resp = c.post("/api/webhook/sonarr?apikey=correct-secret", json=_download_payload())

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "processed"
    assert data["episodes_updated"] == 1

    async def check():
        from src.db.database import get_db
        db = await get_db(db_path)
        ep = await models.get_episode(db, 101)
        history = await models.get_upgrade_history(db)
        await db.close()
        return ep, history

    ep, history = asyncio.run(check())
    assert ep["dub_status"] == "DUBBED"
    assert ep["file_path"] == "/media/show-a/e1.mkv"  # absolute path, not relativePath
    assert history[0]["result"] == "success"
