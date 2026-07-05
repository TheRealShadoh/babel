import pytest

from src.db.database import init_db, get_db
from src.db import models


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "models_test.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


@pytest.mark.asyncio
async def test_settings_roundtrip(db):
    assert await models.get_setting(db, "SONARR_URL") is None
    await models.set_setting(db, "SONARR_URL", "http://sonarr:8989")
    assert await models.get_setting(db, "SONARR_URL") == "http://sonarr:8989"

    # Upsert overwrites
    await models.set_setting(db, "SONARR_URL", "http://new-sonarr:8989")
    assert await models.get_setting(db, "SONARR_URL") == "http://new-sonarr:8989"

    all_settings = await models.get_all_settings(db)
    assert all_settings["SONARR_URL"] == "http://new-sonarr:8989"


@pytest.mark.asyncio
async def test_ignored_paths_add_remove(db):
    assert await models.get_ignored_paths(db) == []

    await models.add_ignored_path(db, "specials", note="skip specials")
    paths = await models.get_ignored_paths(db)
    assert len(paths) == 1
    assert paths[0]["pattern"] == "specials"
    assert paths[0]["note"] == "skip specials"

    # Duplicate pattern is ignored (UNIQUE constraint, INSERT OR IGNORE)
    await models.add_ignored_path(db, "specials")
    assert len(await models.get_ignored_paths(db)) == 1

    await models.remove_ignored_path(db, paths[0]["id"])
    assert await models.get_ignored_paths(db) == []


@pytest.mark.asyncio
async def test_is_path_ignored_case_insensitive_substring(db):
    await models.add_ignored_path(db, "Specials")
    assert await models.is_path_ignored(db, "/media/show/specials/ep1.mkv")
    assert await models.is_path_ignored(db, "/media/SHOW/SPECIALS/ep1.mkv")
    assert not await models.is_path_ignored(db, "/media/show/season1/ep1.mkv")


@pytest.mark.asyncio
async def test_series_exclusion_toggle(db):
    await models.upsert_series(db, 1, "Show A")
    assert not await models.is_series_excluded(db, 1)

    await models.set_series_excluded(db, 1, True)
    assert await models.is_series_excluded(db, 1)

    await models.set_series_excluded(db, 1, False)
    assert not await models.is_series_excluded(db, 1)


@pytest.mark.asyncio
async def test_series_filtered_pagination_and_search(db):
    for i in range(1, 6):
        await models.upsert_series(db, i, f"Show {i}")
        await models.update_series_counts(db, i)

    page1, total = await models.get_series_filtered(db, page=1, per_page=2)
    assert total == 5
    assert len(page1) == 2

    matched, total_matched = await models.get_series_filtered(db, search="Show 3")
    assert total_matched == 1
    assert matched[0]["title"] == "Show 3"


@pytest.mark.asyncio
async def test_update_series_counts_derives_status(db):
    await models.upsert_series(db, 1, "Show A")
    await models.upsert_episode(db, 101, 1, 1, 1, "Ep1", "/media/e1.mkv", 1000)
    await models.upsert_episode(db, 102, 1, 1, 2, "Ep2", "/media/e2.mkv", 2000)
    await models.update_episode_status(db, 101, "DUBBED")
    await models.update_episode_status(db, 102, "SUB_ONLY")

    await models.update_series_counts(db, 1)
    series = await models.get_series(db, 1)
    assert series["dub_status"] == "PARTIAL"
    assert series["dubbed_count"] == 1
    assert series["sub_only_count"] == 1
