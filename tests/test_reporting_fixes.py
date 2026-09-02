"""Counts, pagination, settings casting and API-call volume."""

import asyncio

import pytest

from src.config import get_effective_settings, invalidate_effective_settings_cache
from src.db import models
from src.db.database import init_db, get_db
from src.scanner.sonarr import SonarrClient


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "reporting_test.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


# --- F09: missing episodes are counted and reported --------------------------

@pytest.mark.asyncio
async def test_missing_episodes_are_counted_separately(db):
    # Half the season is downloaded and fully dubbed; the rest has no file.
    # The card used to divide by total_episodes and read "12 / 24 dubbed"
    # while the badge — computed against the downloaded half — said DUBBED.
    await models.upsert_series(db, 1, "Show A", "/media/show-a")
    for i in range(1, 13):
        await models.upsert_episode(db, 100 + i, 1, 1, i, f"Ep{i}", f"/media/a/e{i}.mkv", 500)
        await models.update_episode_status(db, 100 + i, "DUBBED")
    for i in range(13, 25):
        await models.upsert_episode(db, 100 + i, 1, 1, i, f"Ep{i}", None, None)
        await models.update_episode_status(db, 100 + i, "MISSING")
    await models.update_series_counts(db, 1)

    series = await models.get_series(db, 1)
    assert series["dub_status"] == "DUBBED"
    assert series["total_episodes"] == 24
    assert series["missing_count"] == 12
    downloaded = series["total_episodes"] - series["missing_count"]
    assert series["dubbed_count"] == downloaded, "the fraction must agree with the badge"


@pytest.mark.asyncio
async def test_status_change_is_reported_so_syncs_can_be_skipped(db):
    await models.upsert_series(db, 1, "Show A", "/media/show-a")
    await models.upsert_episode(db, 101, 1, 1, 1, "Ep1", "/media/a/e1.mkv", 500)
    await models.update_episode_status(db, 101, "SUB_ONLY")

    assert await models.update_series_counts(db, 1) is True
    assert await models.update_series_counts(db, 1) is False, "an unchanged pass must say so"

    await models.update_episode_status(db, 101, "DUBBED")
    assert await models.update_series_counts(db, 1) is True


# --- F11: a dub lookup is not the last scan ----------------------------------

@pytest.mark.asyncio
async def test_dub_lookup_runs_are_not_reported_as_the_last_scan(db):
    scan_id = await models.start_scan_log(db, kind="scan")
    await models.complete_scan_log(db, scan_id, 5, 0, 0, "completed")
    scan_row = await models.get_scan_logs(db)
    scan_time = scan_row[0]["started_at"]

    lookup_id = await models.start_scan_log(db, kind="dub_lookup")
    await models.complete_scan_log(db, lookup_id, 196, 0, 0, "completed", "Dub lookup: ...")

    stats = await models.get_overview_stats(db)
    assert stats["last_scan_time"] == scan_time


# --- F14: search terms are matched literally ---------------------------------

@pytest.mark.asyncio
async def test_like_wildcards_in_a_search_term_are_literal(db):
    await models.upsert_series(db, 1, "100% Pascal-sensei", "/media/a")
    await models.upsert_series(db, 2, "Another Show", "/media/b")

    matched, count = await models.get_series_filtered(db, search="100%")
    assert count == 1 and matched[0]["id"] == 1

    # A bare % is a literal now, so it finds the one title containing it
    # rather than acting as a wildcard over the whole library.
    matched, count = await models.get_series_filtered(db, search="%")
    assert count == 1 and matched[0]["id"] == 1

    _, count = await models.get_series_filtered(db, search="_nother")
    assert count == 0, "a bare _ must not stand in for any character"


# --- F13: an out-of-range page returns the last page, with rows --------------

@pytest.mark.asyncio
async def test_count_query_is_independent_of_the_page_window(db):
    for i in range(1, 46):
        await models.upsert_series(db, i, f"Show {i:02d}", f"/media/{i}")

    _, total = await models.get_series_filtered(db, page=1, per_page=0)
    assert total == 45

    last_page, _ = await models.get_series_filtered(db, page=2, per_page=30)
    assert len(last_page) == 15


# --- F29: settings are cast, and unusable values are ignored -----------------

def test_settings_are_cast_to_the_type_of_their_default(tmp_path, monkeypatch):
    db_path = str(tmp_path / "cfg_test.db")
    monkeypatch.setenv("DB_PATH", db_path)
    from src.config import get_settings
    get_settings.cache_clear()
    invalidate_effective_settings_cache()

    async def _go():
        await init_db(db_path)
        db = await get_db(db_path)
        try:
            await models.set_setting(db, "SCAN_INTERVAL_HOURS", "4")
            await models.set_setting(db, "FFPROBE_TIMEOUT", "45.5")
            await models.set_setting(db, "SEARCH_RATE_LIMIT", "not a number")
        finally:
            await db.close()
        invalidate_effective_settings_cache()
        return await get_effective_settings()

    cfg = asyncio.run(_go())
    assert cfg["SCAN_INTERVAL_HOURS"] == 4
    # Floats were never cast at all, so a stored timeout arrived as a string
    # and only failed later, at the comparison.
    assert cfg["FFPROBE_TIMEOUT"] == 45.5
    assert isinstance(cfg["FFPROBE_TIMEOUT"], float)
    # An unusable value keeps the default instead of silently disappearing.
    assert cfg["SEARCH_RATE_LIMIT"] == 5

    get_settings.cache_clear()
    invalidate_effective_settings_cache()


# --- F21: unchanged series are not rewritten in Sonarr -----------------------

@pytest.mark.asyncio
async def test_tag_sync_skips_series_whose_tags_already_match(monkeypatch):
    client = SonarrClient("http://sonarr.invalid", "key")
    try:
        async def fake_get_tags():
            return [
                {"id": 1, "label": "babel:dubbed"},
                {"id": 2, "label": "babel:partial-dub"},
                {"id": 3, "label": "babel:sub-only"},
            ]

        written: list[int] = []

        async def fake_set_series_tags(series_id, add, remove):
            written.append(series_id)
            return True

        monkeypatch.setattr(client, "get_tags", fake_get_tags)
        monkeypatch.setattr(client, "set_series_tags", fake_set_series_tags)

        result = await client.sync_dub_tags([
            # Already correct — the "remove the other two Babel tags" list is
            # never empty, so this used to take a GET and a full PUT anyway.
            {"sonarr_id": 10, "dub_status": "DUBBED", "current_tags": [1, 99]},
            # Genuinely wrong: still tagged sub-only.
            {"sonarr_id": 11, "dub_status": "DUBBED", "current_tags": [3]},
            # No tag information — must still be written, not assumed correct.
            {"sonarr_id": 12, "dub_status": "PARTIAL"},
        ])

        assert written == [11, 12]
        assert result["skipped"] == 1
        assert result["tagged"] == 2
    finally:
        await client.close()
