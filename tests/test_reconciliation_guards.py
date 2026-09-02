"""The orphan-cleanup pass must never act on a listing it did not receive.

Every Sonarr fetch used to swallow its error and return an empty list, so a
single timeout made the scan believe the library was empty and reconcile
against nothing — a bare DELETE FROM series whose cascades took every episode,
audio track, search record and upgrade record with it.
"""

from datetime import timedelta

import pytest

from src.db.database import init_db, get_db
from src.db import models
from src.scanner.engine import RateLimiter, _scan_with_sonarr

from tests.fakes import FakeSonarr, base_cfg, sonarr_episode, sonarr_file


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "reconcile_test.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


async def _seed(db, count=4):
    for i in range(1, count + 1):
        await models.upsert_series(db, i, f"Show {i}", f"/media/show-{i}")
        await models.upsert_episode(db, 100 + i, i, 1, 1, "Ep1", f"/media/show-{i}/e1.mkv", 500)
        await models.update_episode_status(db, 100 + i, "DUBBED")
        await models.update_series_counts(db, i)


async def _count(db, table):
    async with db.execute(f"SELECT COUNT(*) AS c FROM {table}") as cur:
        return (await cur.fetchone())["c"]


async def _run(db, cfg, sonarr):
    scan_id = await models.start_scan_log(db)
    return await _scan_with_sonarr(
        db, cfg, sonarr, None, RateLimiter(1000), timedelta(days=7), "eng", scan_id
    )


@pytest.mark.asyncio
async def test_empty_keep_set_never_deletes_everything(db):
    await _seed(db)
    deleted = await models.delete_series_not_in(db, set())
    assert deleted == 0
    assert await _count(db, "series") == 4
    assert await _count(db, "episodes") == 4


@pytest.mark.asyncio
async def test_a_mass_purge_is_refused(db):
    await _seed(db, count=20)
    # Keeping one of twenty is not a deletion anyone performed on purpose.
    deleted = await models.delete_series_not_in(db, {1})
    assert deleted == 0
    assert await _count(db, "series") == 20


@pytest.mark.asyncio
async def test_a_proportionate_prune_still_happens(db):
    await _seed(db, count=20)
    keep = set(range(1, 19))
    deleted = await models.delete_series_not_in(db, keep)
    assert deleted == 2
    assert await _count(db, "series") == 18


@pytest.mark.asyncio
async def test_small_library_may_shrink_freely(db):
    await _seed(db, count=4)
    assert await models.delete_series_not_in(db, {1}) == 3
    assert await _count(db, "series") == 1


@pytest.mark.asyncio
async def test_episode_cleanup_refuses_an_empty_keep_set(db):
    await _seed(db, count=1)
    await models.delete_episodes_not_in(db, 1, set())
    assert await _count(db, "episodes") == 1
    # Explicit consent is required to clear a series' episodes.
    await models.delete_episodes_not_in(db, 1, set(), allow_empty=True)
    assert await _count(db, "episodes") == 0


@pytest.mark.asyncio
async def test_failed_series_listing_aborts_without_touching_data(db):
    await _seed(db)
    sonarr = FakeSonarr(fail_series=True)
    result = await _run(db, base_cfg(), sonarr)

    assert result["status"] == "failed"
    assert await _count(db, "series") == 4
    assert await _count(db, "episodes") == 4


@pytest.mark.asyncio
async def test_one_failed_episode_fetch_suppresses_orphan_cleanup(db):
    await _seed(db, count=4)
    # Sonarr now reports only series 1, but the fetch for it failed. The
    # missing 2-4 must survive: this pass never saw a complete listing.
    sonarr = FakeSonarr(
        series=[{"id": 1, "title": "Show 1", "path": "/media/show-1", "poster_url": None}],
        episodes_by_series={1: [sonarr_episode(101, 1, 1, "Ep1")]},
        files_by_series={1: [sonarr_file(101, "/media/show-1/e1.mkv", 500)]},
        fail_episodes_for={1},
    )
    await _run(db, base_cfg(), sonarr)
    assert await _count(db, "series") == 4


@pytest.mark.asyncio
async def test_a_clean_pass_still_prunes(db):
    await _seed(db, count=4)
    sonarr = FakeSonarr(
        series=[
            {"id": i, "title": f"Show {i}", "path": f"/media/show-{i}", "poster_url": None}
            for i in (1, 2, 3)
        ],
        episodes_by_series={i: [sonarr_episode(100 + i, 1, 1, "Ep1")] for i in (1, 2, 3)},
        files_by_series={
            i: [sonarr_file(100 + i, f"/media/show-{i}/e1.mkv", 500)] for i in (1, 2, 3)
        },
    )
    await _run(db, base_cfg(), sonarr)
    assert await _count(db, "series") == 3


@pytest.mark.asyncio
async def test_series_that_lost_all_its_files_stops_reporting_dubbed(db):
    await _seed(db, count=4)
    sonarr = FakeSonarr(
        series=[
            {"id": i, "title": f"Show {i}", "path": f"/media/show-{i}", "poster_url": None}
            for i in (1, 2, 3, 4)
        ],
        episodes_by_series={1: [sonarr_episode(101, 1, 1, "Ep1", has_file=False)]},
        files_by_series={1: []},
    )
    await _run(db, base_cfg(), sonarr)
    series = await models.get_series(db, 1)
    assert series["dub_status"] == "EMPTY"
    assert series["dubbed_count"] == 0
