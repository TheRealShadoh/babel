"""A restarting Sonarr must not erase the library's cached classifications.

Field incident: Sonarr answered /series normally but /episodefile with []
for every series for the few seconds after a restart. One scan in that
window overwrote 991 series' worth of DUBBED/SUB_ONLY episodes to MISSING;
the following scan (Plex down, media unmounted) re-probed them all to
UNKNOWN, and the dashboard went from a classified library to zeroes.
"""

from datetime import timedelta

import pytest

from src.db.database import init_db, get_db
from src.db import models
from src.scanner.engine import RateLimiter, _scan_with_sonarr

from tests.fakes import FakeSonarr, FakePlex, base_cfg, sonarr_episode, sonarr_file


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "babel_test.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


async def run_scan(db, sonarr, plex):
    scan_id = await models.start_scan_log(db)
    return await _scan_with_sonarr(
        db, base_cfg(), sonarr, plex, RateLimiter(1000), timedelta(days=7), "eng", scan_id,
    )


def healthy_library(n=15):
    """*n* series, one dubbed file each, the way a normal scan sees them."""
    series, episodes, files, tracks = [], {}, {}, {}
    for i in range(1, n + 1):
        ep_id = i * 100 + 1
        path = f"/media/show-{i}/e1.mkv"
        series.append({"id": i, "title": f"Show {i}", "path": f"/media/show-{i}", "poster_url": None})
        episodes[i] = [sonarr_episode(ep_id, 1, 1, "Ep1")]
        files[i] = [sonarr_file(ep_id, path, 1000)]
        tracks[path] = [{"language": "eng", "codec": "aac"}]
    return series, episodes, files, FakePlex(tracks)


async def seed_classified_library(db, n=15):
    series, episodes, files, plex = healthy_library(n)
    result = await run_scan(db, FakeSonarr(series, episodes, files), plex)
    assert result["dubbed"] == n
    return series, episodes


async def statuses(db):
    return sorted((s["id"], s["dub_status"]) for s in await models.get_all_series(db))


@pytest.mark.asyncio
async def test_no_files_but_episodes_marked_downloaded_is_skipped(db):
    """Sonarr contradicting itself (hasFile=True, /episodefile empty) is transient."""
    series, episodes = await seed_classified_library(db)
    restarting = FakeSonarr(series, episodes, files_by_series={})  # every series: []

    result = await run_scan(db, restarting, FakePlex({}))

    assert all(status == "DUBBED" for _, status in await statuses(db))
    assert result["errors"] == 15
    # No reconciliation happened, so nothing was pruned either.
    assert len(await models.get_all_series(db)) == 15


@pytest.mark.asyncio
async def test_most_of_the_library_losing_files_at_once_is_refused(db):
    """Even a self-consistent 'no files anywhere' answer is not believed wholesale."""
    series, episodes = await seed_classified_library(db)
    # Sonarr is consistent this time: hasFile=False everywhere, no files.
    unloaded = {sid: [sonarr_episode(eps[0]["id"], 1, 1, "Ep1", has_file=False)]
                for sid, eps in episodes.items()}
    restarting = FakeSonarr(series, unloaded, files_by_series={})

    result = await run_scan(db, restarting, FakePlex({}))

    assert all(status == "DUBBED" for _, status in await statuses(db))
    assert result["errors"] >= 1
    # The episode rows and their file paths are exactly as they were.
    ep = await models.get_episode(db, 101)
    assert ep["dub_status"] == "DUBBED" and ep["file_path"] == "/media/show-1/e1.mkv"


@pytest.mark.asyncio
async def test_one_series_genuinely_losing_its_files_is_still_recorded(db):
    series, episodes = await seed_classified_library(db)
    episodes[3] = [sonarr_episode(301, 1, 1, "Ep1", has_file=False)]
    _, _, files, plex = healthy_library()
    files[3] = []
    sonarr = FakeSonarr(series, episodes, files)

    result = await run_scan(db, sonarr, plex)

    assert result["errors"] == 0
    by_id = dict(await statuses(db))
    assert by_id[3] == "EMPTY"
    assert all(by_id[i] == "DUBBED" for i in range(1, 16) if i != 3)
    assert (await models.get_episode(db, 301))["dub_status"] == "MISSING"


@pytest.mark.asyncio
async def test_small_library_is_exempt_from_the_floor(db):
    """Dropping 2 of 3 shows is a plausible thing to do on purpose."""
    series, episodes = await seed_classified_library(db, n=3)
    for sid in (1, 2):
        episodes[sid] = [sonarr_episode(episodes[sid][0]["id"], 1, 1, "Ep1", has_file=False)]
    _, _, files, plex = healthy_library(3)
    files[1] = files[2] = []

    await run_scan(db, FakeSonarr(series, episodes, files), plex)

    assert dict(await statuses(db)) == {1: "EMPTY", 2: "EMPTY", 3: "DUBBED"}


@pytest.mark.asyncio
async def test_empty_episode_list_for_a_known_series_is_skipped(db):
    series, episodes = await seed_classified_library(db)
    episodes[1] = []
    _, _, files, plex = healthy_library()
    files[1] = []

    result = await run_scan(db, FakeSonarr(series, episodes, files), plex)

    assert result["errors"] == 1
    assert (await models.get_episode(db, 101))["dub_status"] == "DUBBED"


@pytest.mark.asyncio
async def test_brand_new_series_without_files_is_simply_empty(db):
    """Nothing cached means nothing to protect — the old behaviour stands."""
    series = [{"id": 1, "title": "New Show", "path": "/media/new", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1", has_file=False)]}

    result = await run_scan(db, FakeSonarr(series, episodes, {1: []}), FakePlex({}))

    assert result["errors"] == 0
    assert dict(await statuses(db)) == {1: "EMPTY"}


@pytest.mark.asyncio
async def test_refused_pass_does_not_prune_series_either(db):
    """A pass that could not be trusted for files is not trusted for orphans."""
    series, episodes = await seed_classified_library(db)
    unloaded = {sid: [sonarr_episode(eps[0]["id"], 1, 1, "Ep1", has_file=False)]
                for sid, eps in episodes.items()}
    # Sonarr also "forgot" the last three series entirely this pass.
    restarting = FakeSonarr(series[:12], unloaded, files_by_series={})

    await run_scan(db, restarting, FakePlex({}))

    assert len(await models.get_all_series(db)) == 15
