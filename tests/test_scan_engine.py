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


async def seed_series(db, series_id, title, path="/media/show"):
    await models.upsert_series(db, series_id, title, path)


async def seed_episode(db, series_id, ep_id, season, ep_num, title, file_path, file_size, dub_status, tracks=None):
    await models.upsert_episode(db, ep_id, series_id, season, ep_num, title, file_path, file_size)
    await models.update_episode_status(db, ep_id, dub_status)
    if tracks:
        await models.replace_audio_tracks(db, ep_id, tracks)


async def run_scan(db, cfg, sonarr, plex):
    scan_id = await models.start_scan_log(db)
    rate_limiter = RateLimiter(int(cfg.get("SEARCH_RATE_LIMIT", 1000)))
    cooldown = timedelta(days=int(cfg.get("SEARCH_COOLDOWN_DAYS", 7)))
    target_lang = cfg.get("TARGET_LANGUAGE", "eng")
    return await _scan_with_sonarr(db, cfg, sonarr, plex, rate_limiter, cooldown, target_lang, scan_id)


@pytest.mark.asyncio
async def test_fresh_dubbed_and_subonly_classification(db):
    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [
        sonarr_episode(101, 1, 1, "Ep1"),
        sonarr_episode(102, 1, 2, "Ep2"),
    ]}
    files = {1: [
        sonarr_file(101, "/media/show-a/e1.mkv", 1000),
        sonarr_file(102, "/media/show-a/e2.mkv", 2000),
    ]}
    plex_tracks = {
        "/media/show-a/e1.mkv": [{"language": "eng", "codec": "aac"}],
        "/media/show-a/e2.mkv": [{"language": "jpn", "codec": "aac"}],
    }

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex(plex_tracks)
    cfg = base_cfg()

    result = await run_scan(db, cfg, sonarr, plex)

    assert result["dubbed"] == 1
    assert result["sub_only"] == 1
    assert result["searches_triggered"] == 1
    assert sonarr.searched_ids == [[102]]

    ep1 = await models.get_episode(db, 101)
    ep2 = await models.get_episode(db, 102)
    assert ep1["dub_status"] == "DUBBED"
    assert ep2["dub_status"] == "SUB_ONLY"


@pytest.mark.asyncio
async def test_missing_file_marks_missing_and_skips_search(db):
    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1", has_file=False)]}
    files = {1: [sonarr_file(999, "/media/show-a/other.mkv", 500)]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({})
    cfg = base_cfg()

    result = await run_scan(db, cfg, sonarr, plex)

    ep1 = await models.get_episode(db, 101)
    assert ep1["dub_status"] == "MISSING"
    assert result["searches_triggered"] == 0
    assert sonarr.searched_ids == []


@pytest.mark.asyncio
async def test_cache_hit_never_builds_plex_index(db):
    """When every episode is already cached and unchanged, the scan must
    never touch Plex at all — this is the P2 fix for the eager full-library
    index rebuild that happened on every scan regardless of need."""
    await seed_series(db, 1, "Show A")
    await seed_episode(
        db, 1, 101, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000, "DUBBED",
        tracks=[{"language": "eng", "codec": "aac", "source": "plex"}],
    )

    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 1000)]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({"/media/show-a/e1.mkv": [{"language": "eng", "codec": "aac"}]})
    cfg = base_cfg()

    result = await run_scan(db, cfg, sonarr, plex)

    assert result["skipped_unchanged"] == 1
    assert plex.build_index_calls == 0, "Plex index should never build when nothing needs a lookup"


@pytest.mark.asyncio
async def test_lazy_plex_index_builds_once_when_needed(db):
    """One cached episode + one that needs a fresh lookup: Plex should
    build its index exactly once (lazily), not eagerly before the loop."""
    await seed_series(db, 1, "Show A")
    await seed_episode(
        db, 1, 101, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000, "DUBBED",
        tracks=[{"language": "eng", "codec": "aac", "source": "plex"}],
    )
    # ep 102 is new (no cache) — must hit Plex

    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [
        sonarr_episode(101, 1, 1, "Ep1"),
        sonarr_episode(102, 1, 2, "Ep2"),
    ]}
    files = {1: [
        sonarr_file(101, "/media/show-a/e1.mkv", 1000),
        sonarr_file(102, "/media/show-a/e2.mkv", 2000),
    ]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({
        "/media/show-a/e1.mkv": [{"language": "eng", "codec": "aac"}],
        "/media/show-a/e2.mkv": [{"language": "jpn", "codec": "aac"}],
    })
    cfg = base_cfg()

    result = await run_scan(db, cfg, sonarr, plex)

    assert plex.build_index_calls == 1
    assert result["dubbed"] == 1
    assert result["sub_only"] == 1


@pytest.mark.asyncio
async def test_cooldown_prevents_research(db):
    await seed_series(db, 1, "Show A")
    await seed_episode(
        db, 1, 101, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000, "SUB_ONLY",
        tracks=[{"language": "jpn", "codec": "aac", "source": "plex"}],
    )
    await models.add_search_record(db, 101, trigger_source="auto")  # searched just now

    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 1000)]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({})
    cfg = base_cfg(SEARCH_COOLDOWN_DAYS=7)

    result = await run_scan(db, cfg, sonarr, plex)

    assert result["searches_triggered"] == 0
    assert sonarr.searched_ids == []


@pytest.mark.asyncio
async def test_series_excluded_skips_search(db):
    await seed_series(db, 1, "Show A")
    await models.set_series_excluded(db, 1, True)
    await seed_episode(
        db, 1, 101, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000, "SUB_ONLY",
        tracks=[{"language": "jpn", "codec": "aac", "source": "plex"}],
    )

    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 1000)]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({})
    cfg = base_cfg()

    result = await run_scan(db, cfg, sonarr, plex)

    assert result["searches_triggered"] == 0
    assert sonarr.searched_ids == []


@pytest.mark.asyncio
async def test_upgrade_success_resolves_and_stops_retrying(db):
    await seed_series(db, 1, "Show A")
    # Previously sub-only at 1000 bytes
    await seed_episode(
        db, 1, 101, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000, "SUB_ONLY",
        tracks=[{"language": "jpn", "codec": "aac", "source": "plex"}],
    )
    await models.create_upgrade_record(db, 101, 1, "Show A", 1, 1, 1000)

    # File size changed (new download) and now has English audio
    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 5000)]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({"/media/show-a/e1.mkv": [{"language": "eng", "codec": "aac"}]})
    cfg = base_cfg()

    result = await run_scan(db, cfg, sonarr, plex)

    assert result["upgrades_succeeded"] == 1
    assert result["upgrades_failed"] == 0
    assert sonarr.searched_ids == []  # now dubbed, no need to search again

    history = await models.get_upgrade_history(db)
    assert history[0]["result"] == "success"


@pytest.mark.asyncio
async def test_upgrade_failure_retries_bypassing_cooldown(db):
    await seed_series(db, 1, "Show A")
    await seed_episode(
        db, 1, 101, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000, "SUB_ONLY",
        tracks=[{"language": "jpn", "codec": "aac", "source": "plex"}],
    )
    await models.create_upgrade_record(db, 101, 1, "Show A", 1, 1, 1000)
    await models.add_search_record(db, 101, trigger_source="auto")  # searched moments ago

    # File changed but still not dubbed
    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 5000)]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({"/media/show-a/e1.mkv": [{"language": "jpn", "codec": "aac"}]})
    cfg = base_cfg(SEARCH_COOLDOWN_DAYS=7)  # would normally block re-search

    result = await run_scan(db, cfg, sonarr, plex)

    assert result["upgrades_failed"] == 1
    assert sonarr.searched_ids == [[101]], "failed upgrades must re-search even inside the cooldown window"

    history = await models.get_upgrade_history(db)
    assert history[0]["result"] == "failed"


@pytest.mark.asyncio
async def test_max_search_attempts_skips_only_when_no_results(db):
    await seed_series(db, 1, "Show A")
    await seed_episode(
        db, 1, 101, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000, "SUB_ONLY",
        tracks=[{"language": "jpn", "codec": "aac", "source": "plex"}],
    )
    # 3 prior searches within the last 7 days (get_search_count's window),
    # but SEARCH_COOLDOWN_DAYS=0 below so cooldown itself doesn't block first.
    for _ in range(3):
        await db.execute(
            "INSERT INTO search_history (episode_id, triggered_at, trigger_source) "
            "VALUES (?, datetime('now', '-1 hours'), 'auto')",
            (101,),
        )
    await db.commit()
    rec_id = await models.create_upgrade_record(db, 101, 1, "Show A", 1, 1, 1000)
    await db.execute(
        "UPDATE upgrade_tracking SET download_status = 'no_results' WHERE id = ?", (rec_id,)
    )
    await db.commit()

    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 1000)]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({})
    cfg = base_cfg(MAX_SEARCH_ATTEMPTS=3, SEARCH_COOLDOWN_DAYS=0)

    result = await run_scan(db, cfg, sonarr, plex)

    assert result["searches_triggered"] == 0
    assert sonarr.searched_ids == []


@pytest.mark.asyncio
async def test_ignored_series_path_is_skipped_entirely(db):
    await models.add_ignored_path(db, "show-a", note="test")

    series = [
        {"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None},
        {"id": 2, "title": "Show B", "path": "/media/show-b", "poster_url": None},
    ]
    episodes = {
        1: [sonarr_episode(101, 1, 1, "Ep1")],
        2: [sonarr_episode(201, 1, 1, "Ep1")],
    }
    files = {
        1: [sonarr_file(101, "/media/show-a/e1.mkv", 1000)],
        2: [sonarr_file(201, "/media/show-b/e1.mkv", 1000)],
    }

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({"/media/show-b/e1.mkv": [{"language": "eng", "codec": "aac"}]})
    cfg = base_cfg()

    await run_scan(db, cfg, sonarr, plex)

    assert await models.get_series(db, 1) is None
    assert await models.get_series(db, 2) is not None


@pytest.mark.asyncio
async def test_scan_detail_correlates_by_scan_id_not_timestamp_window(db):
    """Regression test for the P2 fix: scan detail used to correlate
    search_history rows to a scan_log by timestamp window, so a manual
    search fired while an automatic scan happened to be running would leak
    into that scan's detail view. It must now only show rows tagged with
    this exact scan_id."""
    await seed_series(db, 1, "Show A")
    await seed_episode(
        db, 1, 101, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000, "SUB_ONLY",
        tracks=[{"language": "jpn", "codec": "aac", "source": "plex"}],
    )

    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 1000)]}

    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({})
    cfg = base_cfg()

    scan_id = await models.start_scan_log(db)
    rate_limiter = RateLimiter(1000)
    cooldown = timedelta(days=7)
    await _scan_with_sonarr(db, cfg, sonarr, plex, rate_limiter, cooldown, "eng", scan_id)

    # A manual search recorded under a *different* scan_id (simulating one
    # fired from the UI while this scan was running) must not leak in.
    other_scan_id = await models.start_scan_log(db)
    await models.add_search_record(db, 101, trigger_source="manual", scan_id=other_scan_id)

    detail = await models.get_scan_detail(db, scan_id)
    assert len(detail["searches"]) == 1
    assert detail["searches"][0]["trigger_source"] == "auto"

    other_detail = await models.get_scan_detail(db, other_scan_id)
    assert len(other_detail["searches"]) == 1
    assert other_detail["searches"][0]["trigger_source"] == "manual"
