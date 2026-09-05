"""Scan paths that could destroy or corrupt cached data.

Each test here corresponds to a defect found in the 1.2.0 production review:
the media-only fallback that pruned a Sonarr-keyed library, stale audio
tracks surviving a file replacement, an unreadable replacement being written
off as a failed upgrade, and files-without-episodes wiping a series.
"""

from datetime import timedelta

import httpx
import pytest

from src.db.database import init_db, get_db
from src.db import models
from src.scanner import engine
from src.scanner.engine import RateLimiter, _scan_with_sonarr
from src.scanner.media_server import MediaServerGroup

from tests.fakes import FakeSonarr, FakePlex, base_cfg, sonarr_episode, sonarr_file, plex_show, plex_episode


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "babel_test.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


async def run_scan(db, sonarr, plex, cfg=None):
    scan_id = await models.start_scan_log(db)
    return await _scan_with_sonarr(
        db, cfg or base_cfg(), sonarr, plex, RateLimiter(1000), timedelta(days=7), "eng", scan_id,
    )


def one_show(path="/media/show-a/e1.mkv", size=1000, languages=("jpn",)):
    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, path, size)]}
    plex = FakePlex({path: [{"language": lang, "codec": "aac"} for lang in languages]})
    return series, episodes, files, plex


# ---------------------------------------------------------------------------
# Sonarr configured but down must never turn into a media-only pass
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unreachable_sonarr_fails_the_pass_instead_of_falling_back(monkeypatch, tmp_path):
    """The media-only mode keys series by media-server IDs and prunes against
    them; run over a Sonarr-keyed library it deleted every Sonarr row."""
    db_path = str(tmp_path / "fallback.db")
    await init_db(db_path)

    # A Sonarr-keyed library with history that must survive.
    db = await get_db(db_path)
    try:
        series, episodes, files, plex = one_show()
        await run_scan(db, FakeSonarr(series, episodes, files), plex, base_cfg(DB_PATH=db_path))
        assert (await models.get_all_series(db))[0]["dub_status"] == "SUB_ONLY"
        history_before = (await db.execute_fetchall("SELECT COUNT(*) FROM search_history"))[0][0]
        assert history_before == 1
    finally:
        await db.close()

    def sonarr_down(request):
        raise httpx.ConnectError("connection refused", request=request)

    class _DownSonarr(engine.SonarrClient):
        def __init__(self, url, api_key):
            super().__init__(url, api_key)
            self.client = httpx.AsyncClient(
                base_url="http://sonarr:8989/api/v3", transport=httpx.MockTransport(sonarr_down)
            )

    plex_library = [plex_show(1, "Show A", "/media/show-a", [
        plex_episode(11, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000,
                     [{"language": "jpn", "codec": "aac", "source": "plex"}], "SUB_ONLY"),
    ])]

    async def fake_cfg():
        return base_cfg(DB_PATH=db_path, SONARR_URL="http://sonarr:8989", SONARR_API_KEY="k",
                        PLEX_URL="http://plex:32400", PLEX_TOKEN="t")

    monkeypatch.setattr(engine, "get_effective_settings", fake_cfg)
    monkeypatch.setattr(engine, "SonarrClient", _DownSonarr)
    monkeypatch.setattr(
        engine, "create_media_group",
        lambda cfg: MediaServerGroup([(FakePlex({}, library_data=plex_library), "plex")], cfg),
    )

    result = await engine.run_scan()

    assert result["status"] == "failed"
    assert result["retryable"] is True
    assert "unreachable" in result["error"]

    db = await get_db(db_path)
    try:
        rows = await models.get_all_series(db)
        assert [(r["id"], r["dub_status"]) for r in rows] == [(1, "SUB_ONLY")]
        assert (await db.execute_fetchall("SELECT COUNT(*) FROM search_history"))[0][0] == 1
        # Two scans started within the same second; pick the newest by id.
        latest = max(await models.get_scan_logs(db), key=lambda r: r["id"])
        assert latest["status"] == "failed" and "unreachable" in latest["error_message"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_media_only_pruning_never_touches_sonarr_keyed_rows(db):
    """Defence in depth for the same hole: even if the media-only pass runs,
    its orphan cleanup is scoped to media-keyed (negative) IDs."""
    for sid in range(1, 13):
        await models.upsert_series(db, sid, f"Sonarr {sid}", f"/media/s{sid}")
    for sid in (-1000001, -1000002, -1000003):
        await models.upsert_series(db, sid, f"Plex {sid}", "/media/p")

    deleted = await models.delete_series_not_in(db, {-1000001}, only_negative_ids=True)

    remaining = sorted(r["id"] for r in await models.get_all_series(db))
    assert deleted == 2
    assert remaining == [-1000001] + list(range(1, 13))


@pytest.mark.asyncio
async def test_media_only_scan_leaves_a_sonarr_library_alone(db):
    series, episodes, files, plex = one_show()
    await run_scan(db, FakeSonarr(series, episodes, files), plex)

    library = [plex_show(7, "Unrelated Plex Show", "/media/x", [
        plex_episode(70, 1, 1, "Ep1", "/media/x/e1.mkv", 10,
                     [{"language": "eng", "codec": "aac", "source": "plex"}], "DUBBED"),
    ])]
    scan_id = await models.start_scan_log(db)
    await engine._scan_media_server_only(
        db, base_cfg(), FakePlex({}, library_data=library), "eng", scan_id,
    )

    ids = sorted(r["id"] for r in await models.get_all_series(db))
    assert 1 in ids  # the Sonarr row survived
    assert any(i < 0 for i in ids)  # and the Plex row was added


# ---------------------------------------------------------------------------
# A replaced file must not be classified by the old file's audio
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_audio_tracks_are_dropped_when_the_file_changes(db):
    series, episodes, files, plex = one_show(languages=("jpn",))
    await run_scan(db, FakeSonarr(series, episodes, files), plex)
    assert (await models.get_episode(db, 101))["dub_status"] == "SUB_ONLY"

    # Sonarr swaps in a dubbed release, but nothing can read it this pass.
    files = {1: [sonarr_file(101, "/media/show-a/e1-dual.mkv", 2000)]}
    result = await run_scan(db, FakeSonarr(series, episodes, files), FakePlex({}))
    assert (await models.get_episode(db, 101))["dub_status"] == "UNKNOWN"
    assert await models.get_audio_tracks(db, 101) == []  # not the old jpn row
    assert result["upgrades_failed"] == 0

    # Next pass the new file is readable: it must be classified by ITS audio.
    plex = FakePlex({"/media/show-a/e1-dual.mkv": [{"language": "eng", "codec": "aac"},
                                                    {"language": "jpn", "codec": "aac"}]})
    result = await run_scan(db, FakeSonarr(series, episodes, files), plex)

    assert (await models.get_episode(db, 101))["dub_status"] == "DUBBED"
    assert result["upgrades_succeeded"] == 1


@pytest.mark.asyncio
async def test_unreadable_replacement_keeps_the_upgrade_pending(db):
    """Before: UNKNOWN after a size change resolved the upgrade as 'failed'
    and re-searched immediately, so Sonarr grabbed yet another release."""
    series, episodes, files, plex = one_show(languages=("jpn",))
    sonarr = FakeSonarr(series, episodes, files)
    await run_scan(db, sonarr, plex)
    assert sonarr.searched_ids == [[101]]
    pending = await models.get_pending_upgrades(db)
    assert len(pending) == 1

    files = {1: [sonarr_file(101, "/media/show-a/e1-new.mkv", 2000)]}
    sonarr = FakeSonarr(series, episodes, files)
    result = await run_scan(db, sonarr, FakePlex({}))

    assert sonarr.searched_ids == []  # no bypass-cooldown re-search
    assert result["upgrades_failed"] == 0
    assert len(await models.get_pending_upgrades(db)) == 1  # still pending, not failed


@pytest.mark.asyncio
async def test_readable_replacement_that_is_still_sub_only_is_a_failed_upgrade(db):
    """The genuine failure case keeps its behaviour."""
    series, episodes, files, plex = one_show(languages=("jpn",))
    await run_scan(db, FakeSonarr(series, episodes, files), plex)

    files = {1: [sonarr_file(101, "/media/show-a/e1-v2.mkv", 2000)]}
    sonarr = FakeSonarr(series, episodes, files)
    plex = FakePlex({"/media/show-a/e1-v2.mkv": [{"language": "jpn", "codec": "aac"}]})
    result = await run_scan(db, sonarr, plex)

    assert result["upgrades_failed"] == 1
    assert sonarr.searched_ids == [[101]]  # retried, bypassing cooldown


# ---------------------------------------------------------------------------
# Files listed but no episodes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_files_without_episodes_is_skipped_not_applied(db):
    series, episodes, files, plex = one_show()
    await run_scan(db, FakeSonarr(series, episodes, files), plex)

    result = await run_scan(db, FakeSonarr(series, {1: []}, files), plex)

    assert result["errors"] == 1
    assert (await models.get_episode(db, 101))["dub_status"] == "SUB_ONLY"
