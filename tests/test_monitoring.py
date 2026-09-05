"""Sonarr monitor-status updates when a dub becomes available (issue #1)."""

from datetime import timedelta

import httpx
import pytest

from src.db.database import init_db, get_db
from src.db import models
from src.scanner.dub_lookup import monitor_newly_dubbed
from src.scanner.engine import RateLimiter, _scan_with_sonarr
from src.scanner.sonarr import SonarrClient

from tests.fakes import FakeSonarr, FakePlex, base_cfg, sonarr_episode, sonarr_file


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "babel_test.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


async def run_scan(db, cfg, sonarr, plex):
    scan_id = await models.start_scan_log(db)
    rate_limiter = RateLimiter(int(cfg.get("SEARCH_RATE_LIMIT", 1000)))
    cooldown = timedelta(days=int(cfg.get("SEARCH_COOLDOWN_DAYS", 7)))
    return await _scan_with_sonarr(
        db, cfg, sonarr, plex, rate_limiter, cooldown,
        cfg.get("TARGET_LANGUAGE", "eng"), scan_id,
    )


def sub_only_library():
    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 1000)]}
    plex = FakePlex({"/media/show-a/e1.mkv": [{"language": "jpn", "codec": "aac"}]})
    return FakeSonarr(series, episodes, files), plex


@pytest.mark.asyncio
async def test_searched_episodes_are_monitored_when_enabled(db):
    sonarr, plex = sub_only_library()
    result = await run_scan(db, base_cfg(AUTO_MONITOR_DUBS="true"), sonarr, plex)

    assert sonarr.searched_ids == [[101]]
    assert sonarr.monitored_calls == [([101], True)]
    assert result["monitored"] == 1


@pytest.mark.asyncio
async def test_monitoring_can_be_turned_off(db):
    sonarr, plex = sub_only_library()
    result = await run_scan(db, base_cfg(AUTO_MONITOR_DUBS="false"), sonarr, plex)

    assert sonarr.searched_ids == [[101]]  # the search still happens
    assert sonarr.monitored_calls == []
    assert result["monitored"] == 0


@pytest.mark.asyncio
async def test_monitoring_is_on_when_the_setting_is_absent(db):
    """Settings written before this option existed get the on-by-default."""
    cfg = base_cfg()
    del cfg["AUTO_MONITOR_DUBS"]
    sonarr, plex = sub_only_library()

    result = await run_scan(db, cfg, sonarr, plex)

    assert sonarr.monitored_calls == [([101], True)]
    assert result["monitored"] == 1


@pytest.mark.asyncio
async def test_dubbed_episodes_are_not_monitored(db):
    """Nothing is searched for an already-dubbed episode, so nothing is touched."""
    series = [{"id": 1, "title": "Show A", "path": "/media/show-a", "poster_url": None}]
    episodes = {1: [sonarr_episode(101, 1, 1, "Ep1")]}
    files = {1: [sonarr_file(101, "/media/show-a/e1.mkv", 1000)]}
    plex = FakePlex({"/media/show-a/e1.mkv": [{"language": "eng", "codec": "aac"}]})
    sonarr = FakeSonarr(series, episodes, files)

    await run_scan(db, base_cfg(AUTO_MONITOR_DUBS="true"), sonarr, plex)
    assert sonarr.monitored_calls == []


class _RecordingSonarr:
    """Captures what monitor_newly_dubbed sends to Sonarr."""

    instances: list["_RecordingSonarr"] = []

    def __init__(self, url, api_key):
        self.calls: list[tuple[list[int], bool]] = []
        _RecordingSonarr.instances.append(self)

    async def set_episodes_monitored(self, episode_ids, monitored=True):
        self.calls.append((sorted(episode_ids), monitored))
        return True

    async def close(self):
        pass


@pytest.fixture
def recording_sonarr(monkeypatch):
    _RecordingSonarr.instances = []
    monkeypatch.setattr("src.scanner.sonarr.SonarrClient", _RecordingSonarr)
    return _RecordingSonarr


async def seed_sub_only_series(db, series_id=1):
    await models.upsert_series(db, series_id, "Show A", "/media/show-a")
    for ep_id, status in ((101, "SUB_ONLY"), (102, "SUB_ONLY"), (103, "DUBBED")):
        await models.upsert_episode(db, ep_id, series_id, 1, ep_id - 100, "Ep", "/f.mkv", 10)
        await models.update_episode_status(db, ep_id, status)


def monitor_cfg(**overrides):
    cfg = {
        "SONARR_URL": "http://sonarr:8989",
        "SONARR_API_KEY": "key",
        "AUTO_MONITOR_DUBS": "true",
    }
    cfg.update(overrides)
    return cfg


@pytest.mark.asyncio
async def test_newly_dubbed_series_gets_its_sub_only_episodes_monitored(db, recording_sonarr):
    await seed_sub_only_series(db)

    count = await monitor_newly_dubbed(db, monitor_cfg(), [{"id": 1, "title": "Show A"}])

    assert count == 2
    assert recording_sonarr.instances[0].calls == [([101, 102], True)]


@pytest.mark.asyncio
async def test_monitor_newly_dubbed_respects_the_toggle(db, recording_sonarr):
    await seed_sub_only_series(db)

    count = await monitor_newly_dubbed(
        db, monitor_cfg(AUTO_MONITOR_DUBS="false"), [{"id": 1, "title": "Show A"}]
    )

    assert count == 0
    assert recording_sonarr.instances == []


@pytest.mark.asyncio
async def test_monitor_newly_dubbed_skips_media_server_only_series(db, recording_sonarr):
    """Negative IDs are Plex/Jellyfin-only rows that Sonarr knows nothing about."""
    await seed_sub_only_series(db, series_id=-1000001)

    count = await monitor_newly_dubbed(db, monitor_cfg(), [{"id": -1000001, "title": "Show A"}])

    assert count == 0
    assert recording_sonarr.instances == []


@pytest.mark.asyncio
async def test_monitor_newly_dubbed_without_sonarr_configured(db, recording_sonarr):
    await seed_sub_only_series(db)

    count = await monitor_newly_dubbed(
        db, monitor_cfg(SONARR_URL="", SONARR_API_KEY=""), [{"id": 1, "title": "Show A"}]
    )

    assert count == 0


@pytest.mark.asyncio
async def test_set_episodes_monitored_posts_the_expected_payload():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["body"] = request.read().decode()
        return httpx.Response(200, json={})

    sonarr = SonarrClient("http://sonarr:8989", "key")
    sonarr.client = httpx.AsyncClient(
        base_url="http://sonarr:8989/api/v3", transport=httpx.MockTransport(handler)
    )

    assert await sonarr.set_episodes_monitored([5, 6], True) is True
    assert seen["url"].endswith("/api/v3/episode/monitor")
    assert '"episodeIds": [5, 6]' in seen["body"] or '"episodeIds":[5,6]' in seen["body"]
    assert "true" in seen["body"]


@pytest.mark.asyncio
async def test_set_episodes_monitored_ignores_negative_and_empty_ids():
    def handler(request):  # pragma: no cover - must never be called
        raise AssertionError("no request expected")

    sonarr = SonarrClient("http://sonarr:8989", "key")
    sonarr.client = httpx.AsyncClient(
        base_url="http://sonarr:8989/api/v3", transport=httpx.MockTransport(handler)
    )

    assert await sonarr.set_episodes_monitored([]) is True
    assert await sonarr.set_episodes_monitored([-1000001]) is True


@pytest.mark.asyncio
async def test_set_episodes_monitored_reports_failure():
    def handler(request):
        return httpx.Response(500, json={})

    sonarr = SonarrClient("http://sonarr:8989", "key")
    sonarr.client = httpx.AsyncClient(
        base_url="http://sonarr:8989/api/v3", transport=httpx.MockTransport(handler)
    )

    assert await sonarr.set_episodes_monitored([5]) is False
