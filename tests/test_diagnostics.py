"""Why-is-nothing-showing-up diagnosis (issue #2)."""

from datetime import timedelta

import httpx
import pytest

from src.db.database import init_db, get_db
from src.db import models
from src.scanner.engine import RateLimiter, _scan_with_sonarr
from src.scanner.sonarr import SonarrClient

from tests.fakes import FakeSonarr, FakePlex, base_cfg


def stub_dub_self_test(monkeypatch, **overrides):
    """Keep diagnostics tests off the network: the dub check calls out live."""
    report = {
        "title": "Cowboy Bebop",
        "mal": {"ok": True, "reachable": True, "rate_limited": False,
                "matched": "Cowboy Bebop", "dub_status": "available",
                "licensors": ["Funimation"]},
        "ann": {"enabled": True, "ok": True, "has_dub": True,
                "matched": "Cowboy Bebop", "cast_size": 30},
        "verdict": "available",
    }
    for key, value in overrides.items():
        if isinstance(value, dict):
            report[key].update(value)
        else:
            report[key] = value

    async def fake_self_test(title="Cowboy Bebop"):
        return report

    monkeypatch.setattr("src.scanner.dub_lookup.self_test", fake_self_test)
    return report


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "babel_test.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


def sonarr_with(series_payload):
    def handler(request):
        if request.url.path.endswith("/series"):
            return httpx.Response(200, json=series_payload)
        if request.url.path.endswith("/tag"):
            return httpx.Response(200, json=[{"id": 7, "label": "anime"}])
        return httpx.Response(200, json={})

    client = SonarrClient("http://sonarr:8989", "key")
    client.client = httpx.AsyncClient(
        base_url="http://sonarr:8989/api/v3", transport=httpx.MockTransport(handler)
    )
    return client


STANDARD_LIBRARY = [
    {"id": 1, "title": "Show A", "path": "/tv/a", "seriesType": "standard", "tags": []},
    {"id": 2, "title": "Show B", "path": "/tv/b", "seriesType": "standard", "tags": [7]},
]


@pytest.mark.asyncio
async def test_type_filter_matching_nothing_returns_empty_not_none():
    """Empty means "the filter excluded everything"; None means "Sonarr failed".

    They must stay distinct — only the second one may block orphan cleanup.
    """
    matched = await sonarr_with(STANDARD_LIBRARY).get_anime_series("type")
    assert matched == []


@pytest.mark.asyncio
async def test_all_filter_returns_every_series():
    matched = await sonarr_with(STANDARD_LIBRARY).get_anime_series("all")
    assert [s["id"] for s in matched] == [1, 2]


@pytest.mark.asyncio
async def test_tag_filter_matches_tagged_series():
    matched = await sonarr_with(STANDARD_LIBRARY).get_anime_series("tag:anime")
    assert [s["id"] for s in matched] == [2]


@pytest.mark.asyncio
async def test_unknown_tag_is_a_failure_not_an_empty_library():
    assert await sonarr_with(STANDARD_LIBRARY).get_anime_series("tag:nope") is None


@pytest.mark.asyncio
async def test_zero_matches_logs_what_sonarr_actually_has(caplog):
    with caplog.at_level("WARNING"):
        await sonarr_with(STANDARD_LIBRARY).get_anime_series("type")

    text = caplog.text
    assert "2 series" in text
    assert "2 standard" in text  # the type breakdown that explains the mismatch
    assert "'all'" in text  # and what to change it to


def test_filter_hint_covers_each_filter_mode():
    type_hint = SonarrClient.filter_hint("type", STANDARD_LIBRARY)
    assert "2 standard" in type_hint and "tag:YourTag" in type_hint

    tag_hint = SonarrClient.filter_hint("tag:Anime", STANDARD_LIBRARY)
    assert "'Anime'" in tag_hint

    assert "Anime Filter" in SonarrClient.filter_hint("all", STANDARD_LIBRARY)


def test_series_type_counts():
    counts = SonarrClient.series_type_counts(
        STANDARD_LIBRARY + [{"id": 3, "title": "C", "seriesType": "anime"}, {"id": 4, "title": "D"}]
    )
    assert counts == {"standard": 2, "anime": 1, "unknown": 1}


@pytest.mark.asyncio
async def test_scan_with_no_matching_series_records_a_warning(db):
    """The empty scan explains itself instead of looking like a normal pass."""
    sonarr = FakeSonarr(series=[])
    scan_id = await models.start_scan_log(db)
    result = await _scan_with_sonarr(
        db, base_cfg(), sonarr, FakePlex({}), RateLimiter(1000),
        timedelta(days=7), "eng", scan_id,
    )

    assert result["status"] == "completed"
    assert "Anime Filter" in result["warning"]

    logged = await models.get_scan_logs(db, limit=1)
    assert "Anime Filter" in logged[0]["error_message"]


@pytest.mark.asyncio
async def test_run_diagnostics_reports_unconfigured_services(monkeypatch, tmp_path):
    from src.scanner import diagnostics

    db_path = str(tmp_path / "diag.db")
    await init_db(db_path)

    async def fake_cfg():
        return {"SONARR_URL": "", "SONARR_API_KEY": "", "PLEX_URL": "", "PLEX_TOKEN": "",
                "JELLYFIN_URL": "", "JELLYFIN_API_KEY": "", "MEDIA_SERVER": "auto",
                "DB_PATH": db_path}

    class _Settings:
        DB_PATH = db_path

    monkeypatch.setattr(diagnostics, "get_effective_settings", fake_cfg)
    monkeypatch.setattr(diagnostics, "get_settings", lambda: _Settings())
    stub_dub_self_test(monkeypatch)

    report = await diagnostics.run_diagnostics()
    names = {c["name"]: c for c in report["checks"]}

    assert names["Sonarr"]["level"] == "info"
    assert names["Media server"]["level"] == "info"
    # An install that has never scanned should say so rather than look healthy.
    assert names["Last scan"]["level"] == "warn"
    assert report["summary"]["errors"] == 0


@pytest.mark.asyncio
async def test_run_diagnostics_flags_a_filter_that_matches_nothing(monkeypatch, tmp_path):
    from src.scanner import diagnostics

    db_path = str(tmp_path / "diag2.db")
    await init_db(db_path)

    def handler(request):
        path = request.url.path
        if path.endswith("/system/status"):
            return httpx.Response(200, json={"version": "4.0.0"})
        if path.endswith("/series"):
            return httpx.Response(200, json=STANDARD_LIBRARY)
        return httpx.Response(200, json=[])

    def fake_client(url, api_key):
        client = SonarrClient(url, api_key)
        client.client = httpx.AsyncClient(
            base_url="http://sonarr:8989/api/v3", transport=httpx.MockTransport(handler)
        )
        return client

    async def fake_cfg():
        return {"SONARR_URL": "http://sonarr:8989", "SONARR_API_KEY": "key",
                "PLEX_URL": "", "PLEX_TOKEN": "", "JELLYFIN_URL": "", "JELLYFIN_API_KEY": "",
                "MEDIA_SERVER": "auto", "ANIME_FILTER": "type", "DB_PATH": db_path}

    class _Settings:
        DB_PATH = db_path

    monkeypatch.setattr(diagnostics, "get_effective_settings", fake_cfg)
    monkeypatch.setattr(diagnostics, "get_settings", lambda: _Settings())
    monkeypatch.setattr(diagnostics, "SonarrClient", fake_client)
    stub_dub_self_test(monkeypatch)

    report = await diagnostics.run_diagnostics()
    checks = {c["name"]: c for c in report["checks"]}

    assert checks["Sonarr"]["level"] == "ok"
    assert checks["Series filter"]["level"] == "error"
    assert "0 of 2 series" in checks["Series filter"]["message"]
    assert "all" in checks["Series filter"]["hint"]
    assert report["summary"]["errors"] == 1


@pytest.mark.asyncio
async def test_diagnostics_reports_each_media_server_separately(monkeypatch, tmp_path):
    """With both configured, one being down says nothing about the other."""
    from src.scanner import diagnostics

    db_path = str(tmp_path / "diag3.db")
    await init_db(db_path)

    class FakeServer:
        def __init__(self, kind):
            self.kind = kind

        async def test_connection(self):
            if self.kind == "plex":
                return False, "Cannot reach Plex: timed out"
            return True, "Connected to Home (Jellyfin v10.10)"

        async def get_libraries(self):
            return [{"id": "1", "title": "Anime", "type": "show", "path": "/data", "count": 12}]

        async def close(self):
            pass

    async def fake_cfg():
        return {"SONARR_URL": "", "SONARR_API_KEY": "",
                "PLEX_URL": "http://plex:32400", "PLEX_TOKEN": "t",
                "JELLYFIN_URL": "http://jf:8096", "JELLYFIN_API_KEY": "k",
                "MEDIA_SERVER": "auto", "DB_PATH": db_path}

    class _Settings:
        DB_PATH = db_path

    monkeypatch.setattr(diagnostics, "get_effective_settings", fake_cfg)
    monkeypatch.setattr(diagnostics, "get_settings", lambda: _Settings())
    monkeypatch.setattr(diagnostics, "build_client", lambda kind, cfg: FakeServer(kind))
    stub_dub_self_test(monkeypatch)

    report = await diagnostics.run_diagnostics()
    checks = {c["name"]: c for c in report["checks"]}

    assert checks["Plex"]["level"] == "error"
    assert checks["Jellyfin"]["level"] == "ok"
    assert checks["Jellyfin libraries"]["level"] == "ok"
    assert "Plex libraries" not in checks  # an unreachable server is not probed further
