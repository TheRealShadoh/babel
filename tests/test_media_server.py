"""Media-server selection and path translation across Plex/Jellyfin."""

from src.config import translate_path
from src.scanner.media_server import (
    configured_servers,
    create_media_client,
    select_media_server,
    server_label,
)


def cfg(**overrides):
    base = {
        "PLEX_URL": "", "PLEX_TOKEN": "",
        "JELLYFIN_URL": "", "JELLYFIN_API_KEY": "",
        "MEDIA_SERVER": "auto",
    }
    base.update(overrides)
    return base


PLEX_CFG = {"PLEX_URL": "http://plex:32400", "PLEX_TOKEN": "t"}
JELLY_CFG = {"JELLYFIN_URL": "http://jf:8096", "JELLYFIN_API_KEY": "k"}


def test_auto_prefers_plex_when_both_configured():
    assert select_media_server(cfg(**PLEX_CFG, **JELLY_CFG)) == "plex"


def test_auto_uses_jellyfin_when_only_jellyfin_configured():
    assert select_media_server(cfg(**JELLY_CFG)) == "jellyfin"


def test_auto_is_none_when_nothing_configured():
    assert select_media_server(cfg()) == "none"
    assert configured_servers(cfg()) == []


def test_explicit_choice_wins_over_plex_default():
    assert select_media_server(cfg(**PLEX_CFG, **JELLY_CFG, MEDIA_SERVER="jellyfin")) == "jellyfin"


def test_explicit_choice_that_is_not_configured_falls_back_to_none():
    # Better to probe with ffprobe than to build a client with no credentials
    # and report every episode as unreadable.
    assert select_media_server(cfg(**PLEX_CFG, MEDIA_SERVER="jellyfin")) == "none"


def test_none_disables_the_media_server_even_when_configured():
    assert select_media_server(cfg(**PLEX_CFG, MEDIA_SERVER="none")) == "none"


def test_unknown_value_is_treated_as_auto():
    assert select_media_server(cfg(**PLEX_CFG, MEDIA_SERVER="emby")) == "plex"


def test_half_configured_server_does_not_count():
    assert select_media_server(cfg(JELLYFIN_URL="http://jf:8096")) == "none"


def test_create_media_client_returns_matching_kind():
    client, kind = create_media_client(cfg(**JELLY_CFG))
    assert kind == "jellyfin"
    assert client.path_target == "jellyfin"
    assert server_label(kind) == "Jellyfin"


def test_create_media_client_without_config():
    client, kind = create_media_client(cfg())
    assert client is None and kind == "none"


def test_jellyfin_path_prefix_falls_back_to_plex_then_local():
    paths = {"SONARR_PATH_PREFIX": "/tv", "LOCAL_PATH_PREFIX": "/media/tv"}
    assert translate_path("/tv/Show/e1.mkv", "jellyfin", paths) == "/media/tv/Show/e1.mkv"

    with_plex = dict(paths, PLEX_PATH_PREFIX="/data/tv")
    assert translate_path("/tv/Show/e1.mkv", "jellyfin", with_plex) == "/data/tv/Show/e1.mkv"

    with_own = dict(with_plex, JELLYFIN_PATH_PREFIX="/jf/tv")
    assert translate_path("/tv/Show/e1.mkv", "jellyfin", with_own) == "/jf/tv/Show/e1.mkv"
    # Plex is unaffected by the Jellyfin prefix.
    assert translate_path("/tv/Show/e1.mkv", "plex", with_own) == "/data/tv/Show/e1.mkv"


class _JellyfinLikeClient:
    """A media client that reports Jellyfin's path target, like the real one."""

    path_target = "jellyfin"

    def __init__(self, tracks_by_path):
        self.tracks_by_path = tracks_by_path
        self.asked_for: list[str] = []
        self._indexed = False

    def is_indexed(self):
        return self._indexed

    async def build_index(self, ignored_patterns=None):
        self._indexed = True
        return len(self.tracks_by_path)

    async def get_audio_tracks(self, file_path):
        self.asked_for.append(file_path)
        tracks = self.tracks_by_path.get(file_path)
        return [dict(t) for t in tracks] if tracks is not None else None

    def get_sample_paths(self, n):
        return list(self.tracks_by_path)[:n]

    async def sync_collections(self, series_data, changed=True):
        return {"collections_updated": 0, "skipped": False}

    async def close(self):
        pass


async def test_scan_translates_paths_for_the_jellyfin_client(tmp_path):
    """The engine must use each client's own prefix, not always Plex's."""
    from datetime import timedelta

    from src.db.database import init_db, get_db
    from src.db import models
    from src.scanner.engine import RateLimiter, _scan_with_sonarr
    from tests.fakes import FakeSonarr, base_cfg, sonarr_episode, sonarr_file

    db_path = str(tmp_path / "jellyfin_scan.db")
    await init_db(db_path)
    db = await get_db(db_path)
    try:
        scan_cfg = base_cfg(
            SONARR_PATH_PREFIX="/tv",
            LOCAL_PATH_PREFIX="/media/tv",
            PLEX_PATH_PREFIX="/plex/tv",
            JELLYFIN_PATH_PREFIX="/data/tv",
        )
        sonarr = FakeSonarr(
            series=[{"id": 1, "title": "Show A", "path": "/tv/show-a", "poster_url": None}],
            episodes_by_series={1: [sonarr_episode(101, 1, 1, "Ep1")]},
            files_by_series={1: [sonarr_file(101, "/tv/show-a/e1.mkv", 1000)]},
        )
        media = _JellyfinLikeClient({
            "/data/tv/show-a/e1.mkv": [{"language": "eng", "codec": "aac"}],
        })

        scan_id = await models.start_scan_log(db)
        result = await _scan_with_sonarr(
            db, scan_cfg, sonarr, media, RateLimiter(1000), timedelta(days=7), "eng", scan_id,
        )

        assert media.asked_for == ["/data/tv/show-a/e1.mkv"]
        assert result["dubbed"] == 1
        tracks = await models.get_audio_tracks(db, 101)
        assert tracks[0]["source"] == "jellyfin"
    finally:
        await db.close()
