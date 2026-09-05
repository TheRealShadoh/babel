"""Media-server selection, path translation, and Plex+Jellyfin fan-out."""

import pytest

from src.config import translate_path
from src.scanner.media_server import (
    MediaServerGroup,
    client_label,
    configured_servers,
    create_media_clients,
    create_media_group,
    select_media_servers,
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


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


def test_auto_uses_every_configured_server():
    assert select_media_servers(cfg(**PLEX_CFG, **JELLY_CFG)) == ["plex", "jellyfin"]


def test_auto_uses_whichever_single_server_is_configured():
    assert select_media_servers(cfg(**PLEX_CFG)) == ["plex"]
    assert select_media_servers(cfg(**JELLY_CFG)) == ["jellyfin"]


def test_auto_is_empty_when_nothing_configured():
    assert select_media_servers(cfg()) == []
    assert configured_servers(cfg()) == []


def test_explicit_choice_restricts_to_one_server():
    both = cfg(**PLEX_CFG, **JELLY_CFG, MEDIA_SERVER="jellyfin")
    assert select_media_servers(both) == ["jellyfin"]


def test_explicit_choice_that_is_not_configured_falls_back_to_ffprobe():
    assert select_media_servers(cfg(**PLEX_CFG, MEDIA_SERVER="jellyfin")) == []


def test_none_disables_every_configured_server():
    assert select_media_servers(cfg(**PLEX_CFG, **JELLY_CFG, MEDIA_SERVER="none")) == []


def test_unknown_value_is_treated_as_auto():
    assert select_media_servers(cfg(**PLEX_CFG, MEDIA_SERVER="emby")) == ["plex"]


def test_half_configured_server_does_not_count():
    assert select_media_servers(cfg(JELLYFIN_URL="http://jf:8096")) == []


def test_create_media_clients_builds_each_selected_server():
    clients = create_media_clients(cfg(**PLEX_CFG, **JELLY_CFG))
    assert [kind for _, kind in clients] == ["plex", "jellyfin"]
    assert clients[1][0].path_target == "jellyfin"


def test_create_media_group_without_config():
    assert create_media_group(cfg()) is None


def test_group_label_names_every_member():
    group = create_media_group(cfg(**PLEX_CFG, **JELLY_CFG))
    assert group.label == "Plex + Jellyfin"
    assert client_label(group) == "Plex + Jellyfin"
    assert server_label("jellyfin") == "Jellyfin"


# ---------------------------------------------------------------------------
# Path translation
# ---------------------------------------------------------------------------


def test_jellyfin_path_prefix_falls_back_to_plex_then_local():
    paths = {"SONARR_PATH_PREFIX": "/tv", "LOCAL_PATH_PREFIX": "/media/tv"}
    assert translate_path("/tv/Show/e1.mkv", "jellyfin", paths) == "/media/tv/Show/e1.mkv"

    with_plex = dict(paths, PLEX_PATH_PREFIX="/data/tv")
    assert translate_path("/tv/Show/e1.mkv", "jellyfin", with_plex) == "/data/tv/Show/e1.mkv"

    with_own = dict(with_plex, JELLYFIN_PATH_PREFIX="/jf/tv")
    assert translate_path("/tv/Show/e1.mkv", "jellyfin", with_own) == "/jf/tv/Show/e1.mkv"
    # Plex is unaffected by the Jellyfin prefix.
    assert translate_path("/tv/Show/e1.mkv", "plex", with_own) == "/data/tv/Show/e1.mkv"


# ---------------------------------------------------------------------------
# Group behaviour
# ---------------------------------------------------------------------------


class _Event:
    def __init__(self):
        self._set = False

    def set(self):
        self._set = True

    def is_set(self):
        return self._set


class FakeMember:
    def __init__(self, tracks_by_path=None, library=None, reachable=True, name="fake"):
        self.tracks_by_path = tracks_by_path or {}
        self.library = library or []
        self.reachable = reachable
        self.name = name
        self.asked_for: list[str] = []
        self.closed = False
        self.build_index_calls = 0
        self.collection_calls = 0
        self._indexed = False
        self._cancel = _Event()

    async def test_connection(self):
        return (True, f"Connected to {self.name}") if self.reachable else (False, "down")

    def is_indexed(self):
        return self._indexed

    async def build_index(self, ignored_patterns=None):
        self.build_index_calls += 1
        self._indexed = True
        return len(self.tracks_by_path)

    async def get_audio_tracks(self, path):
        self.asked_for.append(path)
        tracks = self.tracks_by_path.get(path)
        return [dict(t) for t in tracks] if tracks is not None else None

    async def get_library_data(self, target_lang="eng"):
        return [
            {**show, "episodes": [dict(ep) for ep in show["episodes"]]}
            for show in self.library
        ]

    async def sync_collections(self, series_data, changed=True):
        self.collection_calls += 1
        if not changed:
            return {"collections_updated": 0, "skipped": True}
        return {"collections_updated": len(series_data), "skipped": False}

    async def get_libraries(self):
        return [{"id": "1", "title": f"{self.name} TV", "type": "show", "path": "/x", "count": 3}]

    def get_sample_paths(self, count=5):
        return list(self.tracks_by_path)[:count]

    def get_index_progress(self):
        return {"current": 1, "total": 1, "section": self.name}

    def get_match_stats(self):
        return {"hit_path": 1, "hit_name": 0, "miss": 0}

    async def close(self):
        self.closed = True


GROUP_CFG = {
    "SONARR_PATH_PREFIX": "/tv",
    "LOCAL_PATH_PREFIX": "/media/tv",
    "PLEX_PATH_PREFIX": "/plex/tv",
    "JELLYFIN_PATH_PREFIX": "/jf/tv",
}


def make_group(plex_member, jellyfin_member, config=None):
    return MediaServerGroup(
        [(plex_member, "plex"), (jellyfin_member, "jellyfin")], config or GROUP_CFG
    )


@pytest.mark.asyncio
async def test_group_translates_each_server_with_its_own_prefix():
    plex = FakeMember({"/plex/tv/Show/e1.mkv": [{"language": "eng", "codec": "aac"}]})
    jellyfin = FakeMember()
    group = make_group(plex, jellyfin)

    tracks = await group.get_audio_tracks("/tv/Show/e1.mkv")

    assert plex.asked_for == ["/plex/tv/Show/e1.mkv"]
    assert tracks[0]["source"] == "plex"
    # Plex answered, so Jellyfin was never asked.
    assert jellyfin.asked_for == []


@pytest.mark.asyncio
async def test_group_falls_through_to_the_second_server():
    plex = FakeMember()
    jellyfin = FakeMember({"/jf/tv/Show/e1.mkv": [{"language": "ja", "codec": "aac"}]})
    group = make_group(plex, jellyfin)

    tracks = await group.get_audio_tracks("/tv/Show/e1.mkv")

    assert plex.asked_for == ["/plex/tv/Show/e1.mkv"]
    assert jellyfin.asked_for == ["/jf/tv/Show/e1.mkv"]
    # Two-letter codes from either server are normalised on the way out.
    assert tracks == [{"language": "jpn", "codec": "aac", "source": "jellyfin"}]


@pytest.mark.asyncio
async def test_group_returns_none_when_no_server_knows_the_file():
    group = make_group(FakeMember(), FakeMember())
    assert await group.get_audio_tracks("/tv/Show/missing.mkv") is None


@pytest.mark.asyncio
async def test_a_server_with_no_audio_streams_does_not_shadow_one_that_has_them():
    plex = FakeMember({"/plex/tv/Show/e1.mkv": []})
    jellyfin = FakeMember({"/jf/tv/Show/e1.mkv": [{"language": "eng", "codec": "aac"}]})

    tracks = await make_group(plex, jellyfin).get_audio_tracks("/tv/Show/e1.mkv")
    assert [t["source"] for t in tracks] == ["jellyfin"]

    # With nobody offering tracks, the empty answer still beats "unknown file".
    only_empty = make_group(FakeMember({"/plex/tv/Show/e1.mkv": []}), FakeMember())
    assert await only_empty.get_audio_tracks("/tv/Show/e1.mkv") == []


@pytest.mark.asyncio
async def test_unreachable_member_is_dropped_and_the_rest_still_scan():
    plex = FakeMember(reachable=False, name="plex")
    jellyfin = FakeMember(
        {"/jf/tv/Show/e1.mkv": [{"language": "eng", "codec": "aac"}]}, name="jellyfin"
    )
    group = make_group(plex, jellyfin)

    ok, message = await group.test_connection()

    assert ok and "jellyfin" in message
    assert group.kinds == ["jellyfin"]
    assert plex.closed  # the dead client is not left open for the whole scan
    assert (await group.get_audio_tracks("/tv/Show/e1.mkv"))[0]["source"] == "jellyfin"


@pytest.mark.asyncio
async def test_group_is_unreachable_only_when_every_member_is():
    group = make_group(FakeMember(reachable=False), FakeMember(reachable=False))
    ok, _ = await group.test_connection()
    assert not ok
    assert group.kinds == []


@pytest.mark.asyncio
async def test_group_index_and_collections_cover_every_member():
    plex = FakeMember({"/plex/tv/a.mkv": []})
    jellyfin = FakeMember({"/jf/tv/a.mkv": [], "/jf/tv/b.mkv": []})
    group = make_group(plex, jellyfin)

    assert group.is_indexed() is False
    assert await group.build_index() == 3
    assert group.is_indexed() is True

    result = await group.sync_collections([{"title": "S", "dub_status": "DUBBED"}])
    assert result == {"collections_updated": 2, "skipped": False}
    assert plex.collection_calls == jellyfin.collection_calls == 1

    skipped = await group.sync_collections([], changed=False)
    assert skipped["skipped"] is True

    assert group.get_match_stats()["hit_path"] == 2
    libraries = await group.get_libraries()
    assert [lib["server"] for lib in libraries] == ["Plex", "Jellyfin"]


@pytest.mark.asyncio
async def test_group_merges_library_data_without_duplicating_shows():
    def show(title, paths):
        return {
            "title": title, "plex_key": abs(hash(title)) % 10000, "path": "/x",
            "poster_url": None,
            "episodes": [
                {"plex_key": i, "season": 1, "episode": i, "title": f"E{i}",
                 "file_path": p, "file_size": 1, "audio_tracks": [], "dub_status": "SUB_ONLY"}
                for i, p in enumerate(paths, 1)
            ],
        }

    plex = FakeMember(library=[show("Shared Show", ["/media/a/e1.mkv"])])
    jellyfin = FakeMember(library=[
        # Same show, same file (different case) plus one Plex has not indexed.
        show("shared show", ["/MEDIA/a/E1.mkv", "/media/a/e2.mkv"]),
        show("Jellyfin Only", ["/media/b/e1.mkv"]),
    ])

    merged = await make_group(plex, jellyfin).get_library_data("eng")

    assert sorted(s["title"] for s in merged) == ["Jellyfin Only", "Shared Show"]
    shared = next(s for s in merged if s["title"] == "Shared Show")
    assert [e["file_path"] for e in shared["episodes"]] == ["/media/a/e1.mkv", "/media/a/e2.mkv"]


@pytest.mark.asyncio
async def test_cancelling_the_group_cancels_every_member():
    plex, jellyfin = FakeMember(), FakeMember()
    group = make_group(plex, jellyfin)

    group._cancel.set()

    assert plex._cancel.is_set() and jellyfin._cancel.is_set()
    assert group._cancel.is_set()


@pytest.mark.asyncio
async def test_closing_the_group_closes_every_member():
    plex, jellyfin = FakeMember(), FakeMember()
    await make_group(plex, jellyfin).close()
    assert plex.closed and jellyfin.closed


# ---------------------------------------------------------------------------
# Through the scan engine
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scan_uses_both_servers_and_records_which_one_answered(tmp_path):
    from datetime import timedelta

    from src.db.database import init_db, get_db
    from src.db import models
    from src.scanner.engine import RateLimiter, _scan_with_sonarr
    from tests.fakes import FakeSonarr, base_cfg, sonarr_episode, sonarr_file

    db_path = str(tmp_path / "both_servers.db")
    await init_db(db_path)
    db = await get_db(db_path)
    try:
        scan_cfg = base_cfg(**GROUP_CFG)
        sonarr = FakeSonarr(
            series=[{"id": 1, "title": "Show A", "path": "/tv/show-a", "poster_url": None}],
            episodes_by_series={1: [
                sonarr_episode(101, 1, 1, "Ep1"), sonarr_episode(102, 1, 2, "Ep2"),
            ]},
            files_by_series={1: [
                sonarr_file(101, "/tv/show-a/e1.mkv", 1000),
                sonarr_file(102, "/tv/show-a/e2.mkv", 2000),
            ]},
        )
        # Plex knows the first episode; only Jellyfin has indexed the second.
        plex = FakeMember({"/plex/tv/show-a/e1.mkv": [{"language": "eng", "codec": "aac"}]})
        jellyfin = FakeMember({"/jf/tv/show-a/e2.mkv": [{"language": "jpn", "codec": "aac"}]})
        group = make_group(plex, jellyfin, scan_cfg)

        scan_id = await models.start_scan_log(db)
        result = await _scan_with_sonarr(
            db, scan_cfg, sonarr, group, RateLimiter(1000), timedelta(days=7), "eng", scan_id,
        )

        assert (result["dubbed"], result["sub_only"]) == (1, 1)
        assert (await models.get_audio_tracks(db, 101))[0]["source"] == "plex"
        assert (await models.get_audio_tracks(db, 102))[0]["source"] == "jellyfin"
    finally:
        await db.close()
