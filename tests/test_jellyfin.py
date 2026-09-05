"""JellyfinClient against a stubbed Jellyfin API."""

import httpx
import pytest

from src.scanner.jellyfin import JellyfinClient, stable_key


def episode_item(item_id, series_id, season, number, path, languages, size=1000):
    return {
        "Id": item_id,
        "Name": f"Episode {number}",
        "SeriesId": series_id,
        "ParentIndexNumber": season,
        "IndexNumber": number,
        "Path": path,
        "MediaSources": [{
            "Size": size,
            "MediaStreams": (
                [{"Type": "Video", "Codec": "h264"}]
                + [{"Type": "Audio", "Language": lang, "Codec": "aac"} for lang in languages]
            ),
        }],
    }


def make_client(handler) -> JellyfinClient:
    client = JellyfinClient("http://jellyfin:8096", "api-key")
    client.client = httpx.AsyncClient(
        base_url="http://jellyfin:8096",
        transport=httpx.MockTransport(handler),
    )
    return client


def items_response(items):
    return httpx.Response(200, json={"Items": items, "TotalRecordCount": len(items)})


@pytest.mark.asyncio
async def test_test_connection_reports_server_name():
    def handler(request):
        assert request.url.path == "/System/Info"
        return httpx.Response(200, json={"ServerName": "Home", "Version": "10.10.3"})

    client = make_client(handler)
    ok, message = await client.test_connection()
    assert ok
    assert "Home" in message and "10.10.3" in message


@pytest.mark.asyncio
async def test_bad_api_key_is_reported_as_such():
    def handler(request):
        return httpx.Response(401, json={})

    ok, message = await make_client(handler).test_connection()
    assert not ok
    assert "API key" in message


@pytest.mark.asyncio
async def test_index_matches_by_path_and_by_filename():
    def handler(request):
        if request.url.path == "/Users":
            return httpx.Response(200, json=[{"Id": "u1", "Policy": {"IsAdministrator": True}}])
        assert request.url.params["userId"] == "u1"
        return items_response([
            episode_item("e1", "s1", 1, 1, "/data/tv/Show/S01E01.mkv", ["eng", "jpn"]),
            episode_item("e2", "s1", 1, 2, "/data/tv/Show/S01E02.mkv", ["jpn"]),
        ])

    client = make_client(handler)
    assert await client.build_index() == 2
    assert client.is_indexed()

    exact = await client.get_audio_tracks("/data/tv/Show/S01E01.mkv")
    assert {t["language"] for t in exact} == {"eng", "jpn"}

    # Different folder layout, same filename — the name index covers it.
    by_name = await client.get_audio_tracks("/other/mount/S01E02.mkv")
    assert [t["language"] for t in by_name] == ["jpn"]

    assert await client.get_audio_tracks("/data/tv/Show/S09E99.mkv") is None
    assert client.get_match_stats() == {"hit_path": 1, "hit_name": 1, "miss": 1}


@pytest.mark.asyncio
async def test_index_skips_ignored_paths():
    def handler(request):
        if request.url.path == "/Users":
            return httpx.Response(200, json=[])
        return items_response([
            episode_item("e1", "s1", 1, 1, "/data/tv/Keep/e1.mkv", ["eng"]),
            episode_item("e2", "s2", 1, 1, "/data/skip/Show/skipped.mkv", ["eng"]),
        ])

    client = make_client(handler)
    assert await client.build_index(ignored_patterns=["/data/skip/"]) == 1
    assert await client.get_audio_tracks("/data/skip/Show/skipped.mkv") is None


@pytest.mark.asyncio
async def test_index_paginates():
    page_size_seen = []

    def handler(request):
        if request.url.path == "/Users":
            return httpx.Response(200, json=[])
        start = int(request.url.params["startIndex"])
        page_size_seen.append(start)
        total = 750
        items = [
            episode_item(f"e{i}", "s1", 1, i, f"/data/tv/Show/e{i}.mkv", ["jpn"])
            for i in range(start, min(start + 500, total))
        ]
        return httpx.Response(200, json={"Items": items, "TotalRecordCount": total})

    client = make_client(handler)
    assert await client.build_index() == 750
    assert page_size_seen == [0, 500]


@pytest.mark.asyncio
async def test_get_library_data_groups_episodes_and_classifies_dubs():
    def handler(request):
        if request.url.path == "/Users":
            return httpx.Response(200, json=[])
        item_type = request.url.params["includeItemTypes"]
        if item_type == "Series":
            return items_response([
                {"Id": "s1", "Name": "Show A", "Path": "/data/tv/Show A",
                 "ImageTags": {"Primary": "abc"}},
                {"Id": "s2", "Name": "Empty Show", "Path": "/data/tv/Empty"},
            ])
        return items_response([
            episode_item("e1", "s1", 1, 1, "/data/tv/Show A/e1.mkv", ["eng", "jpn"], size=10),
            episode_item("e2", "s1", 1, 2, "/data/tv/Show A/e2.mkv", ["jpn"], size=20),
            episode_item("e3", "s1", 1, 3, "/data/tv/Show A/e3.mkv", [], size=30),
            # Belongs to no known series — must not crash the walk.
            episode_item("e4", "missing", 1, 1, "/data/tv/Ghost/e1.mkv", ["jpn"]),
        ])

    shows = await make_client(handler).get_library_data("eng")

    assert [s["title"] for s in shows] == ["Show A"]  # the empty show is dropped
    show = shows[0]
    assert show["plex_key"] == stable_key("s1")
    assert show["poster_url"].endswith("/Items/s1/Images/Primary?maxHeight=450")
    assert [e["dub_status"] for e in show["episodes"]] == ["DUBBED", "SUB_ONLY", "UNKNOWN"]
    assert [e["file_size"] for e in show["episodes"]] == [10, 20, 30]
    assert show["episodes"][0]["audio_tracks"][0]["source"] == "jellyfin"


@pytest.mark.asyncio
async def test_language_codes_are_normalised_to_iso_639_2():
    def handler(request):
        if request.url.path == "/Users":
            return httpx.Response(200, json=[])
        if request.url.params["includeItemTypes"] == "Series":
            return items_response([{"Id": "s1", "Name": "Show", "Path": "/data/tv/Show"}])
        # Jellyfin can report two-letter codes depending on the mux.
        return items_response([
            episode_item("e1", "s1", 1, 1, "/data/tv/Show/e1.mkv", ["en"]),
        ])

    shows = await make_client(handler).get_library_data("eng")
    assert shows[0]["episodes"][0]["dub_status"] == "DUBBED"


@pytest.mark.asyncio
async def test_stable_key_is_deterministic_and_positive():
    assert stable_key("abc") == stable_key("abc")
    assert stable_key("abc") != stable_key("abd")
    assert stable_key("abc") > 0


@pytest.mark.asyncio
async def test_get_libraries_lists_show_locations_with_counts():
    def handler(request):
        if request.url.path == "/Library/VirtualFolders":
            return httpx.Response(200, json=[
                {"ItemId": "lib1", "Name": "Anime", "CollectionType": "tvshows",
                 "Locations": ["/data/anime", "/data/anime2"]},
                {"ItemId": "lib2", "Name": "Movies", "CollectionType": "movies",
                 "Locations": ["/data/movies"]},
            ])
        if request.url.path == "/Users":
            return httpx.Response(200, json=[])
        return httpx.Response(200, json={"Items": [], "TotalRecordCount": 42})

    libraries = await make_client(handler).get_libraries()
    assert [lib["path"] for lib in libraries] == ["/data/anime", "/data/anime2"]
    assert all(lib["count"] == 42 and lib["type"] == "show" for lib in libraries)


@pytest.mark.asyncio
async def test_sync_collections_creates_and_diffs_membership():
    created = []
    added = []
    removed = []

    def handler(request):
        path = request.url.path
        params = request.url.params
        if path == "/Users":
            return httpx.Response(200, json=[])
        if path == "/Collections" and request.method == "POST":
            created.append((params["name"], params["ids"]))
            return httpx.Response(200, json={"Id": "new-coll"})
        if path.startswith("/Collections/") and path.endswith("/Items"):
            (added if request.method == "POST" else removed).append(params["ids"])
            return httpx.Response(204)
        if params.get("includeItemTypes") == "Series":
            return items_response([
                {"Id": "s1", "Name": "Dubbed Show"},
                {"Id": "s2", "Name": "Sub Show"},
            ])
        if params.get("includeItemTypes") == "BoxSet":
            return items_response([{"Id": "coll-dub", "Name": "Dubbed Anime"}])
        if params.get("parentId") == "coll-dub":
            # Already holds s1 plus a show that no longer belongs.
            return items_response([{"Id": "s1", "Name": "Dubbed Show"},
                                   {"Id": "s9", "Name": "Stale Show"}])
        return items_response([])

    client = make_client(handler)
    result = await client.sync_collections([
        {"title": "Dubbed Show", "dub_status": "DUBBED"},
        {"title": "Sub Show", "dub_status": "SUB_ONLY"},
    ])

    assert result["skipped"] is False
    assert created == [("Sub-Only Anime", "s2")]  # created for the missing collection
    assert added == []  # s1 already in the dubbed collection
    assert removed == ["s9"]  # the stale member is pulled out
    assert result["collections_updated"] == 2


@pytest.mark.asyncio
async def test_sync_collections_skipped_when_nothing_changed():
    def handler(request):  # pragma: no cover - must never be called
        raise AssertionError("no requests expected")

    result = await make_client(handler).sync_collections([], changed=False)
    assert result == {"collections_updated": 0, "skipped": True}


@pytest.mark.asyncio
async def test_item_fetch_failure_returns_what_it_has():
    def handler(request):
        if request.url.path == "/Users":
            return httpx.Response(200, json=[])
        return httpx.Response(500, json={})

    client = make_client(handler)
    assert await client.build_index() == 0
    # An index that exists but is empty must not be mistaken for "not built".
    assert client.is_indexed()


@pytest.mark.asyncio
async def test_media_server_only_scan_runs_against_jellyfin(monkeypatch, tmp_path):
    """A Jellyfin-only install (no Sonarr) completes a scan and stores results."""
    from src.db.database import init_db, get_db
    from src.db import models
    from src.scanner import engine

    def handler(request):
        path = request.url.path
        if path == "/System/Info":
            return httpx.Response(200, json={"ServerName": "JF", "Version": "10.10"})
        if path == "/Users":
            return httpx.Response(200, json=[])
        item_type = request.url.params.get("includeItemTypes")
        if item_type == "Series":
            return items_response([{"Id": "s1", "Name": "Show", "Path": "/data/tv/Show"}])
        if item_type == "Episode":
            return items_response([
                episode_item("e1", "s1", 1, 1, "/data/tv/Show/e1.mkv", ["jpn"]),
                episode_item("e2", "s1", 1, 2, "/data/tv/Show/e2.mkv", ["eng", "jpn"]),
            ])
        return items_response([])

    db_path = str(tmp_path / "jellyfin_only.db")
    await init_db(db_path)

    async def fake_cfg():
        return {
            "DB_PATH": db_path, "SONARR_URL": "", "SONARR_API_KEY": "",
            "JELLYFIN_URL": "http://jellyfin:8096", "JELLYFIN_API_KEY": "k",
            "MEDIA_SERVER": "jellyfin", "TARGET_LANGUAGE": "eng",
            "AUTO_COLLECTIONS_PLEX": "false", "DISCORD_WEBHOOK_URL": "",
            "SEARCH_RATE_LIMIT": 5, "SEARCH_COOLDOWN_DAYS": 7,
        }

    monkeypatch.setattr(engine, "get_effective_settings", fake_cfg)
    monkeypatch.setattr(engine, "create_media_client", lambda cfg: (make_client(handler), "jellyfin"))

    result = await engine.run_scan()

    assert result["status"] == "completed"
    assert result["mode"] == "jellyfin_only"
    assert (result["dubbed"], result["sub_only"]) == (1, 1)

    db = await get_db(db_path)
    try:
        series = await models.get_all_series(db)
        assert [s["title"] for s in series] == ["Show"]
        # Media-server-only rows are stored under negative IDs so they can
        # never collide with Sonarr's.
        assert series[0]["id"] < 0
    finally:
        await db.close()
