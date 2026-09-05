"""Dub availability: what each source is asked, and what its answer means.

The verdicts here are the whole point of the feature — a wrong "no dub known"
takes a show off the Dub Expected list and (with auto-monitor on) leaves its
episodes unmonitored in Sonarr.
"""

import httpx
import pytest

from src.scanner import ann, dub_lookup


# ---------------------------------------------------------------------------
# MyAnimeList classification
# ---------------------------------------------------------------------------


def jikan_anime(mal_id=1, title="Show", english=None, status="Finished Airing",
                licensors=(), aired_from="2015-01-01T00:00:00+00:00"):
    return {
        "mal_id": mal_id, "title": title, "title_english": english,
        "titles": [{"type": "Default", "title": title}]
        + ([{"type": "English", "title": english}] if english else []),
        "type": "TV", "status": status, "episodes": 12,
        "aired": {"from": aired_from, "to": None},
        "licensors": [{"name": name} for name in licensors],
    }


def jikan_client(results_by_query):
    def handler(request):
        query = request.url.params["q"]
        return httpx.Response(200, json={"data": results_by_query.get(query, [])})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_known_dub_licensor_is_available():
    client = jikan_client({"Show": [jikan_anime(licensors=["Funimation"])]})
    info = await dub_lookup.lookup_dub_info("Show", client=client)
    assert info["dub_status"] == "available"


@pytest.mark.asyncio
async def test_streaming_platforms_count_as_dub_licensors():
    """Netflix and friends commission more dubs than most classic licensors."""
    for platform in ("Netflix", "Amazon Prime Video", "HIDIVE", "Muse Communication"):
        client = jikan_client({"Show": [jikan_anime(licensors=[platform])]})
        info = await dub_lookup.lookup_dub_info("Show", client=client)
        assert info["dub_status"] == "available", platform


@pytest.mark.asyncio
async def test_licensor_matching_ignores_case_and_padding():
    client = jikan_client({"Show": [jikan_anime(licensors=["  CRUNCHYROLL "])]})
    info = await dub_lookup.lookup_dub_info("Show", client=client)
    assert info["dub_status"] == "available"


@pytest.mark.asyncio
async def test_unrecognised_licensor_is_only_likely():
    client = jikan_client({"Show": [jikan_anime(licensors=["Some Regional Distributor"])]})
    info = await dub_lookup.lookup_dub_info("Show", client=client)
    assert info["dub_status"] == "likely"


@pytest.mark.asyncio
async def test_finished_show_with_no_licensor_is_unlikely():
    client = jikan_client({"Show": [jikan_anime(licensors=[])]})
    info = await dub_lookup.lookup_dub_info("Show", client=client)
    assert info["dub_status"] == "unlikely"


@pytest.mark.asyncio
async def test_airing_show_with_no_licensor_is_unknown_not_no_dub():
    """MAL rarely has licensor data while a show is still airing.

    Calling that "no dub" put currently-airing shows on the No Dub list and
    stopped them being re-checked for a month, which is exactly the window in
    which a dub gets announced.
    """
    for status in ("Currently Airing", "Not yet aired"):
        client = jikan_client({"Show": [jikan_anime(status=status, licensors=[])]})
        info = await dub_lookup.lookup_dub_info("Show", client=client)
        assert info["dub_status"] == "unknown", status


@pytest.mark.asyncio
async def test_english_title_matches_the_romaji_entry():
    client = jikan_client({"Frieren: Beyond Journey's End": [
        jikan_anime(mal_id=52991, title="Sousou no Frieren",
                    english="Frieren: Beyond Journey's End", licensors=["Crunchyroll"]),
    ]})
    info = await dub_lookup.lookup_dub_info("Frieren: Beyond Journey's End", client=client)
    assert info["mal_id"] == 52991
    assert info["source_title"] == "Sousou no Frieren"


@pytest.mark.asyncio
async def test_exact_title_beats_a_more_relevant_spin_off():
    client = jikan_client({"Attack on Titan": [
        jikan_anime(mal_id=20, title="Shingeki no Kyojin: Kuinaki Sentaku",
                    english="Attack on Titan: No Regrets", licensors=[]),
        jikan_anime(mal_id=16498, title="Shingeki no Kyojin",
                    english="Attack on Titan", licensors=["Funimation"]),
    ]})
    info = await dub_lookup.lookup_dub_info("Attack on Titan", client=client)
    assert info["mal_id"] == 16498
    assert info["dub_status"] == "available"


@pytest.mark.asyncio
async def test_tv_search_miss_falls_back_to_an_unfiltered_search():
    calls = []

    def handler(request):
        calls.append(dict(request.url.params))
        if "type" in request.url.params:
            return httpx.Response(200, json={"data": []})
        return httpx.Response(200, json={"data": [jikan_anime(title="Movie Thing")]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    info = await dub_lookup.lookup_dub_info("Movie Thing", client=client)

    assert [c.get("type") for c in calls] == ["tv", None]
    assert info["ok"] is True and info["source_title"] == "Movie Thing"


@pytest.mark.asyncio
async def test_no_match_is_answered_but_unknown():
    """MAL knowing nothing is a real answer; it must not read as unreachable."""
    client = jikan_client({})
    info = await dub_lookup.lookup_dub_info("Nonexistent Show", client=client)
    assert info["ok"] is True
    assert info["dub_status"] == "unknown" and info["mal_id"] is None


# ---------------------------------------------------------------------------
# Anime News Network
# ---------------------------------------------------------------------------


REPORT_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<report>
  <item><id>4084</id><gid>1</gid><type>TV</type><name>Cowboy Bebop</name></item>
  <item><id>1234</id><gid>2</gid><type>TV</type><name>Cowboy Bebop: The Movie</name></item>
</report>"""

DETAIL_WITH_DUB = b"""<?xml version="1.0" encoding="UTF-8"?>
<ann>
  <anime id="4084" name="Cowboy Bebop" type="TV">
    <cast lang="JA"><role>Spike</role><person id="1">Koichi Yamadera</person></cast>
    <cast lang="EN"><role>Spike</role><person id="2">Steve Blum</person></cast>
    <cast lang="EN"><role>Faye</role><person id="3">Wendee Lee</person></cast>
  </anime>
</ann>"""

DETAIL_SUB_ONLY = b"""<?xml version="1.0" encoding="UTF-8"?>
<ann>
  <anime id="4084" name="Cowboy Bebop" type="TV">
    <cast lang="JA"><role>Spike</role><person id="1">Koichi Yamadera</person></cast>
  </anime>
</ann>"""


def ann_client(report=REPORT_XML, detail=DETAIL_WITH_DUB, report_status=200, detail_status=200):
    def handler(request):
        if request.url.path.endswith("reports.xml"):
            return httpx.Response(report_status, content=report)
        return httpx.Response(detail_status, content=detail)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_ann_reports_an_english_cast_as_a_dub():
    result = await ann.find_english_dub("Cowboy Bebop", client=ann_client())
    assert result == {"ok": True, "has_dub": True, "ann_id": "4084",
                      "matched_title": "Cowboy Bebop", "cast_size": 2}


@pytest.mark.asyncio
async def test_ann_japanese_only_cast_is_a_real_negative():
    result = await ann.find_english_dub("Cowboy Bebop", client=ann_client(detail=DETAIL_SUB_ONLY))
    assert result["ok"] is True and result["has_dub"] is False


@pytest.mark.asyncio
async def test_ann_only_accepts_an_exact_title_match():
    """ANN's search is a substring match, so 'Naruto' also returns sequels.

    Crediting one show's dub to another is worse than returning no answer.
    """
    report = b"""<report><item><id>9</id><name>Cowboy Bebop: The Movie</name></item></report>"""
    result = await ann.find_english_dub("Cowboy Bebop", client=ann_client(report=report))
    assert result["ok"] is True
    assert result["ann_id"] is None and result["has_dub"] is False


@pytest.mark.asyncio
async def test_ann_title_matching_tolerates_punctuation_differences():
    report = b"""<report><item><id>9</id><name>Steins;Gate</name></item></report>"""
    result = await ann.find_english_dub("Steins Gate", client=ann_client(report=report))
    assert result["ann_id"] == "9"


@pytest.mark.asyncio
async def test_ann_failures_are_never_read_as_no_dub():
    unreachable = await ann.find_english_dub("Cowboy Bebop", client=ann_client(report_status=500))
    assert unreachable["ok"] is False and unreachable["has_dub"] is False

    detail_down = await ann.find_english_dub("Cowboy Bebop", client=ann_client(detail_status=503))
    assert detail_down["ok"] is False

    malformed = await ann.find_english_dub("Cowboy Bebop", client=ann_client(report=b"<not xml"))
    assert malformed["ok"] is False


@pytest.mark.asyncio
async def test_ann_refuses_to_parse_a_document_with_a_dtd():
    """A declared entity is the billion-laughs shape; refuse rather than expand."""
    bomb = b"""<?xml version="1.0"?><!DOCTYPE r [<!ENTITY a "aaaa">]><report><item>
        <id>1</id><name>&a;</name></item></report>"""
    result = await ann.find_english_dub("Cowboy Bebop", client=ann_client(report=bomb))
    assert result["ok"] is False


# ---------------------------------------------------------------------------
# The two sources together
# ---------------------------------------------------------------------------


async def _no_sleep(_seconds):
    return None


@pytest.mark.asyncio
async def test_ann_upgrades_an_unsettled_mal_verdict():
    results = {
        1: {"ok": True, "dub_status": "unknown", "source_title": "Airing Show", "licensors": []},
        2: {"ok": True, "dub_status": "unlikely", "source_title": "Old Show", "licensors": []},
    }

    changed = await dub_lookup.corroborate_with_ann(
        results, sleep=_no_sleep, client=ann_client(report=b"""<report>
            <item><id>1</id><name>Airing Show</name></item>
            <item><id>2</id><name>Old Show</name></item></report>"""),
    )

    assert changed == 2
    assert [r["dub_status"] for r in results.values()] == ["available", "available"]
    assert "Anime News Network" in results[1]["licensors"][0]


@pytest.mark.asyncio
async def test_ann_is_not_asked_about_titles_mal_already_settled():
    asked = []

    def handler(request):
        asked.append(request.url.params.get("name"))
        return httpx.Response(200, content=REPORT_XML)

    results = {
        1: {"ok": True, "dub_status": "available", "source_title": "Settled", "licensors": []},
        2: {"ok": True, "dub_status": "likely", "source_title": "Also Settled", "licensors": []},
        3: {"ok": False, "dub_status": "unknown", "source_title": None, "licensors": []},
    }

    changed = await dub_lookup.corroborate_with_ann(
        results, sleep=_no_sleep,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    assert changed == 0 and asked == []


@pytest.mark.asyncio
async def test_ann_saying_no_leaves_the_mal_verdict_alone():
    results = {1: {"ok": True, "dub_status": "unlikely", "source_title": "Cowboy Bebop",
                   "licensors": []}}

    changed = await dub_lookup.corroborate_with_ann(
        results, sleep=_no_sleep, client=ann_client(detail=DETAIL_SUB_ONLY),
    )

    assert changed == 0
    assert results[1]["dub_status"] == "unlikely"


@pytest.mark.asyncio
async def test_ann_being_down_does_not_break_the_run():
    results = {1: {"ok": True, "dub_status": "unlikely", "source_title": "Show", "licensors": []}}

    changed = await dub_lookup.corroborate_with_ann(
        results, sleep=_no_sleep, client=ann_client(report_status=500),
    )

    assert changed == 0
    assert results[1]["dub_status"] == "unlikely"


# ---------------------------------------------------------------------------
# Self-test (what Diagnostics and the CLI report)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_self_test_reports_both_sources(monkeypatch):
    async def fake_cfg():
        return {"DUB_LOOKUP_ANN": "true"}

    async def fake_lookup(title, client=None):
        return {"ok": True, "rate_limited": False, "source_title": "Cowboy Bebop",
                "dub_status": "unlikely", "licensors": [], "mal_id": 1}

    async def fake_ann(title, client=None):
        return {"ok": True, "has_dub": True, "ann_id": "4084",
                "matched_title": "Cowboy Bebop", "cast_size": 12}

    monkeypatch.setattr(dub_lookup, "get_effective_settings", fake_cfg, raising=False)
    monkeypatch.setattr("src.config.get_effective_settings", fake_cfg)
    monkeypatch.setattr(dub_lookup, "lookup_dub_info", fake_lookup)
    monkeypatch.setattr("src.scanner.ann.find_english_dub", fake_ann)

    report = await dub_lookup.self_test("Cowboy Bebop")

    assert report["mal"]["ok"] is True
    assert report["ann"]["has_dub"] is True
    # ANN's direct evidence overrides MAL's inference.
    assert report["verdict"] == "available"


@pytest.mark.asyncio
async def test_self_test_flags_an_unreachable_mal(monkeypatch):
    async def fake_cfg():
        return {"DUB_LOOKUP_ANN": "false"}

    async def fake_lookup(title, client=None):
        return {"ok": False, "rate_limited": False, "source_title": None,
                "dub_status": "unknown", "licensors": [], "mal_id": None}

    monkeypatch.setattr("src.config.get_effective_settings", fake_cfg)
    monkeypatch.setattr(dub_lookup, "lookup_dub_info", fake_lookup)

    report = await dub_lookup.self_test("Cowboy Bebop")

    assert report["mal"]["ok"] is False and report["mal"]["reachable"] is False
    assert report["ann"]["enabled"] is False
