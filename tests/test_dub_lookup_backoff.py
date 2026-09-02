"""A throttled MyAnimeList lookup must be retried, not recorded as "no dub".

Pacing at exactly Jikan's 60/minute ceiling meant nearly every request came
back 429, and a 429 was returned as dub_status "unknown" with no retry — so a
run of 196 series reported "0 available, 0 likely, 0 unlikely" and looked
identical to a library MAL genuinely knows nothing about.
"""

import httpx
import pytest

from src.scanner import dub_lookup


def _response(status, payload=None, headers=None):
    request = httpx.Request("GET", "https://api.jikan.moe/v4/anime")
    return httpx.Response(status, json=payload or {}, headers=headers or {}, request=request)


class _Client:
    """Replays a scripted sequence of Jikan responses."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    async def get(self, url, params=None):
        self.calls += 1
        resp = self._responses.pop(0) if self._responses else _response(200, {"data": []})
        resp.raise_for_status()
        return resp


@pytest.mark.asyncio
async def test_rate_limited_lookup_is_not_a_result():
    client = _Client([_response(429, headers={"Retry-After": "2"})])
    info = await dub_lookup.lookup_dub_info("Some Show", client=client)

    assert info["rate_limited"] is True
    assert info["ok"] is False
    assert info["retry_after"] == 2.0


@pytest.mark.asyncio
async def test_successful_lookup_is_marked_answered():
    payload = {"data": [{
        "mal_id": 7, "title": "Some Show", "status": "Finished Airing",
        "episodes": 12, "aired": {}, "licensors": [{"name": "Crunchyroll"}],
    }]}
    client = _Client([_response(200, payload)])
    info = await dub_lookup.lookup_dub_info("Some Show", client=client)

    assert info["ok"] is True
    assert info["dub_status"] == "available"
    assert info["mal_id"] == 7


@pytest.mark.asyncio
async def test_retry_after_is_clamped_and_validated():
    assert dub_lookup._parse_retry_after("5") == 5.0
    assert dub_lookup._parse_retry_after("9999") == dub_lookup.MAX_BACKOFF
    assert dub_lookup._parse_retry_after("") is None
    assert dub_lookup._parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") is None
    assert dub_lookup._parse_retry_after("-1") is None


@pytest.mark.asyncio
async def test_bulk_lookup_retries_a_throttled_title(monkeypatch):
    attempts = {"n": 0}
    slept: list[float] = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    async def fake_lookup(title, client=None):
        attempts["n"] += 1
        if attempts["n"] == 1:
            return {"dub_status": "unknown", "ok": False, "rate_limited": True, "retry_after": 3.0}
        return {"dub_status": "available", "ok": True, "rate_limited": False, "licensors": []}

    monkeypatch.setattr(dub_lookup, "lookup_dub_info", fake_lookup)
    results = await dub_lookup.bulk_lookup([(1, "Some Show")], delay=0, sleep=fake_sleep)

    assert attempts["n"] == 2, "a 429 must be retried, not accepted as an answer"
    assert results[1]["ok"] is True
    assert 3.0 in slept, "Retry-After must be honoured"


@pytest.mark.asyncio
async def test_bulk_lookup_keys_by_series_not_title(monkeypatch):
    async def fake_sleep(seconds):
        return None

    async def fake_lookup(title, client=None):
        return {"dub_status": "likely", "ok": True, "rate_limited": False, "licensors": []}

    monkeypatch.setattr(dub_lookup, "lookup_dub_info", fake_lookup)
    # Two distinct series sharing a title — a title-keyed result dict silently
    # dropped one of them and left it permanently unchecked.
    results = await dub_lookup.bulk_lookup(
        [(11, "Fruits Basket"), (22, "Fruits Basket")], delay=0, sleep=fake_sleep
    )

    assert set(results) == {11, 22}


@pytest.mark.asyncio
async def test_pacing_default_stays_under_the_per_minute_ceiling():
    # 60 requests/minute is the binding Jikan limit and a missed search costs
    # two requests, so the default delay has to leave real headroom.
    assert dub_lookup.DEFAULT_DELAY >= 2.0
