"""Anime News Network encyclopedia lookup — does an English dub cast exist?

MyAnimeList only records *licensors*, which is an inference: a licensor may
distribute subtitled-only, and for a show still airing MAL usually has no
licensor at all. ANN lists the actual voice cast per language, so an English
cast is direct evidence that a dub was produced — the one question Babel
actually wants answered.

Used as a second opinion when MAL is inconclusive, not as the primary source:
ANN asks API users to keep to about one request a second, and every title
costs two requests here (search, then detail).
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET

import httpx

logger = logging.getLogger(__name__)

ANN_BASE = "https://cdn.animenewsnetwork.com/encyclopedia"

# ANN's "anime titles" report. Anything else returns a different schema.
_TITLES_REPORT_ID = 155

# ANN responses are external input. ElementTree expands internal entities, so
# a hostile or corrupted document could blow up memory; both guards below make
# that impossible without pulling in a parsing dependency.
_MAX_BYTES = 4 * 1024 * 1024
_DOCTYPE_RE = re.compile(rb"<!\s*(DOCTYPE|ENTITY)", re.IGNORECASE)


def _parse_xml(payload: bytes) -> ET.Element | None:
    if len(payload) > _MAX_BYTES:
        logger.warning("ANN response too large (%d bytes) — ignoring", len(payload))
        return None
    if _DOCTYPE_RE.search(payload):
        logger.warning("ANN response carries a DTD — ignoring rather than parsing it")
        return None
    try:
        return ET.fromstring(payload)
    except ET.ParseError as e:
        logger.warning("Could not parse ANN response: %s", e)
        return None


def _normalize(title: str) -> str:
    """Loosen a title enough to compare two catalogues' spellings of it."""
    return re.sub(r"[^a-z0-9]+", " ", (title or "").lower()).strip()


def _best_match(title: str, items: list[tuple[str, str]]) -> tuple[str, str] | None:
    """Pick the ANN entry for *title* from (id, name) pairs.

    Exact (normalised) equality only. ANN's search is a substring match, so
    "Naruto" also returns "Naruto Shippuden" — accepting a near miss would
    attribute one show's dub to another, which is worse than no answer.
    """
    wanted = _normalize(title)
    if not wanted:
        return None
    for ann_id, name in items:
        if _normalize(name) == wanted:
            return ann_id, name
    return None


async def find_english_dub(title: str, client: httpx.AsyncClient) -> dict:
    """Ask ANN whether *title* has an English dub cast.

    Returns {"ok": bool, "has_dub": bool, "ann_id": str|None,
             "matched_title": str|None, "cast_size": int}. "ok" is False when
    ANN could not be reached or understood — never conflated with "no dub".
    """
    result = {"ok": False, "has_dub": False, "ann_id": None,
              "matched_title": None, "cast_size": 0}

    try:
        resp = await client.get(
            f"{ANN_BASE}/reports.xml",
            params={"id": _TITLES_REPORT_ID, "type": "anime", "name": title, "nlist": 10},
        )
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.debug("ANN title search failed for %r: %s", title, e)
        return result

    root = _parse_xml(resp.content)
    if root is None:
        return result

    candidates: list[tuple[str, str]] = []
    for item in root.iter("item"):
        ann_id = (item.findtext("id") or "").strip()
        name = (item.findtext("name") or "").strip()
        if ann_id and name:
            candidates.append((ann_id, name))

    # ANN answered; from here on a negative is a real negative.
    result["ok"] = True

    match = _best_match(title, candidates)
    if match is None:
        return result

    ann_id, matched_title = match
    result["ann_id"] = ann_id
    result["matched_title"] = matched_title

    try:
        resp = await client.get(f"{ANN_BASE}/api.xml", params={"anime": ann_id})
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.debug("ANN detail lookup failed for %r (id=%s): %s", title, ann_id, e)
        result["ok"] = False
        return result

    detail = _parse_xml(resp.content)
    if detail is None:
        result["ok"] = False
        return result

    cast_size = sum(
        1 for cast in detail.iter("cast")
        if (cast.get("lang") or "").upper() == "EN"
    )
    result["cast_size"] = cast_size
    result["has_dub"] = cast_size > 0
    return result
