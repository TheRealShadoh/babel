"""
Dub availability lookup via Jikan API (MyAnimeList unofficial API).

Checks whether an anime has known English dub licensors, helping users
know if a dub even exists before wasting searches.
"""

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

JIKAN_BASE = "https://api.jikan.moe/v4"

# Jikan allows 3 requests/second and 60/minute. The per-minute ceiling is the
# binding one, and a title that misses the type=tv search spends two requests,
# so pacing at exactly 60/minute guarantees throttling. Back off further when
# the server says to.
DEFAULT_DELAY = 2.0
MAX_RETRIES = 3
MAX_BACKOFF = 60.0

# Companies known to produce English dubs
DUB_LICENSORS = {
    "funimation", "crunchyroll", "sentai filmworks", "aniplex of america",
    "viz media", "hidive", "discotek media", "adv films", "bang zoom!",
    "bandai entertainment", "nis america", "nozomi entertainment",
    "media play news", "geneon entertainment usa", "manga entertainment",
}


async def lookup_dub_info(title: str, client: httpx.AsyncClient | None = None) -> dict:
    """Look up dub availability for an anime title via Jikan/MAL.

    Returns: {
        "mal_id": int or None,
        "dub_status": "available" | "likely" | "unlikely" | "unknown",
        "licensors": ["Funimation", ...],
        "status": "Finished Airing" | "Currently Airing" | ...,
        "aired_from": "2024-01-01" or None,
        "aired_to": "2024-06-01" or None,
        "episodes": 12,
        "source_title": "matched title from MAL",
    }
    """
    result = {
        "mal_id": None, "dub_status": "unknown", "licensors": [],
        "status": None, "aired_from": None, "aired_to": None,
        "episodes": None, "source_title": None,
        # "ok" separates "MAL answered and knows nothing" from "we never got
        # an answer". Only the former is worth recording; treating a throttled
        # request as a real result is what made every lookup report zeroes.
        "ok": False,
        "rate_limited": False,
    }

    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=15)

    try:
        # Search by title
        resp = await client.get(f"{JIKAN_BASE}/anime", params={
            "q": title, "limit": 3, "type": "tv",
        })
        resp.raise_for_status()
        data = resp.json().get("data", [])

        if not data:
            # Try without "tv" filter
            resp = await client.get(f"{JIKAN_BASE}/anime", params={
                "q": title, "limit": 3,
            })
            resp.raise_for_status()
            data = resp.json().get("data", [])

        result["ok"] = True

        if not data:
            return result

        # Find best title match
        anime = _best_match(title, data)
        if not anime:
            anime = data[0]

        result["mal_id"] = anime.get("mal_id")
        result["source_title"] = anime.get("title")
        result["status"] = anime.get("status")
        result["episodes"] = anime.get("episodes")

        aired = anime.get("aired", {})
        result["aired_from"] = aired.get("from", "")[:10] if aired.get("from") else None
        result["aired_to"] = aired.get("to", "")[:10] if aired.get("to") else None

        # Check licensors
        licensors = [lic.get("name", "") for lic in anime.get("licensors", [])]
        result["licensors"] = licensors

        licensor_names = {lic.lower() for lic in licensors}
        if licensor_names & DUB_LICENSORS:
            result["dub_status"] = "available"
        elif anime.get("licensors"):
            result["dub_status"] = "likely"
        else:
            result["dub_status"] = "unlikely"

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            result["rate_limited"] = True
            retry_after = e.response.headers.get("retry-after", "")
            result["retry_after"] = _parse_retry_after(retry_after)
            logger.warning(
                "Jikan rate limited on '%s' (retry-after: %s)", title, retry_after or "unset"
            )
        else:
            logger.warning("Jikan API error for '%s': %s", title, e)
    except Exception as e:
        logger.warning("Dub lookup failed for '%s': %s", title, e)
    finally:
        if owns_client:
            await client.aclose()

    return result


def _parse_retry_after(value: str) -> float | None:
    """Seconds to wait from a Retry-After header, or None if unusable."""
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if seconds < 0:
        return None
    return min(seconds, MAX_BACKOFF)


def _best_match(title: str, results: list[dict]) -> dict | None:
    """Find the best matching anime from search results."""
    title_lower = title.lower().strip()
    for anime in results:
        for field in ["title", "title_english", "title_japanese"]:
            if anime.get(field) and anime[field].lower().strip() == title_lower:
                return anime
        # Check alternative titles
        for alt in anime.get("titles", []):
            if alt.get("title", "").lower().strip() == title_lower:
                return anime
    return None


async def bulk_lookup(
    items: list[tuple], delay: float = DEFAULT_DELAY, sleep=None
) -> dict:
    """Look up dub info for many series, pacing requests and backing off on 429.

    *items* is a list of ``(key, title)`` pairs and the result is keyed by
    ``key``. Keying by the caller's own identifier rather than by title
    matters: two series can share a title, and a title-keyed dict silently
    dropped one of them.

    A rate-limited title is retried with exponential backoff (honouring
    ``Retry-After`` when present) instead of being recorded as "no dub known".
    """
    sleep = sleep or asyncio.sleep
    results: dict = {}
    total = len(items)
    async with httpx.AsyncClient(timeout=15) as client:
        for i, (key, title) in enumerate(items):
            backoff = delay
            for attempt in range(MAX_RETRIES):
                info = await lookup_dub_info(title, client=client)
                if not info.get("rate_limited"):
                    break
                wait = info.get("retry_after") or backoff
                backoff = min(backoff * 2, MAX_BACKOFF)
                if attempt < MAX_RETRIES - 1:
                    logger.info(
                        "Backing off %.1fs before retrying '%s' (attempt %d/%d)",
                        wait, title, attempt + 2, MAX_RETRIES,
                    )
                    await sleep(wait)
            results[key] = info
            if i < total - 1:
                await sleep(delay)
            if (i + 1) % 10 == 0:
                logger.info("Dub lookup: %d/%d series checked", i + 1, total)
    return results


async def run_dub_lookup(force: bool = False) -> dict:
    """Run dub availability lookup for series that need it.

    Records the run in scan_log for history tracking.
    Returns: {"checked": N, "available": N, "likely": N, "unlikely": N}
    """
    from src.config import get_settings, get_effective_settings
    from src.db.database import get_db
    from src.db import models

    settings = get_settings()
    cfg = await get_effective_settings()
    db = await get_db(settings.DB_PATH)
    summary = {"checked": 0, "available": 0, "likely": 0, "unlikely": 0,
               "unreachable": 0}

    # Recorded in scan_log for history, but tagged so the Overview does not
    # mistake a dub lookup for the most recent media scan.
    scan_id = await models.start_scan_log(db, kind="dub_lookup")

    try:
        if force:
            series_list = await models.get_dub_lookup_candidates(db)
        else:
            series_list = await models.get_series_needing_dub_lookup(db)

        if not series_list:
            await models.complete_scan_log(db, scan_id, 0, 0, 0, "completed",
                                           "Dub lookup: all series already checked")
            return summary

        # Keyed by series id, not title: two series can share a title and a
        # title-keyed map silently dropped one of them.
        items = [(s["id"], s["title"]) for s in series_list]
        by_id = {s["id"]: s for s in series_list}

        logger.info("Dub lookup started: %d series to check", len(items))
        results = await bulk_lookup(items)

        newly_available = []
        for series_id, info in results.items():
            series = by_id.get(series_id)
            if not info.get("ok"):
                # Never reached MAL — leave the stored value and the
                # dub_checked_at stamp alone so it gets retried.
                summary["unreachable"] += 1
                continue
            if series and info["dub_status"] != "unknown":
                old_status = series.get("dub_available")
                new_status = info["dub_status"]

                # Detect transition to available/likely
                if new_status in ("available", "likely") and old_status in (None, "unknown", "unlikely"):
                    newly_available.append({
                        "title": series["title"],
                        "licensors": ", ".join(info["licensors"]),
                        "poster_url": series.get("poster_url"),
                    })

                licensors_str = ", ".join(info["licensors"]) if info["licensors"] else ""
                await models.update_dub_availability(
                    db, series["id"], new_status, licensors_str, info["mal_id"]
                )

        if newly_available:
            webhook_url = cfg.get("DISCORD_WEBHOOK_URL", "")
            if webhook_url:
                from src.notifications import send_discord_embed
                lines = [f"**{s['title']}** — {s['licensors']}" for s in newly_available[:10]]
                await send_discord_embed(
                    webhook_url,
                    f"\U0001f389 Dub Announced for {len(newly_available)} Series!",
                    "\n".join(lines),
                    color=0xa855f7,
                    thumbnail_url=newly_available[0].get("poster_url") if newly_available else None,
                )
            logger.info("Dub newly available for %d series", len(newly_available))

        answered = [r for r in results.values() if r.get("ok")]
        summary["checked"] = len(answered)
        summary["available"] = sum(1 for r in answered if r["dub_status"] == "available")
        summary["likely"] = sum(1 for r in answered if r["dub_status"] == "likely")
        summary["unlikely"] = sum(1 for r in answered if r["dub_status"] == "unlikely")

        msg = (
            f"Dub lookup: {summary['checked']} checked, {summary['available']} available, "
            f"{summary['likely']} likely, {summary['unlikely']} unlikely"
        )
        if summary["unreachable"]:
            msg += f", {summary['unreachable']} unreachable"
            logger.warning(
                "%d of %d dub lookups never reached MyAnimeList and were left "
                "unrecorded for a later retry.", summary["unreachable"], len(results),
            )
        await models.complete_scan_log(db, scan_id, summary["checked"], 0, 0, "completed", msg)
        logger.info(msg)

    except Exception as e:
        logger.exception("Dub lookup failed: %s", e)
        await models.complete_scan_log(db, scan_id, summary["checked"], 0, 0, "failed", f"Dub lookup failed: {e}")
    finally:
        await db.close()

    return summary
