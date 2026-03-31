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
            logger.warning("Jikan rate limited, will retry later")
        else:
            logger.warning("Jikan API error for '%s': %s", title, e)
    except Exception as e:
        logger.warning("Dub lookup failed for '%s': %s", title, e)
    finally:
        if owns_client:
            await client.aclose()

    return result


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


async def bulk_lookup(titles: list[str], delay: float = 1.0) -> dict[str, dict]:
    """Look up dub info for multiple titles with rate limiting.
    Jikan has a 3 requests/second rate limit.
    Returns: {title: dub_info_dict, ...}
    """
    results = {}
    async with httpx.AsyncClient(timeout=15) as client:
        for i, title in enumerate(titles):
            results[title] = await lookup_dub_info(title, client=client)
            if i < len(titles) - 1:
                await asyncio.sleep(delay)  # Rate limit
            if (i + 1) % 10 == 0:
                logger.info("Dub lookup: %d/%d titles checked", i + 1, len(titles))
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
    summary = {"checked": 0, "available": 0, "likely": 0, "unlikely": 0}

    # Log to scan_log as a "dub_lookup" type
    scan_id = await models.start_scan_log(db)

    try:
        if force:
            # Re-check all sub-only/partial series regardless of existing data
            from src.db.models import _rows_to_dicts
            async with db.execute(
                "SELECT * FROM series WHERE dub_status IN ('SUB_ONLY', 'PARTIAL') ORDER BY title"
            ) as cur:
                series_list = _rows_to_dicts(await cur.fetchall())
        else:
            series_list = await models.get_series_needing_dub_lookup(db)

        if not series_list:
            await models.complete_scan_log(db, scan_id, 0, 0, 0, "completed",
                                           "Dub lookup: all series already checked")
            return summary

        titles = [s["title"] for s in series_list]
        title_to_series = {s["title"]: s for s in series_list}

        logger.info("Dub lookup started: %d series to check", len(titles))
        results = await bulk_lookup(titles, delay=1.0)

        newly_available = []
        for title, info in results.items():
            series = title_to_series.get(title)
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

        summary["checked"] = len(titles)
        summary["available"] = sum(1 for r in results.values() if r["dub_status"] == "available")
        summary["likely"] = sum(1 for r in results.values() if r["dub_status"] == "likely")
        summary["unlikely"] = sum(1 for r in results.values() if r["dub_status"] == "unlikely")

        msg = f"Dub lookup: {summary['checked']} checked, {summary['available']} available, {summary['likely']} likely, {summary['unlikely']} unlikely"
        await models.complete_scan_log(db, scan_id, summary["checked"], 0, 0, "completed", msg)
        logger.info(msg)

    except Exception as e:
        logger.exception("Dub lookup failed: %s", e)
        await models.complete_scan_log(db, scan_id, summary["checked"], 0, 0, "failed", f"Dub lookup failed: {e}")
    finally:
        await db.close()

    return summary
