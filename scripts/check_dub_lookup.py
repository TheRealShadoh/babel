#!/usr/bin/env python3
"""Check that dub availability lookup works from *this* machine.

Babel's dub intelligence depends on two outside services, and when a container
cannot reach them the symptom is silence: the Dub Intelligence page just stays
empty. This script runs the same code a scheduled lookup runs, one title at a
time, and prints what each source actually said.

    docker exec babel python scripts/check_dub_lookup.py
    docker exec babel python scripts/check_dub_lookup.py "Frieren: Beyond Journey's End"
    docker exec babel python scripts/check_dub_lookup.py --from-library 10

With no arguments it checks a handful of shows whose answers are known, so a
wrong verdict is as visible as an unreachable service.
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

from src.scanner.ann import find_english_dub  # noqa: E402
from src.scanner.dub_lookup import lookup_dub_info  # noqa: E402

# Long-settled shows: the first three have English dubs beyond dispute, so a
# verdict other than "available" points at the heuristics, not the network.
KNOWN_DUBBED = ["Cowboy Bebop", "Fullmetal Alchemist: Brotherhood", "Steins;Gate"]


async def check_title(title: str, client: httpx.AsyncClient, use_ann: bool) -> dict:
    info = await lookup_dub_info(title, client=client)

    print(f"\n{title}")
    if not info["ok"]:
        if info.get("rate_limited"):
            print("  MyAnimeList : rate limited (a scheduled run would back off and retry)")
        else:
            print("  MyAnimeList : UNREACHABLE — no answer from api.jikan.moe")
    elif info["mal_id"] is None:
        print("  MyAnimeList : answered, but has no entry matching this title")
    else:
        print(f"  MyAnimeList : matched {info['source_title']!r} (mal_id={info['mal_id']}, "
              f"{info['status']})")
        print(f"                licensors: {', '.join(info['licensors']) or 'none recorded'}")
        print(f"                verdict from MAL alone: {info['dub_status']}")

    verdict = info["dub_status"]
    if use_ann and info["ok"]:
        ann = await find_english_dub(info.get("source_title") or title, client=client)
        if not ann["ok"]:
            print("  ANN         : UNREACHABLE — no answer from cdn.animenewsnetwork.com")
        elif not ann["matched_title"]:
            print("  ANN         : answered, no entry under that exact title")
        else:
            print(f"  ANN         : matched {ann['matched_title']!r} (id={ann['ann_id']}), "
                  f"English cast roles: {ann['cast_size']}")
        if ann["ok"] and ann["has_dub"] and verdict in ("unlikely", "unknown"):
            verdict = "available"

    print(f"  VERDICT     : {verdict}")
    return {"title": title, "ok": info["ok"], "verdict": verdict}


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("titles", nargs="*", help="Titles to check (default: known-dubbed shows)")
    parser.add_argument("--from-library", type=int, metavar="N",
                        help="Instead check N sub-only series from Babel's database")
    parser.add_argument("--no-ann", action="store_true",
                        help="Skip Anime News Network and use MyAnimeList only")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Seconds between titles (default 2, respects Jikan's limits)")
    args = parser.parse_args()

    titles = list(args.titles)
    expect_dubbed = not titles and not args.from_library

    if args.from_library:
        from src.config import get_settings
        from src.db import models
        from src.db.database import get_db

        db = await get_db(get_settings().DB_PATH)
        try:
            series = await models.get_dub_lookup_candidates(db)
        finally:
            await db.close()
        titles = [s["title"] for s in series[: args.from_library]]
        if not titles:
            print("No sub-only or partially dubbed series in the database yet — run a scan first.")
            return 1

    if not titles:
        titles = KNOWN_DUBBED

    print(f"Checking {len(titles)} title(s). Sources: MyAnimeList"
          f"{'' if args.no_ann else ' + Anime News Network'}")

    results = []
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        for i, title in enumerate(titles):
            results.append(await check_title(title, client, use_ann=not args.no_ann))
            if i < len(titles) - 1:
                await asyncio.sleep(args.delay)

    reached = sum(1 for r in results if r["ok"])
    print(f"\n{'=' * 60}")
    print(f"Reached MyAnimeList for {reached}/{len(results)} title(s)")

    if reached == 0:
        print("RESULT: dub lookup is NOT working — the container cannot reach api.jikan.moe.")
        print("        Check outbound HTTPS (DNS, firewall, proxy) from the Babel container.")
        return 1

    if expect_dubbed:
        wrong = [r["title"] for r in results if r["verdict"] != "available"]
        if wrong:
            print("RESULT: services are reachable, but these known-dubbed shows did not "
                  "come back 'available':")
            for title in wrong:
                print(f"        - {title}")
            print("        The lookup is running; its sources disagree about these titles.")
            return 2
        print("RESULT: dub lookup is working — every known-dubbed show resolved to 'available'.")
        return 0

    print("RESULT: dub lookup is working. Verdicts above are what Babel would store.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
