"""Self-diagnosis for "Babel isn't finding anything".

Every way a scan can quietly produce nothing — Sonarr unreachable, a filter
that matches no series, a media server whose paths don't line up with
Sonarr's, a library Babel was told to ignore — looks identical from the
dashboard: an empty page. This module asks each of those questions directly
and reports what it found alongside what to do about it.
"""

from __future__ import annotations

import asyncio
import logging
import os

from src.config import get_effective_settings, get_settings, translate_path
from src.db import models
from src.db.database import get_db
from src.scanner.media_server import build_client, select_media_servers, server_label
from src.scanner.sonarr import SonarrClient

logger = logging.getLogger(__name__)

# A dead NFS/SMB mount blocks os.path.exists forever. Diagnostics runs from a
# web request, so every filesystem touch is bounded.
_PATH_CHECK_TIMEOUT = 3.0

OK = "ok"
WARN = "warn"
ERROR = "error"
INFO = "info"


def _check(name: str, level: str, message: str, hint: str = "") -> dict:
    return {"name": name, "level": level, "message": message, "hint": hint}


async def _path_exists(path: str) -> bool | None:
    """True/False, or None if the check timed out on an unresponsive mount."""
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(os.path.exists, path), _PATH_CHECK_TIMEOUT
        )
    except (asyncio.TimeoutError, OSError):
        return None


async def _diagnose_sonarr(cfg: dict, checks: list[dict]) -> list[str]:
    """Check Sonarr connectivity and the series filter. Returns sample paths."""
    if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
        checks.append(_check(
            "Sonarr", INFO, "Not configured",
            "Babel can run against a media server alone, but Sonarr is what "
            "triggers dub searches.",
        ))
        return []

    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    try:
        ok, message = await sonarr.test_connection()
        if not ok:
            checks.append(_check(
                "Sonarr", ERROR, message,
                "Check the URL (include http:// and the port) and API key in Settings.",
            ))
            return []
        checks.append(_check("Sonarr", OK, message))

        all_series = await sonarr.get_all_series()
        if all_series is None:
            checks.append(_check(
                "Sonarr series", ERROR, "Could not list series",
                "Sonarr answered /system/status but not /series — check its logs.",
            ))
            return []

        filter_mode = str(cfg.get("ANIME_FILTER", "type") or "type").strip()
        counts = sonarr.series_type_counts(all_series)
        breakdown = ", ".join(f"{n} {t}" for t, n in sorted(counts.items())) or "none"

        matched = await sonarr.get_anime_series(filter_mode)
        matched_count = len(matched) if matched is not None else 0

        if not all_series:
            checks.append(_check(
                "Sonarr library", WARN, "Sonarr has no series at all",
                "Add series to Sonarr first — Babel only looks at what Sonarr manages.",
            ))
            return []

        detail = (
            f"{matched_count} of {len(all_series)} series match the filter "
            f"'{filter_mode}' (types: {breakdown})"
        )
        if matched_count == 0:
            checks.append(_check(
                "Series filter", ERROR, detail,
                sonarr.filter_hint(filter_mode, all_series),
            ))
        else:
            checks.append(_check("Series filter", OK, detail))

        # Episode files are what a scan actually reads. A series list with no
        # files behind it is the other way a scan comes back empty.
        samples: list[str] = []
        if matched:
            files_seen = 0
            for series in matched[:5]:
                files = await sonarr.get_episode_files(series["id"])
                if not files:
                    continue
                files_seen += len(files)
                for f in files[:2]:
                    if f.get("path"):
                        samples.append(f["path"])
            if files_seen == 0:
                checks.append(_check(
                    "Episode files", WARN,
                    "The first matched series have no downloaded episode files",
                    "Babel classifies files that exist on disk; series with nothing "
                    "downloaded show up as MISSING and are never probed.",
                ))
            else:
                checks.append(_check(
                    "Episode files", OK,
                    f"{files_seen} files across the first {min(len(matched), 5)} matched series",
                ))
            return samples
        return []
    except Exception as e:
        logger.exception("Sonarr diagnostics failed")
        checks.append(_check("Sonarr", ERROR, f"Diagnostics failed: {e}"))
        return []
    finally:
        await sonarr.close()


async def _diagnose_media_server(cfg: dict, checks: list[dict]) -> None:
    """Check every media server the current settings would use.

    Plex and Jellyfin are configured independently, so each one is reported
    on its own line: with both set up, one being down is a warning about that
    server, not a verdict on the other.
    """
    kinds = select_media_servers(cfg)
    if not kinds:
        checks.append(_check(
            "Media server", INFO, "No media server configured",
            "Without Plex or Jellyfin, audio detection falls back to ffprobe, "
            "which needs the media mounted inside the container.",
        ))
        return

    for kind in kinds:
        label = server_label(kind)
        client = build_client(kind, cfg)
        try:
            ok, message = await client.test_connection()
            if not ok:
                checks.append(_check(
                    label, ERROR, message,
                    f"Check the {label} URL and credential in Settings.",
                ))
                continue
            checks.append(_check(label, OK, message))

            libraries = await client.get_libraries()
            if not libraries:
                checks.append(_check(
                    f"{label} libraries", WARN, "No TV/show libraries found",
                    f"Babel only reads show-type libraries. Confirm the {label} "
                    "token/API key can see them.",
                ))
                continue

            total = sum(lib.get("count", 0) for lib in libraries)
            names = ", ".join(
                f"{lib['title']} ({lib.get('count', 0)})" for lib in libraries[:5]
            )
            checks.append(_check(
                f"{label} libraries", OK if total else WARN,
                f"{len(libraries)} show library location(s), {total} episodes: {names}",
                "" if total else "The libraries are empty — nothing to match against.",
            ))
        except Exception as e:
            logger.exception("%s diagnostics failed", label)
            checks.append(_check(label, ERROR, f"Diagnostics failed: {e}"))
        finally:
            await client.close()


async def _diagnose_paths(cfg: dict, sonarr_samples: list[str], checks: list[dict]) -> None:
    if not sonarr_samples:
        return

    sample = sonarr_samples[0]
    local_path = translate_path(sample, "local", cfg)
    exists = await _path_exists(local_path)

    if exists is True:
        checks.append(_check(
            "Path mapping", OK,
            f"Sonarr path {sample} resolves to {local_path}, which exists in the container",
        ))
    elif exists is None:
        checks.append(_check(
            "Path mapping", WARN,
            f"Checking {local_path} timed out",
            "The mount is not responding. ffprobe reads will stall on it too — "
            "see the unresponsive-mounts section of the README.",
        ))
    else:
        checks.append(_check(
            "Path mapping", WARN,
            f"Sonarr path {sample} resolves to {local_path}, which does not exist "
            "inside the Babel container",
            "This only matters for the ffprobe fallback: set SONARR_PATH_PREFIX / "
            "LOCAL_PATH_PREFIX under Path Mapping, or mount the media at the same "
            "path Sonarr uses. A media server can still supply the audio tracks.",
        ))


async def _diagnose_ignores(checks: list[dict]) -> None:
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        ignored = await models.get_ignored_paths(db)
        stats = await models.get_overview_stats(db)
        async with db.execute("SELECT COUNT(*) AS c FROM episodes") as cur:
            row = await cur.fetchone()
        episode_count = (row["c"] if row else 0) or 0
        scans = await models.get_scan_logs(db, limit=1)
    finally:
        await db.close()

    if ignored:
        patterns = ", ".join(p["pattern"] for p in ignored[:10])
        checks.append(_check(
            "Ignored paths", WARN if len(ignored) else INFO,
            f"{len(ignored)} ignore pattern(s) active: {patterns}",
            "Anything whose path contains one of these is skipped entirely.",
        ))

    series_count = stats.get("total_series", 0)
    checks.append(_check(
        "Babel database",
        OK if episode_count else WARN,
        f"{series_count} series / {episode_count} episodes stored",
        "" if episode_count else "Nothing has been stored yet — the checks above "
        "should say why.",
    ))

    if scans:
        last = scans[0]
        message = f"Last run {last.get('started_at')} — status {last.get('status')}"
        if last.get("error_message"):
            checks.append(_check(
                "Last scan", WARN, message, last["error_message"],
            ))
        else:
            checks.append(_check("Last scan", INFO, message))
    else:
        checks.append(_check(
            "Last scan", WARN, "No scan has run yet",
            "Trigger one with Scan Now on the Overview page.",
        ))


# One canary lookup per diagnostics run, bounded so a slow or blocked network
# cannot hold the request open.
_DUB_LOOKUP_TIMEOUT = 25.0


async def _diagnose_dub_lookup(checks: list[dict]) -> None:
    """Prove the dub-availability sources are actually reachable from here."""
    from src.scanner.dub_lookup import self_test

    try:
        report = await asyncio.wait_for(self_test(), _DUB_LOOKUP_TIMEOUT)
    except asyncio.TimeoutError:
        checks.append(_check(
            "Dub lookup", WARN, "The lookup did not finish within 25s",
            "MyAnimeList (api.jikan.moe) is slow or unreachable from the container. "
            "Dub intelligence will retry on its next run; scanning is unaffected.",
        ))
        return
    except Exception as e:
        logger.exception("Dub lookup self-test failed")
        checks.append(_check("Dub lookup", ERROR, f"Self-test failed: {e}"))
        return

    mal = report["mal"]
    if mal["ok"]:
        detail = f"MyAnimeList answered for '{report['title']}' — matched {mal['matched']!r}"
        if mal["licensors"]:
            detail += f", licensors: {', '.join(mal['licensors'][:3])}"
        checks.append(_check("Dub lookup (MyAnimeList)", OK, detail))
    elif mal["rate_limited"]:
        checks.append(_check(
            "Dub lookup (MyAnimeList)", WARN, "Rate limited by api.jikan.moe",
            "Normal under load — lookups back off and retry. Persistent throttling "
            "means the daily run will take longer, not that it is broken.",
        ))
    else:
        checks.append(_check(
            "Dub lookup (MyAnimeList)", ERROR, "Could not reach api.jikan.moe",
            "Check outbound HTTPS from the container (DNS, firewall, proxy). "
            "Dub availability stays blank until this works; audio detection and "
            "searches are unaffected.",
        ))

    ann = report["ann"]
    if not ann["enabled"]:
        checks.append(_check(
            "Dub lookup (Anime News Network)", INFO, "Disabled in Settings",
            "MyAnimeList alone cannot tell a sub-only licence from a dubbed one.",
        ))
    elif ann["ok"]:
        detail = f"ANN answered — matched {ann['matched']!r}" if ann["matched"] else \
            "ANN answered but has no entry under that title"
        if ann["has_dub"]:
            detail += f", English dub cast of {ann['cast_size']} roles"
        checks.append(_check("Dub lookup (Anime News Network)", OK, detail))
    else:
        checks.append(_check(
            "Dub lookup (Anime News Network)", WARN,
            "Could not reach cdn.animenewsnetwork.com",
            "Babel falls back to MyAnimeList alone, which is less certain for "
            "currently-airing shows.",
        ))

    checks.append(_check(
        "Dub lookup verdict", INFO,
        f"'{report['title']}' resolved to '{report['verdict']}'",
        "A known-dubbed show should resolve to 'available'.",
    ))


async def run_diagnostics() -> dict:
    """Run every check and return {"checks": [...], "summary": {...}}."""
    cfg = await get_effective_settings()
    checks: list[dict] = []

    sonarr_samples = await _diagnose_sonarr(cfg, checks)
    await _diagnose_media_server(cfg, checks)
    await _diagnose_paths(cfg, sonarr_samples, checks)
    await _diagnose_dub_lookup(checks)
    await _diagnose_ignores(checks)

    summary = {
        "errors": sum(1 for c in checks if c["level"] == ERROR),
        "warnings": sum(1 for c in checks if c["level"] == WARN),
        "ok": sum(1 for c in checks if c["level"] == OK),
    }
    return {"checks": checks, "summary": summary}
