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
from src.scanner.media_server import select_media_server, server_label
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


async def _diagnose_media_server(cfg: dict, checks: list[dict]) -> list[str]:
    """Check the selected media server. Returns sample library paths."""
    from src.scanner.media_server import create_media_client

    kind = select_media_server(cfg)
    if kind == "none":
        checks.append(_check(
            "Media server", INFO, "No media server configured",
            "Without Plex or Jellyfin, audio detection falls back to ffprobe, "
            "which needs the media mounted inside the container.",
        ))
        return []

    client, _ = create_media_client(cfg)
    label = server_label(kind)
    try:
        ok, message = await client.test_connection()
        if not ok:
            checks.append(_check(
                label, ERROR, message,
                f"Check the {label} URL and credential in Settings.",
            ))
            return []
        checks.append(_check(label, OK, message))

        libraries = await client.get_libraries()
        if not libraries:
            checks.append(_check(
                f"{label} libraries", WARN, "No TV/show libraries found",
                f"Babel only reads show-type libraries. Confirm the {label} "
                "token/API key can see them.",
            ))
            return []

        total = sum(lib.get("count", 0) for lib in libraries)
        names = ", ".join(f"{lib['title']} ({lib.get('count', 0)})" for lib in libraries[:5])
        checks.append(_check(
            f"{label} libraries", OK
            if total else WARN,
            f"{len(libraries)} show library location(s), {total} episodes: {names}",
            "" if total else "The libraries are empty — nothing to match against.",
        ))
        return [lib["path"] for lib in libraries if lib.get("path")]
    except Exception as e:
        logger.exception("Media server diagnostics failed")
        checks.append(_check(label, ERROR, f"Diagnostics failed: {e}"))
        return []
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


async def run_diagnostics() -> dict:
    """Run every check and return {"checks": [...], "summary": {...}}."""
    cfg = await get_effective_settings()
    checks: list[dict] = []

    sonarr_samples = await _diagnose_sonarr(cfg, checks)
    await _diagnose_media_server(cfg, checks)
    await _diagnose_paths(cfg, sonarr_samples, checks)
    await _diagnose_ignores(checks)

    summary = {
        "errors": sum(1 for c in checks if c["level"] == ERROR),
        "warnings": sum(1 for c in checks if c["level"] == WARN),
        "ok": sum(1 for c in checks if c["level"] == OK),
    }
    return {"checks": checks, "summary": summary}
