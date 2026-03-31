import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

from src.config import get_settings, get_effective_settings, normalize_language
from src.scanner.sonarr import SonarrClient
from src.scanner.plex import PlexClient
from src.scanner import ffprobe
from src.db.database import get_db
from src.db import models

logger = logging.getLogger(__name__)

_scan_lock = asyncio.Lock()
_scan_cancel = asyncio.Event()
_plex_client_ref = None

# Live scan progress — read by the UI for real-time updates
_scan_progress = {
    "phase": "",          # "indexing_plex", "scanning", "idle"
    "current_series": "",
    "series_index": 0,
    "series_total": 0,
    "episodes_checked": 0,
    "dubbed_found": 0,
    "sub_only_found": 0,
    "searches_triggered": 0,
    "last_log": "",
}


def get_scan_progress() -> dict:
    return dict(_scan_progress)


def request_scan_cancel():
    """Signal the running scan to stop after the current series."""
    _scan_cancel.set()
    # Also cancel the Plex client's blocking thread operations
    if _plex_client_ref is not None:
        _plex_client_ref._cancel.set()


def is_scan_running() -> bool:
    return _scan_lock.locked()


def _translate_path(sonarr_path: str, target: str, cfg: dict) -> str:
    prefix = cfg.get("SONARR_PATH_PREFIX", "")
    if not prefix or not sonarr_path.startswith(prefix):
        return sonarr_path
    if target == "plex":
        replacement = cfg.get("PLEX_PATH_PREFIX", "") or cfg.get("LOCAL_PATH_PREFIX", "/media")
    else:
        replacement = cfg.get("LOCAL_PATH_PREFIX", "/media")
    return sonarr_path.replace(prefix, replacement, 1)


class RateLimiter:
    def __init__(self, max_per_minute: int):
        self.interval = 60.0 / max_per_minute if max_per_minute > 0 else 0
        self._last_call = 0.0

    async def wait(self):
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self.interval:
            await asyncio.sleep(self.interval - elapsed)
        self._last_call = time.monotonic()


async def check_download_status() -> dict:
    """Check Sonarr queue and history for episodes Babel searched.

    For each pending upgrade_tracking record:
    1. Check Sonarr queue -- is it downloading?
    2. Check Sonarr history -- was it grabbed? imported? failed?

    Returns summary: {"checked": N, "downloading": N, "grabbed": N,
                       "imported": N, "failed": N, "no_results": N}
    """
    cfg = await get_effective_settings()
    db_path = cfg.get("DB_PATH", get_settings().DB_PATH)
    db = await get_db(db_path)

    summary = {
        "checked": 0,
        "downloading": 0,
        "grabbed": 0,
        "imported": 0,
        "failed": 0,
        "no_results": 0,
    }

    try:
        if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
            logger.warning("check_download_status: Sonarr not configured")
            return summary

        sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
        try:
            pending = await models.get_pending_upgrades(db)
            if not pending:
                return summary

            # Build a set of episode IDs we care about
            pending_episode_ids = {r["episode_id"] for r in pending}

            # Fetch queue once and index by episodeId
            queue_items = await sonarr.get_queue()
            queue_by_episode: dict[int, dict] = {}
            for item in queue_items:
                eid = item.get("episodeId")
                if eid and eid in pending_episode_ids:
                    queue_by_episode[eid] = item

            for record in pending:
                episode_id = record["episode_id"]
                summary["checked"] += 1

                # 1. Check queue
                if episode_id in queue_by_episode:
                    qi = queue_by_episode[episode_id]
                    state = qi.get("trackedDownloadState", "").lower()
                    status = qi.get("trackedDownloadStatus", "").lower()
                    if state == "importing" or status == "completed":
                        dl_status = "importing"
                    elif state == "downloading" or status == "ok":
                        dl_status = "downloading"
                        summary["downloading"] += 1
                    else:
                        dl_status = "downloading"
                        summary["downloading"] += 1
                    await models.update_download_status(db, episode_id, dl_status)
                    if dl_status != "importing":
                        continue

                # 2. Check history for this episode
                history = await sonarr.get_history_for_episode(episode_id)
                if not history:
                    await models.update_download_status(db, episode_id, "no_results")
                    summary["no_results"] += 1
                    continue

                # Find the most relevant recent event
                resolved = False
                for event in history:
                    event_type = event.get("eventType", "")
                    if event_type == "downloadFolderImported":
                        await models.update_download_status(db, episode_id, "imported")
                        summary["imported"] += 1
                        resolved = True
                        break
                    elif event_type == "downloadFailed":
                        await models.update_download_status(db, episode_id, "failed")
                        summary["failed"] += 1
                        resolved = True
                        break
                    elif event_type == "grabbed":
                        await models.update_download_status(db, episode_id, "grabbed")
                        summary["grabbed"] += 1
                        resolved = True
                        break

                if not resolved:
                    await models.update_download_status(db, episode_id, "no_results")
                    summary["no_results"] += 1

        finally:
            await sonarr.close()
    finally:
        await db.close()

    logger.info(
        "Download status check: %d checked, %d downloading, %d grabbed, %d imported, %d failed, %d no_results",
        summary["checked"], summary["downloading"], summary["grabbed"],
        summary["imported"], summary["failed"], summary["no_results"],
    )
    return summary


async def resolve_stuck_imports() -> dict:
    """Find and resolve stuck Sonarr imports for Babel-tracked episodes.

    Safeguards:
    - Only acts on episodes Babel searched for (in upgrade_tracking)
    - Only items stuck for 30+ minutes
    - First attempts re-scan, then blocklist+re-search as last resort
    """
    cfg = await get_effective_settings()
    if cfg.get("AUTO_RESOLVE_IMPORTS", "true") == "false":
        return {"checked": 0, "resolved": 0, "skipped": 0}

    db_path = cfg.get("DB_PATH", get_settings().DB_PATH)
    db = await get_db(db_path)
    summary = {"checked": 0, "resolved": 0, "retried": 0, "skipped": 0}

    try:
        if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
            return summary

        sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
        try:
            stuck = await sonarr.get_stuck_imports(min_age_minutes=30)
            if not stuck:
                return summary

            # Get all Babel-tracked episode IDs
            pending = await models.get_pending_upgrades(db)
            tracked_episode_ids = {r["episode_id"] for r in pending}

            for item in stuck:
                summary["checked"] += 1
                ep_id = item.get("episode_id")

                if ep_id not in tracked_episode_ids:
                    summary["skipped"] += 1
                    continue

                # Collect all error messages for this item
                all_messages = " ".join(
                    msg.get("title", "") + " " + " ".join(msg.get("messages", []))
                    for msg in item.get("status_messages", [])
                ).lower()
                qid = item.get("queue_id")

                # Category 1: Already imported / not an upgrade / sample — remove from queue
                is_already_imported = "already imported" in all_messages
                is_not_upgrade = "not a custom format upgrade" in all_messages or "not an upgrade" in all_messages
                is_sample = "sample" in all_messages and len(all_messages) < 30
                is_no_files = "no files found are eligible" in all_messages
                is_executable = "executable file" in all_messages

                if qid and (is_already_imported or is_no_files):
                    # Safe to just remove — already have it or nothing to import
                    await sonarr.remove_from_queue(qid, blocklist=False)
                    summary["resolved"] += 1
                    logger.info("Removed (already imported/empty): %s", item["title"][:80])
                    continue

                if qid and is_not_upgrade:
                    # Sonarr grabbed something worse than what we have — remove, don't blocklist
                    # (the release itself isn't bad, just not better than current)
                    await sonarr.remove_from_queue(qid, blocklist=False)
                    summary["resolved"] += 1
                    logger.info("Removed (not an upgrade): %s", item["title"][:80])
                    continue

                if qid and (is_sample or is_executable):
                    # Bad release — blocklist and re-search
                    await sonarr.remove_from_queue(qid, blocklist=True)
                    if ep_id:
                        await sonarr.search_episodes([ep_id])
                    summary["resolved"] += 1
                    logger.info("Blocklisted (sample/exe) and re-searched: %s", item["title"][:80])
                    continue

                # Category 2: ID mismatch — force manual import
                is_id_mismatch = "matched to series by id" in all_messages or "grab history" in all_messages
                if is_id_mismatch and qid and item.get("series_id"):
                    ep_ids = [ep_id] if ep_id else []
                    success = await sonarr.force_manual_import(
                        qid, item["series_id"], ep_ids
                    )
                    if success:
                        summary["resolved"] += 1
                        logger.info("Force-imported (ID mismatch): %s", item["title"][:80])
                        continue

                # Category 3: Episode number mismatch — try manual import
                is_episode_mismatch = "was unexpected" in all_messages
                if is_episode_mismatch and qid and item.get("series_id"):
                    ep_ids = [ep_id] if ep_id else []
                    success = await sonarr.force_manual_import(
                        qid, item["series_id"], ep_ids
                    )
                    if success:
                        summary["resolved"] += 1
                        logger.info("Force-imported (episode mismatch): %s", item["title"][:80])
                        continue

                # Category 4: Fallback — retry import scan
                if item.get("output_path"):
                    success = await sonarr.retry_import(item["output_path"])
                    if success:
                        summary["retried"] += 1
                        logger.info("Retried import: %s", item["title"][:80])
                        continue

        finally:
            await sonarr.close()
    except Exception:
        logger.warning("Stuck import resolution failed", exc_info=True)
    finally:
        await db.close()

    if summary["checked"] > 0:
        logger.info(
            "Stuck imports: %d checked, %d retried, %d blocklisted+re-searched, %d skipped (not Babel-tracked)",
            summary["checked"], summary["retried"], summary["resolved"], summary["skipped"],
        )
    return summary


async def run_scan() -> dict:
    if _scan_lock.locked():
        return {"status": "skipped", "reason": "scan already in progress"}
    _scan_cancel.clear()
    async with _scan_lock:
        return await _execute_scan()


async def _execute_scan() -> dict:
    cfg = await get_effective_settings()
    db_path = cfg.get("DB_PATH", get_settings().DB_PATH)
    db = await get_db(db_path)
    scan_id = await models.start_scan_log(db)

    episodes_checked = 0
    searches_triggered = 0
    errors = 0
    dubbed_found = 0
    sub_only_found = 0
    missing_found = 0
    skipped_unchanged = 0
    plex_cache_hits = 0
    plex_fresh_lookups = 0
    target_lang = cfg.get("TARGET_LANGUAGE", "eng")

    try:
        # Determine what's available
        sonarr = None
        sonarr_ok = False
        if cfg.get("SONARR_URL") and cfg.get("SONARR_API_KEY"):
            sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
            sonarr_ok, sonarr_msg = await sonarr.test_connection()
            if not sonarr_ok:
                logger.warning("Sonarr unreachable: %s", sonarr_msg)
                await sonarr.close()
                sonarr = None

        global _plex_client_ref
        plex = None
        if cfg.get("PLEX_URL") and cfg.get("PLEX_TOKEN"):
            plex = PlexClient(cfg["PLEX_URL"], cfg["PLEX_TOKEN"])
            _plex_client_ref = plex
            plex_ok, plex_msg = await plex.test_connection()
            if not plex_ok:
                logger.warning("Plex unreachable: %s", plex_msg)
                plex = None
                _plex_client_ref = None

        if not sonarr and not plex:
            msg = "Neither Sonarr nor Plex is reachable. Configure at least one in Settings."
            logger.error(msg)
            await models.complete_scan_log(db, scan_id, 0, 0, 1, "failed", msg)
            return {"status": "failed", "error": msg}

        rate_limiter = RateLimiter(int(cfg.get("SEARCH_RATE_LIMIT", 5)))
        cooldown = timedelta(days=int(cfg.get("SEARCH_COOLDOWN_DAYS", 7)))

        try:
            if sonarr:
                return await _scan_with_sonarr(
                    db, cfg, sonarr, plex, rate_limiter, cooldown, target_lang,
                    scan_id, episodes_checked, searches_triggered, errors,
                    dubbed_found, sub_only_found, missing_found,
                    skipped_unchanged, plex_cache_hits, plex_fresh_lookups,
                )
            else:
                return await _scan_plex_only(
                    db, cfg, plex, target_lang, scan_id,
                )
        finally:
            if sonarr:
                await sonarr.close()
            if plex:
                await plex.close()

    except Exception as e:
        logger.exception("Scan failed: %s", e)
        await models.complete_scan_log(
            db, scan_id, episodes_checked, searches_triggered, errors, "failed", str(e)
        )
        return {"status": "failed", "error": str(e)}
    finally:
        _plex_client_ref = None
        await db.close()


async def _load_ignore_patterns(db) -> list[str]:
    """Load all ignored path patterns from DB."""
    rows = await models.get_ignored_paths(db)
    return [r["pattern"].lower() for r in rows]


def _is_ignored(path: str, patterns: list[str]) -> bool:
    """Check if a path matches any ignored pattern (case-insensitive substring)."""
    if not path or not patterns:
        return False
    path_lower = path.lower()
    return any(p in path_lower for p in patterns)


async def _scan_plex_only(db, cfg, plex, target_lang, scan_id) -> dict:
    """Scan using Plex as the sole data source (no Sonarr)."""
    logger.info("=" * 60)
    logger.info("SCAN STARTED — Plex-only mode (Sonarr unavailable)")
    logger.info("Target language: %s", target_lang)
    logger.info("=" * 60)

    ignore_patterns = await _load_ignore_patterns(db)
    if ignore_patterns:
        logger.info("Ignore patterns: %s", ignore_patterns)

    logger.info("Scanning Plex library for all shows and audio tracks...")

    plex_series = await plex.get_library_data(target_lang)

    # Filter out ignored paths
    if ignore_patterns:
        before = len(plex_series)
        plex_series = [s for s in plex_series if not _is_ignored(s.get("path", ""), ignore_patterns)]
        skipped = before - len(plex_series)
        if skipped:
            logger.info("Skipped %d shows matching ignore patterns", skipped)

    total_series = len(plex_series)
    logger.info("Found %d shows with episodes in Plex", total_series)

    episodes_checked = 0
    dubbed_found = 0
    sub_only_found = 0
    errors = 0
    plex_series_ids = set()

    for i, series in enumerate(plex_series, 1):
        if _scan_cancel.is_set():
            logger.info("Scan cancelled at series %d/%d", i, total_series)
            break

        # Use plex_key as a stable numeric ID (offset to avoid collision with Sonarr IDs)
        series_id = -(abs(series["plex_key"]) + 1000000)
        plex_series_ids.add(series_id)
        await models.upsert_series(db, series_id, series["title"], series.get("path"), poster_url=series.get("poster_url"))

        series_dubbed = 0
        series_sub = 0
        series_unknown = 0
        episode_ids = set()

        for ep in series["episodes"]:
            # Generate a stable episode ID using hash to avoid collisions
            ep_id = -(abs(hash((series["plex_key"], ep["season"], ep["episode"]))) % 2_000_000_000)
            episode_ids.add(ep_id)

            await models.upsert_episode(
                db, ep_id, series_id,
                ep["season"], ep["episode"],
                ep["title"], ep["file_path"], ep["file_size"],
            )

            # Store audio tracks
            if ep["audio_tracks"]:
                await models.replace_audio_tracks(db, ep_id, ep["audio_tracks"])

            status = ep["dub_status"]
            await models.update_episode_status(db, ep_id, status)
            episodes_checked += 1

            if status == "DUBBED":
                series_dubbed += 1
                dubbed_found += 1
            elif status == "SUB_ONLY":
                series_sub += 1
                sub_only_found += 1
            else:
                series_unknown += 1
                errors += 1

        await models.delete_episodes_not_in(db, series_id, episode_ids)
        await models.update_series_counts(db, series_id)

        total_eps = len(series["episodes"])
        if total_eps > 0:
            dub_pct = int(series_dubbed / total_eps * 100)
            status_icon = "✅" if series_dubbed == total_eps else ("🟡" if series_dubbed > 0 else "🔴")
            logger.info(
                "[%d/%d] %s %s — %d eps (%d dubbed, %d sub-only) %d%% dubbed",
                i, total_series, status_icon, series["title"],
                total_eps, series_dubbed, series_sub, dub_pct,
            )

    await models.delete_series_not_in(db, plex_series_ids)
    await models.complete_scan_log(
        db, scan_id, episodes_checked, 0, errors, "completed"
    )

    logger.info("=" * 60)
    logger.info("SCAN COMPLETE (Plex-only)")
    logger.info("  Shows found:      %d", total_series)
    logger.info("  Episodes checked: %d", episodes_checked)
    logger.info("  Dubbed:           %d", dubbed_found)
    logger.info("  Sub-only:         %d", sub_only_found)
    logger.info("  Note: Sonarr searches disabled (Sonarr not connected)")
    logger.info("=" * 60)

    # Plex collection management
    if cfg.get("AUTO_COLLECTIONS_PLEX", "true") != "false":
        try:
            all_series_data = await models.get_all_series(db)
            coll_result = await plex.sync_collections(all_series_data)
            logger.info("Plex collections synced: %d shows updated", coll_result["collections_updated"])
        except Exception:
            logger.warning("Plex collection sync failed", exc_info=True)

    # Discord notifications
    webhook_url = cfg.get("DISCORD_WEBHOOK_URL", "")
    if webhook_url:
        from src.notifications import notify_scan_complete, notify_upgrades
        return_stats = {
            "episodes_checked": episodes_checked,
            "dubbed": dubbed_found,
            "sub_only": sub_only_found,
            "searches_triggered": 0,
        }
        await notify_scan_complete(webhook_url, return_stats)

    return {
        "status": "completed",
        "mode": "plex_only",
        "episodes_checked": episodes_checked,
        "searches_triggered": 0,
        "dubbed": dubbed_found,
        "sub_only": sub_only_found,
        "errors": errors,
    }


async def _scan_with_sonarr(
    db, cfg, sonarr, plex, rate_limiter, cooldown, target_lang,
    scan_id, episodes_checked, searches_triggered, errors,
    dubbed_found, sub_only_found, missing_found,
    skipped_unchanged, plex_cache_hits, plex_fresh_lookups,
) -> dict:
    """Full scan using Sonarr as primary data source + Plex/ffprobe for audio."""
    upgrades_succeeded = 0
    upgrades_failed = 0
    successful_upgrades = []

    anime_series = await sonarr.get_anime_series(cfg.get("ANIME_FILTER", "type"))

    # Filter ignored paths
    ignore_patterns = await _load_ignore_patterns(db)
    if ignore_patterns:
        before = len(anime_series)
        anime_series = [s for s in anime_series if not _is_ignored(s.get("path", ""), ignore_patterns)]
        skipped = before - len(anime_series)
        if skipped:
            logger.info("Skipped %d series matching ignore patterns", skipped)

    total_series = len(anime_series)
    logger.info("=" * 60)
    logger.info("SCAN STARTED — %d anime series to process", total_series)
    logger.info("Target language: %s | Cooldown: %d days | Rate limit: %s/min",
                target_lang, int(cfg.get("SEARCH_COOLDOWN_DAYS", 7)),
                cfg.get("SEARCH_RATE_LIMIT", 5))
    logger.info("=" * 60)

    if plex:
        logger.info("Building Plex audio track index (this may take a minute)...")
        _scan_progress.update(phase="indexing_plex", last_log="Building Plex audio track index...")
        plex_count = await plex.build_index(ignored_patterns=ignore_patterns)
        logger.info("Plex index ready: %d episode files indexed", plex_count)
        _scan_progress["last_log"] = f"Plex index ready: {plex_count} files"
        samples = plex.get_sample_paths(3)
        if samples:
            logger.info("Sample Plex paths: %s", samples)
    else:
        logger.info("Plex not configured — will use ffprobe only")

    sonarr_series_ids = set()

    for i, series in enumerate(anime_series, 1):
        if _scan_cancel.is_set():
            logger.info("Scan cancelled at series %d/%d", i, total_series)
            break

        sonarr_series_ids.add(series["id"])
        _scan_progress.update(
            phase="scanning",
            current_series=series["title"],
            series_index=i,
            series_total=total_series,
        )
        await models.upsert_series(db, series["id"], series["title"], series["path"], poster_url=series.get("poster_url"))

        series_excluded = await models.is_series_excluded(db, series["id"])

        # Quick check: get episode files first — if none, skip the full episode fetch
        episode_files = await sonarr.get_episode_files(series["id"])
        if not episode_files:
            # No files at all — mark as empty, skip episode-level processing
            existing_series = await models.get_series(db, series["id"])
            if existing_series and existing_series["dub_status"] == "EMPTY":
                skipped_unchanged += existing_series.get("total_episodes", 0)
            await models.update_series_counts(db, series["id"])
            continue

        episodes = await sonarr.get_episodes(series["id"])
        file_map = {ef["id"]: ef for ef in episode_files}

        series_dubbed = 0
        series_sub = 0
        series_missing = 0
        series_unknown = 0
        series_searched = 0
        series_skipped = 0
        sonarr_episode_ids = set()
        sub_only_to_search = []
        failed_retry_ids = []

        for ep in episodes:
            sonarr_episode_ids.add(ep["id"])
            file_info = file_map.get(ep["episodeFileId"]) if ep["hasFile"] else None
            file_path = file_info["path"] if file_info else None
            file_size = file_info["size"] if file_info else None

            existing_ep = await models.get_episode(db, ep["id"])
            await models.upsert_episode(
                db, ep["id"], series["id"],
                ep["seasonNumber"], ep["episodeNumber"],
                ep["title"], file_path, file_size,
            )

            if not file_path:
                await models.update_episode_status(db, ep["id"], "MISSING")
                series_missing += 1
                missing_found += 1
                continue

            # Skip unchanged episodes
            if (existing_ep
                    and existing_ep["file_size"] == file_size
                    and existing_ep["file_path"] == file_path
                    and existing_ep["dub_status"] not in ("UNKNOWN", "MISSING", None)):
                status = existing_ep["dub_status"]
                skipped_unchanged += 1
                series_skipped += 1
                if status == "DUBBED":
                    series_dubbed += 1
                    dubbed_found += 1
                elif status == "SUB_ONLY":
                    series_sub += 1
                    sub_only_found += 1
                    if not series_excluded:
                        last_search = await models.get_last_search_time(db, ep["id"])
                        now = datetime.now(timezone.utc)
                        if last_search is None or (now - last_search) > cooldown:
                            sub_only_to_search.append(ep["id"])
                continue

            # Detect if file changed (potential upgrade)
            file_changed = (
                existing_ep
                and existing_ep["file_size"] is not None
                and existing_ep["file_size"] != file_size
                and existing_ep["dub_status"] == "SUB_ONLY"
            )

            episodes_checked += 1
            tracks = None

            # File changed — MUST re-check audio (don't use cache)
            if file_changed:
                if plex:
                    plex_path = _translate_path(file_path, "plex", cfg)
                    tracks = await plex.get_audio_tracks(plex_path)
                    if tracks is not None:
                        plex_fresh_lookups += 1
                        for t in tracks:
                            t["language"] = normalize_language(t["language"])
                            t["source"] = "plex"
                if tracks is None:
                    local_path = _translate_path(file_path, "local", cfg)
                    tracks = await ffprobe.get_audio_tracks(local_path)
                    if tracks is not None:
                        plex_fresh_lookups += 1
                        for t in tracks:
                            t["source"] = "ffprobe"
            else:
                # Normal flow: check cache first, then Plex, then ffprobe
                tracks_from_cache = False
                if (existing_ep
                        and existing_ep["file_size"] == file_size
                        and existing_ep["file_path"] == file_path):
                    cached_tracks = await models.get_audio_tracks(db, ep["id"])
                    if cached_tracks:
                        tracks = cached_tracks
                        tracks_from_cache = True
                        plex_cache_hits += 1

                if tracks is None and plex:
                    plex_path = _translate_path(file_path, "plex", cfg)
                    tracks = await plex.get_audio_tracks(plex_path)
                    if tracks is not None:
                        plex_fresh_lookups += 1
                        for t in tracks:
                            t["language"] = normalize_language(t["language"])
                            t["source"] = "plex"

                if tracks is None:
                    local_path = _translate_path(file_path, "local", cfg)
                    tracks = await ffprobe.get_audio_tracks(local_path)
                    if tracks is not None:
                        plex_fresh_lookups += 1
                        for t in tracks:
                            t["source"] = "ffprobe"

            # Classify
            if tracks is not None and len(tracks) > 0:
                if not (not file_changed and locals().get("tracks_from_cache")):
                    await models.replace_audio_tracks(db, ep["id"], tracks)
                languages = {t["language"] for t in tracks}
                if target_lang in languages:
                    status = "DUBBED"
                    series_dubbed += 1
                    dubbed_found += 1
                else:
                    status = "SUB_ONLY"
                    series_sub += 1
                    sub_only_found += 1
            elif tracks is not None:
                status = "UNKNOWN"
                series_unknown += 1
                errors += 1
            else:
                status = "UNKNOWN"
                series_unknown += 1
                errors += 1

            await models.update_episode_status(db, ep["id"], status)

            # Upgrade tracking: resolve pending upgrades when file changes
            if file_changed:
                if status == "DUBBED":
                    await models.resolve_upgrade(
                        db, ep["id"], file_size, status, "success"
                    )
                    upgrades_succeeded += 1
                    successful_upgrades.append({
                        "series_title": series["title"],
                        "season": ep["seasonNumber"] or 0,
                        "episode": ep["episodeNumber"] or 0,
                        "poster_url": series.get("poster_url"),
                    })
                    logger.info(
                        "  ✅ Upgrade SUCCESS: %s S%02dE%02d — now dubbed!",
                        series["title"], ep["seasonNumber"] or 0, ep["episodeNumber"] or 0,
                    )
                else:
                    await models.resolve_upgrade(
                        db, ep["id"], file_size, status, "failed"
                    )
                    upgrades_failed += 1
                    # Failed upgrade — force re-search (bypass cooldown)
                    failed_retry_ids.append(ep["id"])
                    logger.warning(
                        "  ❌ Upgrade FAILED: %s S%02dE%02d — new file still %s, will retry",
                        series["title"], ep["seasonNumber"] or 0, ep["episodeNumber"] or 0, status,
                    )

            # Collect sub-only for batch search (normal cooldown)
            if status == "SUB_ONLY" and ep["id"] not in failed_retry_ids and not series_excluded:
                last_search = await models.get_last_search_time(db, ep["id"])
                now = datetime.now(timezone.utc)
                if last_search is None or (now - last_search) > cooldown:
                    sub_only_to_search.append(ep["id"])

        # Filter out episodes that have exceeded max search attempts
        max_search_attempts = int(cfg.get("MAX_SEARCH_ATTEMPTS", 3))
        if max_search_attempts > 0 and sub_only_to_search:
            filtered = []
            for eid in sub_only_to_search:
                search_count = await models.get_search_count(db, eid)
                if search_count >= max_search_attempts:
                    # Only skip if the latest upgrade record shows no_results
                    upgrade_rows = await db.execute(
                        "SELECT download_status FROM upgrade_tracking WHERE episode_id = ? ORDER BY triggered_at DESC LIMIT 1",
                        (eid,),
                    )
                    row = await upgrade_rows.fetchone()
                    if row and row["download_status"] == "no_results":
                        continue
                filtered.append(eid)
            skipped_max = len(sub_only_to_search) - len(filtered)
            if skipped_max > 0:
                logger.info("  Skipping %d episodes (max search attempts reached)", skipped_max)
            sub_only_to_search = filtered

        # Batch Sonarr search — normal cooldown-based
        all_to_search = sub_only_to_search + failed_retry_ids
        if all_to_search:
            await rate_limiter.wait()
            success = await sonarr.search_episodes(all_to_search)
            if success:
                for eid in all_to_search:
                    await models.add_search_record(db, eid)
                    # Create upgrade tracking record
                    ep_data = await models.get_episode(db, eid)
                    if ep_data:
                        await models.create_upgrade_record(
                            db, eid, series["title"],
                            ep_data.get("season_number", 0),
                            ep_data.get("episode_number", 0),
                            ep_data.get("file_size", 0),
                        )
                searches_triggered += len(all_to_search)
                series_searched = len(all_to_search)
                if failed_retry_ids:
                    logger.info(
                        "  🔄 Retry search: %s — %d failed upgrades re-queued",
                        series["title"], len(failed_retry_ids),
                    )
                if sub_only_to_search:
                    logger.info(
                        "  🔍 Batch search: %s — %d sub-only episodes",
                        series["title"], len(sub_only_to_search),
                    )

        await models.delete_episodes_not_in(db, series["id"], sonarr_episode_ids)
        await models.update_series_counts(db, series["id"])

        total_eps = len(episodes)
        with_files = total_eps - series_missing
        if with_files > 0:
            dub_pct = int(series_dubbed / with_files * 100) if with_files else 0
            status_icon = "✅" if series_dubbed == with_files else ("🟡" if series_dubbed > 0 else "🔴")
            skip_note = f" | {series_skipped} cached" if series_skipped else ""
            log_line = f"[{i}/{total_series}] {status_icon} {series['title']} — {with_files} eps ({series_dubbed} dubbed, {series_sub} sub-only) {dub_pct}% dubbed"
            logger.info(
                "[%d/%d] %s %s — %d eps (%d dubbed, %d sub-only, %d unknown) %d%% dubbed%s%s",
                i, total_series, status_icon, series["title"],
                with_files, series_dubbed, series_sub, series_unknown, dub_pct,
                f" | {series_searched} searches" if series_searched else "",
                skip_note,
            )
        else:
            log_line = f"[{i}/{total_series}] ⚪ {series['title']} — no downloaded episodes"
            logger.info("[%d/%d] ⚪ %s — no downloaded episodes", i, total_series, series["title"])

        _scan_progress.update(
            episodes_checked=episodes_checked,
            dubbed_found=dubbed_found,
            sub_only_found=sub_only_found,
            searches_triggered=searches_triggered,
            last_log=log_line,
        )

    cancelled = _scan_cancel.is_set()
    if not cancelled:
        await models.delete_series_not_in(db, sonarr_series_ids)

    status_label = "cancelled" if cancelled else "completed"
    await models.complete_scan_log(
        db, scan_id, episodes_checked, searches_triggered, errors, status_label
    )

    _scan_progress.update(phase="idle", current_series="", last_log="")

    logger.info("=" * 60)
    logger.info("SCAN %s", "CANCELLED" if cancelled else "COMPLETE")
    logger.info("  Series processed:   %d", total_series)
    logger.info("  Episodes checked:   %d", episodes_checked)
    logger.info("  Skipped (cached):   %d", skipped_unchanged)
    logger.info("  Dubbed:             %d", dubbed_found)
    logger.info("  Sub-only:           %d", sub_only_found)
    logger.info("  Missing files:      %d", missing_found)
    logger.info("  Searches triggered: %d", searches_triggered)
    logger.info("  Plex cache hits:    %d", plex_cache_hits)
    logger.info("  Fresh lookups:      %d", plex_fresh_lookups)
    if upgrades_succeeded or upgrades_failed:
        logger.info("  Upgrades succeeded: %d", upgrades_succeeded)
        logger.info("  Upgrades failed:    %d (re-queued)", upgrades_failed)
    logger.info("  Errors:             %d", errors)
    logger.info("=" * 60)

    # Auto-check download status for pending upgrades
    try:
        dl_summary = await check_download_status()
        if dl_summary["checked"] > 0:
            logger.info("Download status: %d downloading, %d grabbed, %d imported, %d failed, %d no results",
                         dl_summary["downloading"], dl_summary["grabbed"], dl_summary["imported"],
                         dl_summary["failed"], dl_summary["no_results"])
    except Exception:
        logger.warning("Auto download check failed", exc_info=True)

    # Auto-resolve stuck imports
    try:
        import_summary = await resolve_stuck_imports()
        if import_summary["checked"] > 0:
            logger.info("Stuck imports resolved: %d retried, %d blocklisted",
                         import_summary["retried"], import_summary["resolved"])
    except Exception:
        logger.warning("Stuck import resolution failed", exc_info=True)

    # Auto-tag series in Sonarr
    if cfg.get("AUTO_TAG_SONARR", "true") != "false":
        try:
            statuses = []
            for sid in sonarr_series_ids:
                s = await models.get_series(db, sid)
                if s and s["dub_status"] in ("DUBBED", "PARTIAL", "SUB_ONLY"):
                    statuses.append({"sonarr_id": sid, "dub_status": s["dub_status"]})
            if statuses:
                tag_result = await sonarr.sync_dub_tags(statuses)
                logger.info("Sonarr tags synced: %d tagged, %d errors", tag_result["tagged"], tag_result["errors"])
        except Exception:
            logger.warning("Sonarr tag sync failed", exc_info=True)

    # Plex collection management
    if plex and cfg.get("AUTO_COLLECTIONS_PLEX", "true") != "false":
        try:
            all_series_data = await models.get_all_series(db)
            coll_result = await plex.sync_collections(all_series_data)
            logger.info("Plex collections synced: %d shows updated", coll_result["collections_updated"])
        except Exception:
            logger.warning("Plex collection sync failed", exc_info=True)

    # DB cleanup — prune old records
    try:
        cleaned = await models.cleanup_old_records(db, days=30)
        total_cleaned = sum(cleaned.values())
        if total_cleaned > 0:
            logger.info("DB cleanup: removed %d old records", total_cleaned)
    except Exception:
        logger.warning("DB cleanup failed", exc_info=True)

    # Discord notifications
    webhook_url = cfg.get("DISCORD_WEBHOOK_URL", "")
    if webhook_url:
        from src.notifications import notify_scan_complete, notify_upgrades
        return_stats = {
            "episodes_checked": episodes_checked,
            "dubbed": dubbed_found,
            "sub_only": sub_only_found,
            "searches_triggered": searches_triggered,
            "upgrades_succeeded": upgrades_succeeded,
            "upgrades_failed": upgrades_failed,
        }
        await notify_scan_complete(webhook_url, return_stats)
        await notify_upgrades(webhook_url, successful_upgrades)

    return {
        "status": "completed",
        "episodes_checked": episodes_checked,
        "skipped_unchanged": skipped_unchanged,
        "searches_triggered": searches_triggered,
        "dubbed": dubbed_found,
        "sub_only": sub_only_found,
        "upgrades_succeeded": upgrades_succeeded,
        "upgrades_failed": upgrades_failed,
        "errors": errors,
    }
