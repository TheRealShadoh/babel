"""
Babel web routes — FastAPI router for dashboard pages and API endpoints.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from markupsafe import escape as esc

from src.config import get_settings, get_effective_settings, translate_path, cfg_bool
from src.db.database import get_db
from src.db import models
from src.scanner.engine import run_scan, check_download_status, resolve_stuck_imports
from src.scanner.sonarr import SonarrClient
from src.web.auth import hash_password
from src.scanner.plex import PlexClient
from src.scheduler import get_next_run_time

logger = logging.getLogger(__name__)

router = APIRouter()

# asyncio keeps only a weak reference to a running task, so a fire-and-forget
# create_task() whose handle is dropped can be collected mid-flight. Anything
# started from a request holds a strong reference here until it finishes.
_background_tasks: set[asyncio.Task] = set()


def _spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


# Rendered into the settings form in place of a stored secret. Submitting the
# field unchanged (i.e. empty) leaves the stored value alone, so the API key
# and Plex token never travel back to the browser in the page source.
SECRET_KEYS = frozenset({"SONARR_API_KEY", "PLEX_TOKEN", "JELLYFIN_API_KEY", "WEBHOOK_SECRET", "AUTH_PASSWORD"})


def _to_utc_display(value: str | None) -> str | None:
    """Normalise a timestamp to naive UTC 'YYYY-MM-DD HH:MM:SS'.

    Everything the DB stores is naive UTC, but APScheduler hands back an
    offset-aware local time. Rendering the two side by side made a six-hour
    gap look like two, so both go through here and the browser converts to
    local time from a single known basis.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return value
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SETTING_KEYS = (
    "SONARR_URL",
    "SONARR_API_KEY",
    "PLEX_URL",
    "PLEX_TOKEN",
    "JELLYFIN_URL",
    "JELLYFIN_API_KEY",
    "MEDIA_SERVER",
    "SCAN_INTERVAL_HOURS",
    "TARGET_LANGUAGE",
    "SEARCH_COOLDOWN_DAYS",
    "SEARCH_RATE_LIMIT",
    "SONARR_PATH_PREFIX",
    "LOCAL_PATH_PREFIX",
    "PLEX_PATH_PREFIX",
    "JELLYFIN_PATH_PREFIX",
    "ANIME_FILTER",
    "LOG_LEVEL",
    "SHOW_THUMBNAILS",
    "MAX_SEARCH_ATTEMPTS",
    "AUTO_TAG_SONARR",
    "DISCORD_WEBHOOK_URL",
    "AUTO_COLLECTIONS_PLEX",
    "AUTO_RESOLVE_IMPORTS",
    "AUTO_MONITOR_DUBS",
    "STUCK_IMPORT_DRY_RUN",
    "WEBHOOK_SECRET",
    "AUTH_USERNAME",
    "AUTH_PASSWORD",
)


def _templates(request: Request):
    """Shortcut to the Jinja2Templates instance stored on the app."""
    return request.app.state.templates


def _build_sonarr_url(base_url: str, series: dict) -> str | None:
    """Build a Sonarr series page URL.

    Prefers Sonarr's own titleSlug (captured at scan time); older rows
    scanned before that column existed fall back to a folder-name guess.
    """
    if not base_url or series.get("id", 0) <= 0:
        return None
    slug = series.get("title_slug")
    if not slug:
        if not series.get("sonarr_path"):
            return None
        import re
        folder = series["sonarr_path"].rstrip("/").split("/")[-1]
        slug = re.sub(r"[^a-z0-9]+", "-", folder.lower()).strip("-")
    return f"{base_url.rstrip('/')}/series/{slug}"


# ---------------------------------------------------------------------------
# Dashboard pages
# ---------------------------------------------------------------------------


@router.get("/")
async def overview(request: Request):
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        stats = await models.get_overview_stats(db)
        upgrade_stats = await models.get_upgrade_stats(db)
    finally:
        await db.close()

    # Both timestamps are normalised to UTC here and converted to the
    # viewer's local time in the browser (see the [data-timestamp] hook).
    next_scan = _to_utc_display(get_next_run_time())

    return _templates(request).TemplateResponse(
        request,
        "overview.html",
        {
            "stats": stats,
            "next_scan_time": next_scan,
            "upgrade_stats": upgrade_stats,
        },
    )


@router.get("/series")
async def series_list(request: Request):
    import math

    settings = get_settings()
    db = await get_db(settings.DB_PATH)

    filter_status = request.query_params.get("status") or None
    search_query = request.query_params.get("q") or None
    sort_by = request.query_params.get("sort") or None
    try:
        page = max(1, int(request.query_params.get("page", 1)))
    except (ValueError, TypeError):
        page = 1

    per_page = 30

    try:
        # Count first so an out-of-range ?page= is clamped before the query
        # runs. Clamping afterwards produced "Page 34 of 34" with no rows on
        # it, because the offset had already overshot.
        _, total_count = await models.get_series_filtered(
            db, status=filter_status, search=search_query,
            page=1, per_page=0, sort=sort_by,
        )
        total_pages = max(1, math.ceil(total_count / per_page))
        page = min(page, total_pages)

        all_series, _ = await models.get_series_filtered(
            db,
            status=filter_status,
            search=search_query,
            page=page,
            per_page=per_page,
            sort=sort_by,
        )
        show_thumbs_val = await models.get_setting(db, "SHOW_THUMBNAILS")
    finally:
        await db.close()
    show_thumbnails = cfg_bool(show_thumbs_val)

    return _templates(request).TemplateResponse(
        request,
        "series_list.html",
        {
            "series": all_series,
            "filter_status": filter_status,
            "search_query": search_query or "",
            "sort_by": sort_by or "",
            "page": page,
            "total_pages": total_pages,
            "total_count": total_count,
            "show_thumbnails": show_thumbnails,
        },
    )


@router.get("/series/{series_id}")
async def series_detail(request: Request, series_id: int):
    cfg = await get_effective_settings()
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        series = await models.get_series(db, series_id)
        if series is None:
            return HTMLResponse(
                f"<h2>Series {series_id} not found</h2>", status_code=404
            )

        episodes = await models.get_episodes_for_series(db, series_id)
        excluded = await models.is_series_excluded(db, series_id)

        # Enrich each episode with audio tracks and last search time (batch)
        all_tracks = await models.get_audio_tracks_for_series(db, series_id)
        all_searches = await models.get_last_search_times_for_series(db, series_id)
        for ep in episodes:
            ep["audio_tracks"] = all_tracks.get(ep["id"], [])
            ep["last_search_time"] = all_searches.get(ep["id"])
    finally:
        await db.close()

    return _templates(request).TemplateResponse(
        request,
        "series_detail.html",
        {"series": series, "episodes": episodes, "excluded": excluded,
         "sonarr_url": _build_sonarr_url(cfg.get("SONARR_URL", ""), series)},
    )


@router.get("/history")
async def history(request: Request):
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        scan_logs = await models.get_scan_logs(db)
        search_history = await models.get_search_history(db)
        upgrade_history = await models.get_upgrade_history(db, limit=50)
        upgrade_stats = await models.get_upgrade_stats(db)
    finally:
        await db.close()

    from src.scanner.engine import is_scan_running, _scan_cancel
    scan_stopping = is_scan_running() and _scan_cancel.is_set()

    return _templates(request).TemplateResponse(
        request,
        "history.html",
        {
            "scan_logs": scan_logs,
            "search_history": search_history,
            "upgrade_history": upgrade_history,
            "upgrade_stats": upgrade_stats,
            "scan_stopping": scan_stopping,
        },
    )


@router.get("/history/{scan_id}")
async def scan_detail(request: Request, scan_id: int):
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        detail = await models.get_scan_detail(db, scan_id)
    finally:
        await db.close()

    if detail is None:
        return HTMLResponse(
            f"<h2>Scan #{scan_id} not found</h2>", status_code=404
        )

    return _templates(request).TemplateResponse(
        request,
        "scan_detail.html",
        {
            "scan": detail["scan"],
            "series_summary": detail["series_summary"],
            "searches": detail["searches"],
        },
    )


@router.get("/settings")
async def settings_page(request: Request, saved: int = 0):
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        db_settings = await models.get_all_settings(db)
        ignored_paths = await models.get_ignored_paths(db)
    finally:
        await db.close()

    # Start with env defaults, overlay any DB overrides
    current = {}
    secret_set = {}
    for key in SETTING_KEYS:
        env_val = getattr(settings, key, "")
        value = db_settings.get(key, str(env_val))
        if key in SECRET_KEYS:
            # Never render a stored secret into the page. The form submits an
            # empty field to mean "leave it as it is".
            secret_set[key] = bool(value) or bool(
                key == "AUTH_PASSWORD" and db_settings.get("AUTH_PASSWORD_HASH")
            )
            value = ""
        current[key] = value

    flash_message = None
    if saved == 1:
        flash_message = "Settings saved."
    elif saved == 2:
        flash_message = "Could not save settings — check the server logs."

    auth_on = bool(
        (current.get("AUTH_USERNAME") or settings.AUTH_USERNAME)
        and (secret_set.get("AUTH_PASSWORD") or settings.AUTH_PASSWORD)
    )

    return _templates(request).TemplateResponse(
        request,
        "settings.html",
        {
            "settings": current,
            "secret_set": secret_set,
            "flash_message": flash_message,
            "flash_error": saved == 2,
            "auth_enabled": auth_on,
            "ignored_paths": ignored_paths,
        },
    )


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


@router.post("/api/scan")
async def trigger_scan(request: Request):
    from src.scanner.engine import reserve_scan, release_scan_reservation
    # Reserved synchronously: two clicks landing in the same event-loop tick
    # would both see an unlocked scan lock and both schedule a scan, because
    # the first task has not started running yet.
    if not reserve_scan():
        return HTMLResponse(
            '<div class="flash" style="background-color:rgba(245,158,11,0.15);border:1px solid #f59e0b;padding:0.75rem 1rem;border-radius:6px;color:#f59e0b;">'
            'A scan is already running.</div>'
        )
    try:
        _spawn(run_scan())
    except Exception:
        release_scan_reservation()
        raise
    logger.info("Scan triggered via web UI")

    return HTMLResponse(
        '<div class="flash" style="background-color:rgba(9,132,227,0.15);border:1px solid #0984e3;padding:0.75rem 1rem;border-radius:6px;color:#0984e3;">'
        'Scan started. Check history for results.</div>'
    )


@router.post("/api/scan/stop")
async def stop_scan(request: Request):
    from src.scanner.engine import is_scan_running, request_scan_cancel
    if not is_scan_running():
        return HTMLResponse(
            '<div class="flash" style="background-color:rgba(100,116,139,0.15);border:1px solid #64748b;padding:0.75rem 1rem;border-radius:6px;color:#64748b;">'
            'No scan is running.</div>'
        )
    request_scan_cancel()
    logger.info("Scan stop requested via web UI")
    return HTMLResponse(
        '<div class="flash" style="background-color:rgba(245,158,11,0.15);border:1px solid #f59e0b;padding:0.75rem 1rem;border-radius:6px;color:#f59e0b;">'
        'Stop requested. Scan will finish the current series and stop.</div>'
    )


@router.post("/api/check-downloads")
async def check_downloads(request: Request):
    """Trigger a download status check (faster than full rescan)."""
    try:
        summary = await check_download_status()
    except Exception as e:
        logger.exception("Download status check failed: %s", e)
        return HTMLResponse(
            '<span style="color:var(--red);">Error: Download status check failed. Check server logs.</span>'
        )

    if summary["checked"] == 0:
        return HTMLResponse(
            '<span style="color:var(--text-muted);font-size:0.82rem;">No pending upgrades to check.</span>'
        )

    parts = []
    parts.append(f'{summary["checked"]} checked')
    if summary["downloading"]:
        parts.append(f'<span style="color:var(--blue);">{summary["downloading"]} downloading</span>')
    if summary["grabbed"]:
        parts.append(f'<span style="color:var(--yellow);">{summary["grabbed"]} grabbed</span>')
    if summary["imported"]:
        parts.append(f'<span style="color:var(--green);">{summary["imported"]} imported</span>')
    if summary["failed"]:
        parts.append(f'<span style="color:var(--red);">{summary["failed"]} failed</span>')
    if summary["no_results"]:
        parts.append(f'{summary["no_results"]} no results')

    html = (
        '<div style="font-size:0.82rem;font-family:\'JetBrains Mono\',monospace;'
        'display:flex;gap:0.6rem;flex-wrap:wrap;align-items:center;">'
        + " &middot; ".join(parts)
        + '</div>'
    )
    return HTMLResponse(html)


@router.post("/api/resolve-imports")
async def resolve_imports(request: Request):
    """Attempt to resolve stuck Sonarr imports for Babel-tracked episodes."""
    try:
        summary = await resolve_stuck_imports()
    except Exception as e:
        logger.exception("Stuck import resolution failed: %s", e)
        return HTMLResponse('<span style="color:var(--red);">Error: Import resolution failed. Check server logs.</span>')

    if summary["checked"] == 0:
        return HTMLResponse(
            '<span style="color:var(--text-muted);font-size:0.82rem;">No stuck imports found.</span>'
        )

    parts = [f'{summary["checked"]} stuck']
    if summary["resolved"]:
        parts.append(f'<span style="color:var(--green);">{summary["resolved"]} force-imported</span>')
    if summary["retried"]:
        parts.append(f'<span style="color:var(--blue);">{summary["retried"]} retried</span>')
    if summary["skipped"]:
        parts.append(f'{summary["skipped"]} not Babel-tracked')

    html = (
        '<div style="font-size:0.82rem;font-family:\'JetBrains Mono\',monospace;'
        'display:flex;gap:0.6rem;flex-wrap:wrap;align-items:center;">'
        + " &middot; ".join(parts) + '</div>'
    )
    return HTMLResponse(html)


@router.post("/api/search/{episode_id}")
async def search_episode(request: Request, episode_id: int):
    cfg = await get_effective_settings()
    if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
        return _templates(request).TemplateResponse(
            request,
            "partials/episode_row.html",
            {"episode_id": episode_id, "success": False, "message": "Sonarr is not configured."},
        )
    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    db = await get_db(cfg.get("DB_PATH", get_settings().DB_PATH))
    try:
        success = await sonarr.search_episodes([episode_id])
        if success:
            await models.add_search_record(db, episode_id, trigger_source="manual")
            message = "Search triggered successfully."
        else:
            message = "Failed to trigger search in Sonarr."
    except Exception as e:
        logger.exception("Manual search failed for episode %d: %s", episode_id, e)
        message = "Error: Search failed. Check server logs."
        success = False
    finally:
        await sonarr.close()
        await db.close()

    return _templates(request).TemplateResponse(
        request,
        "partials/episode_row.html",
        {"episode_id": episode_id, "success": success, "message": message},
    )


@router.post("/api/search-all/{series_id}")
async def search_all_sub_only(request: Request, series_id: int):
    """Search all sub-only episodes for a series.

    Sonarr's EpisodeSearch command accepts a batch of episode IDs and searches
    them all in one call, so there's no need to loop with per-episode delays
    (that previously held the HTTP request open for minutes on large series).
    """
    cfg = await get_effective_settings()
    if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
        return HTMLResponse('<span style="color:var(--red);">Sonarr is not configured.</span>')

    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    db = await get_db(cfg.get("DB_PATH", get_settings().DB_PATH))
    triggered = 0
    errors = 0
    try:
        episodes = await models.get_episodes_for_series(db, series_id)
        sub_only_ids = [ep["id"] for ep in episodes if ep["dub_status"] == "SUB_ONLY"]

        if sub_only_ids:
            try:
                success = await sonarr.search_episodes(sub_only_ids)
                if success:
                    for ep_id in sub_only_ids:
                        await models.add_search_record(db, ep_id, trigger_source="manual_bulk")
                    triggered = len(sub_only_ids)
                else:
                    errors = len(sub_only_ids)
            except Exception:
                logger.exception("Bulk search failed for series %d", series_id)
                errors = len(sub_only_ids)
    finally:
        await sonarr.close()
        await db.close()

    message = f"Triggered {triggered} searches"
    if errors:
        message += f" ({errors} failed)"

    return HTMLResponse(f"<span>{message}</span>")


@router.post("/api/series/{series_id}/exclude")
async def toggle_series_exclude(request: Request, series_id: int):
    """Toggle search exclusion for a series."""
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        currently_excluded = await models.is_series_excluded(db, series_id)
        new_state = not currently_excluded
        await models.set_series_excluded(db, series_id, new_state)
    finally:
        await db.close()

    if new_state:
        btn_html = (
            '<button class="btn-exclude btn-exclude--active"'
            f' hx-post="/api/series/{series_id}/exclude"'
            ' hx-target="#exclude-btn-wrap"'
            ' hx-swap="innerHTML">'
            '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
            '<circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/>'
            '</svg>'
            'Excluded from Search'
            '<span class="htmx-indicator"><span class="spinner"></span></span>'
            '</button>'
        )
    else:
        btn_html = (
            '<button class="btn-exclude"'
            f' hx-post="/api/series/{series_id}/exclude"'
            ' hx-target="#exclude-btn-wrap"'
            ' hx-swap="innerHTML">'
            '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
            '<circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/>'
            '</svg>'
            'Exclude from Search'
            '<span class="htmx-indicator"><span class="spinner"></span></span>'
            '</button>'
        )

    return HTMLResponse(btn_html)


@router.post("/api/test-sonarr")
async def test_sonarr(request: Request):
    form = await request.form()
    settings = get_settings()
    cfg = await get_effective_settings()
    url = form.get("SONARR_URL", "") or cfg.get("SONARR_URL", "")
    # The form no longer carries the stored key, so fall back to what is saved.
    api_key = form.get("SONARR_API_KEY", "") or cfg.get("SONARR_API_KEY", "")

    if not url:
        return HTMLResponse(
            '<span style="color:#e17055">Sonarr URL is required.</span>'
        )

    sonarr = SonarrClient(url, api_key)
    try:
        ok, message = await sonarr.test_connection()
    except Exception as e:
        logger.warning("Sonarr connection test failed: %s", e)
        ok, message = False, "Connection test failed. Check server logs."
    finally:
        await sonarr.close()

    # Auto-save connection settings on successful test
    if ok:
        db = await get_db(settings.DB_PATH)
        try:
            await models.set_setting(db, "SONARR_URL", url)
            await models.set_setting(db, "SONARR_API_KEY", api_key)
        finally:
            await db.close()
        from src.config import invalidate_effective_settings_cache
        invalidate_effective_settings_cache()
        message += " (saved)"

    if not ok:
        message = "Could not reach Sonarr. Check the URL and API key, then the server logs."
    color = "#2dd4bf" if ok else "#f43f5e"
    return HTMLResponse(f'<span style="color:{color}">{esc(message)}</span>')


@router.post("/api/test-plex")
async def test_plex(request: Request):
    form = await request.form()
    settings = get_settings()
    cfg = await get_effective_settings()
    url = form.get("PLEX_URL", "") or cfg.get("PLEX_URL", "")
    token = form.get("PLEX_TOKEN", "") or cfg.get("PLEX_TOKEN", "")

    if not url:
        return HTMLResponse(
            '<span style="color:#e17055">Plex URL is required.</span>'
        )

    plex = PlexClient(url, token)
    try:
        ok, message = await plex.test_connection()
    except Exception as e:
        logger.warning("Plex connection test failed: %s", e)
        ok, message = False, "Connection test failed. Check server logs."
    finally:
        await plex.close()

    # Auto-save connection settings on successful test
    if ok:
        db = await get_db(settings.DB_PATH)
        try:
            await models.set_setting(db, "PLEX_URL", url)
            await models.set_setting(db, "PLEX_TOKEN", token)
        finally:
            await db.close()
        from src.config import invalidate_effective_settings_cache
        invalidate_effective_settings_cache()
        message += " (saved)"

    if not ok:
        message = "Could not reach Plex. Check the URL and token, then the server logs."
    color = "#2dd4bf" if ok else "#f43f5e"
    return HTMLResponse(f'<span style="color:{color}">{esc(message)}</span>')


@router.post("/api/test-jellyfin")
async def test_jellyfin(request: Request):
    from src.scanner.jellyfin import JellyfinClient

    form = await request.form()
    settings = get_settings()
    cfg = await get_effective_settings()
    url = form.get("JELLYFIN_URL", "") or cfg.get("JELLYFIN_URL", "")
    api_key = form.get("JELLYFIN_API_KEY", "") or cfg.get("JELLYFIN_API_KEY", "")

    if not url:
        return HTMLResponse(
            '<span style="color:#e17055">Jellyfin URL is required.</span>'
        )

    jellyfin = JellyfinClient(url, api_key)
    try:
        ok, message = await jellyfin.test_connection()
    except Exception as e:
        logger.warning("Jellyfin connection test failed: %s", e)
        ok, message = False, "Connection test failed. Check server logs."
    finally:
        await jellyfin.close()

    if ok:
        db = await get_db(settings.DB_PATH)
        try:
            await models.set_setting(db, "JELLYFIN_URL", url)
            await models.set_setting(db, "JELLYFIN_API_KEY", api_key)
        finally:
            await db.close()
        from src.config import invalidate_effective_settings_cache
        invalidate_effective_settings_cache()
        message += " (saved)"
    else:
        message = "Could not reach Jellyfin. Check the URL and API key, then the server logs."

    color = "#2dd4bf" if ok else "#f43f5e"
    return HTMLResponse(f'<span style="color:{color}">{esc(message)}</span>')


@router.post("/api/setup-sonarr-dub")
async def setup_sonarr_dub(request: Request):
    """Auto-configure Sonarr to prefer dubbed releases."""
    cfg = await get_effective_settings()
    if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
        return HTMLResponse('<span style="color:#e17055">Sonarr is not configured.</span>')
    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    try:
        result = await sonarr.ensure_dub_preference()
    finally:
        await sonarr.close()

    if result.get("error"):
        return HTMLResponse(f'<span style="color:#e17055">Error: {esc(result["error"])}</span>')

    parts = []
    if result["format_created"]:
        parts.append(f'Created custom format: {esc(result["format_name"])}')
    else:
        parts.append(f'Custom format already exists: {esc(result["format_name"])}')
    if result["profiles_updated"]:
        parts.append(f'Updated profiles: {esc(", ".join(result["profiles_updated"]))}')
    if result["profiles_already_configured"]:
        parts.append(f'Already configured: {esc(", ".join(result["profiles_already_configured"]))}')

    return HTMLResponse(f'<span style="color:#00b894">{"<br>".join(parts)}</span>')


@router.post("/settings")
async def save_settings(request: Request):
    form = await request.form()
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    ok = True
    try:
        for key in SETTING_KEYS:
            value = form.get(key)
            if key in ("SHOW_THUMBNAILS", "AUTO_TAG_SONARR", "AUTO_COLLECTIONS_PLEX",
                       "AUTO_RESOLVE_IMPORTS", "AUTO_MONITOR_DUBS", "STUCK_IMPORT_DRY_RUN"):
                # Checkbox: present = "true", absent = "false"
                await models.set_setting(db, key, "true" if value else "false")
            elif key in SECRET_KEYS:
                # The form never carries the stored secret, so an empty field
                # means "unchanged" rather than "clear it".
                if value:
                    if key == "AUTH_PASSWORD":
                        await models.set_setting(db, "AUTH_PASSWORD_HASH", hash_password(str(value)))
                    else:
                        await models.set_setting(db, key, str(value))
            elif value is not None:
                await models.set_setting(db, key, str(value))
    except Exception:
        logger.exception("Failed to save settings")
        ok = False
    finally:
        await db.close()

    from src.config import invalidate_effective_settings_cache
    invalidate_effective_settings_cache()

    interval_value = form.get("SCAN_INTERVAL_HOURS")
    if interval_value:
        try:
            from src.scheduler import reschedule_scan
            reschedule_scan(int(interval_value))
        except (ValueError, TypeError):
            logger.warning("Invalid SCAN_INTERVAL_HOURS value: %r", interval_value)

    return RedirectResponse(url=f"/settings?saved={1 if ok else 2}", status_code=303)


@router.post("/api/ignore-path")
async def add_ignore_path(request: Request):
    form = await request.form()
    pattern = form.get("pattern", "").strip()
    note = form.get("note", "").strip() or None
    if not pattern:
        return HTMLResponse('<span style="color:#f43f5e">Pattern is required</span>')

    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        await models.add_ignored_path(db, pattern, note=note)
        paths = await models.get_ignored_paths(db)
    finally:
        await db.close()

    return _render_ignore_list(paths)


@router.post("/api/ignore-path/remove/{path_id}")
async def remove_ignore_path(request: Request, path_id: int):
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        await models.remove_ignored_path(db, path_id)
        paths = await models.get_ignored_paths(db)
    finally:
        await db.close()

    return _render_ignore_list(paths)


def _render_ignore_list(paths: list[dict]) -> HTMLResponse:
    if not paths:
        return HTMLResponse('<p class="text-muted" style="font-size:0.85rem;">No ignored paths. All libraries and series will be scanned.</p>')

    rows = []
    for p in paths:
        rows.append(
            f'<div style="display:flex;align-items:center;gap:0.5rem;padding:0.4rem 0;border-bottom:1px solid var(--border-color);">'
            f'<code style="flex:1;font-size:0.82rem;color:var(--text-primary);">{esc(p["pattern"])}</code>'
            f'<span style="font-size:0.75rem;color:var(--text-muted);">{esc(p.get("note") or "")}</span>'
            f'<button class="btn btn-sm" style="background:rgba(244,63,94,0.15);color:var(--red);border:1px solid rgba(244,63,94,0.3);padding:0.2rem 0.5rem;font-size:0.72rem;" '
            f'hx-post="/api/ignore-path/remove/{p["id"]}" hx-target="#ignore-list" hx-swap="innerHTML" '
            f'hx-confirm="Remove this pattern?">Remove</button>'
            f'</div>'
        )
    return HTMLResponse("".join(rows))


@router.get("/api/discover/plex")
async def discover_plex(request: Request):
    """Return the configured media server's libraries as JSON for the settings UI.

    Still served from the historical /api/discover/plex path so existing
    bookmarks and the settings page keep working, but it now reports whichever
    of Plex/Jellyfin is in use.
    """
    from src.scanner.media_server import create_media_client, server_label

    cfg = await get_effective_settings()
    client, kind = create_media_client(cfg)
    if client is None:
        return JSONResponse({"error": "No media server configured"}, status_code=400)

    label = server_label(kind)
    try:
        libraries = await client.get_libraries()
    except Exception as e:
        logger.exception("Failed to discover %s libraries: %s", label, e)
        return JSONResponse(
            {"error": f"Failed to discover {label} libraries. Check server logs."},
            status_code=500,
        )
    finally:
        await client.close()

    # Check which ones are currently ignored
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        ignored = await models.get_ignored_paths(db)
    finally:
        await db.close()

    ignored_patterns = [p["pattern"].lower() for p in ignored]
    for lib in libraries:
        lib["ignored"] = (
            any(p in lib["path"].lower() for p in ignored_patterns)
            if lib.get("path")
            else False
        )

    return JSONResponse({"libraries": libraries, "server": label})


@router.get("/api/discover/sonarr")
async def discover_sonarr(request: Request):
    """Return Sonarr root folders and tags as JSON."""
    cfg = await get_effective_settings()
    if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
        return JSONResponse({"error": "Sonarr not configured"}, status_code=400)

    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    try:
        root_folders = await sonarr.get_root_folders()
        tags = await sonarr.get_tags()
        series_counts = await sonarr.get_series_by_root_folder()
    except Exception as e:
        logger.exception("Failed to discover Sonarr resources: %s", e)
        return JSONResponse({"error": "Failed to discover Sonarr resources. Check server logs."}, status_code=500)
    finally:
        await sonarr.close()

    # Enrich root folders with series count
    for rf in root_folders:
        rf["series_count"] = series_counts.get(rf["path"], 0)

    # Check which are ignored
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        ignored = await models.get_ignored_paths(db)
    finally:
        await db.close()

    ignored_patterns = [p["pattern"].lower() for p in ignored]
    for rf in root_folders:
        rf["ignored"] = any(p in rf["path"].lower() for p in ignored_patterns)
    for tag in tags:
        tag["ignored"] = any(p in tag["label"].lower() for p in ignored_patterns)

    return JSONResponse({"root_folders": root_folders, "tags": tags})


@router.get("/api/diagnostics")
async def diagnostics_json(request: Request):
    """Machine-readable "why is Babel finding nothing" report."""
    from src.scanner.diagnostics import run_diagnostics

    try:
        return JSONResponse(await run_diagnostics())
    except Exception:
        logger.exception("Diagnostics failed")
        return JSONResponse({"error": "Diagnostics failed. Check server logs."}, status_code=500)


@router.post("/api/diagnostics/html")
async def diagnostics_html(request: Request):
    """Same report, rendered for the Settings page."""
    from src.scanner.diagnostics import run_diagnostics

    try:
        report = await run_diagnostics()
    except Exception:
        logger.exception("Diagnostics failed")
        return HTMLResponse(
            '<div class="flash flash-error">Diagnostics failed. Check server logs.</div>'
        )

    colors = {"ok": "#2dd4bf", "warn": "#fbbf24", "error": "#f43f5e", "info": "var(--text-muted)"}
    icons = {"ok": "✓", "warn": "!", "error": "×", "info": "·"}

    rows = []
    for check in report["checks"]:
        color = colors.get(check["level"], "var(--text-muted)")
        hint = (
            f'<div style="font-size:0.72rem;color:var(--text-muted);margin-top:0.15rem;">'
            f'{esc(check["hint"])}</div>'
            if check.get("hint") else ""
        )
        rows.append(
            f'<div style="padding:0.4rem 0;border-bottom:1px solid var(--border-color);">'
            f'<div style="font-size:0.8rem;">'
            f'<span style="color:{color};font-weight:700;margin-right:0.4rem;">{icons.get(check["level"], "·")}</span>'
            f'<strong style="color:var(--text-primary);">{esc(check["name"])}</strong>'
            f'<span style="color:var(--text-muted);"> — {esc(check["message"])}</span>'
            f'</div>{hint}</div>'
        )

    summary = report["summary"]
    headline = (
        f'{summary["ok"]} ok, {summary["warnings"]} warning(s), {summary["errors"]} error(s)'
    )
    return HTMLResponse(
        f'<div style="font-size:0.75rem;color:var(--text-muted);margin-bottom:0.4rem;">{headline}</div>'
        + "".join(rows)
    )


@router.get("/api/scan/progress")
async def scan_progress(request: Request):
    """Return live scan progress as an HTML partial for HTMX polling."""
    from src.scanner.engine import is_scan_running, get_scan_progress, _scan_cancel
    if not is_scan_running():
        return HTMLResponse("")

    p = get_scan_progress()
    stopping = _scan_cancel.is_set()

    if p["phase"] == "indexing_plex":
        status_html = '<span class="badge badge-blue" style="margin-right:0.4rem;">Indexing Plex</span>'
        # Try to get live Plex indexing progress
        from src.scanner.engine import _plex_client_ref
        if _plex_client_ref is not None:
            plex_prog = _plex_client_ref.get_index_progress()
            if plex_prog.get("total", 0) > 0:
                detail = f"Indexing: {plex_prog['current']}/{plex_prog['total']} episodes ({plex_prog['section']})"
            else:
                detail = p.get("last_log", "Building audio track index...")
        else:
            detail = p.get("last_log", "Building audio track index...")
    elif p["phase"] == "scanning":
        status_html = f'<span class="badge badge-blue" style="margin-right:0.4rem;">{p["series_index"]}/{p["series_total"]}</span>'
        detail = p.get("last_log", p["current_series"])
    else:
        status_html = '<span class="badge badge-blue" style="margin-right:0.4rem;">Starting...</span>'
        detail = "Initializing scan..."

    if stopping:
        status_html = '<span class="badge badge-yellow" style="margin-right:0.4rem;">Stopping...</span>'

    bar_pct = int(p["series_index"] / p["series_total"] * 100) if p["series_total"] else 0

    html = f'''
    <div style="background:var(--bg-card);border:1px solid var(--border-color);border-radius:8px;padding:0.75rem 1rem;margin-bottom:0.75rem;">
        <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem;">
            <span class="spinner" style="width:14px;height:14px;border-width:2px;flex-shrink:0;"></span>
            {status_html}
            <span style="font-size:0.82rem;color:var(--text-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{esc(detail)}</span>
        </div>
        <div style="width:100%;height:4px;background:rgba(255,255,255,0.06);border-radius:2px;overflow:hidden;margin-bottom:0.5rem;">
            <div style="height:100%;width:{bar_pct}%;background:linear-gradient(90deg,var(--accent),var(--green));border-radius:2px;transition:width 0.3s ease;"></div>
        </div>
        <div style="display:flex;gap:0.6rem;font-size:0.75rem;font-family:\'JetBrains Mono\',monospace;color:var(--text-muted);">
            <span>{p["episodes_checked"]} checked</span>
            <span style="color:var(--green);">{p["dubbed_found"]} dubbed</span>
            <span style="color:var(--red);">{p["sub_only_found"]} sub-only</span>
            <span style="color:var(--blue);">{p["searches_triggered"]} searches</span>
        </div>
    </div>
    '''
    return HTMLResponse(html)


@router.get("/activity")
async def activity_page(request: Request):
    return _templates(request).TemplateResponse(request, "activity.html", {})


async def _build_activity_data(sonarr, db) -> dict:
    """Shared queue/recent-upgrades assembly used by both the JSON and HTML
    activity endpoints — previously duplicated ~90 lines between them."""
    from datetime import datetime, timezone

    queue_items = await sonarr.get_queue()
    pending_ids = await models.get_pending_episode_ids(db)
    upgrade_stats = await models.get_upgrade_stats(db)

    relevant_items = [i for i in queue_items if i.get("episodeId") in pending_ids]
    series_ids = {i["seriesId"] for i in relevant_items if i.get("seriesId")}
    poster_urls = await models.get_poster_urls_for_series(db, series_ids)

    queue = []
    counts = {"downloading": 0, "completed": 0, "warning": 0}
    for item in relevant_items:
        size_total = item.get("size", 0) or 0
        size_left = item.get("sizeleft", 0) or 0
        progress = int((1 - size_left / size_total) * 100) if size_total > 0 else 0

        state = (item.get("trackedDownloadState") or "").lower()
        status_val = (item.get("trackedDownloadStatus") or "").lower()
        status_msgs = item.get("statusMessages") or []

        if status_val in ("warning", "error"):
            cat = "warning"
        elif state == "importpending" or progress >= 100:
            cat = "completed"
        else:
            cat = "downloading"

        counts[cat] += 1

        warning_msg = ""
        if status_msgs:
            msgs = []
            for sm in status_msgs:
                for m in sm.get("messages", []):
                    msgs.append(m)
            warning_msg = " | ".join(msgs)

        queue.append({
            "title": item.get("title", ""),
            "series": (item.get("series", {}) or {}).get("title", ""),
            "episode": _fmt_episode(item),
            "status": cat,
            "progress": max(0, min(progress, 100)),
            "size": _fmt_bytes(size_total),
            "downloaded": _fmt_bytes(size_total - size_left),
            "eta": item.get("timeleft") or "",
            "message": warning_msg,
            "poster_url": poster_urls.get(item.get("seriesId")),
        })

    # Sort: downloading first (by progress desc), then completed, then warning
    order = {"downloading": 0, "completed": 1, "warning": 2}
    queue.sort(key=lambda x: (order.get(x["status"], 9), -x["progress"]))

    recent_rows = await models.get_recent_resolved_upgrades(db, limit=20)
    now = datetime.now(timezone.utc)
    recent = []
    for r in recent_rows:
        resolved_at = r.get("resolved_at", "")
        when_str = _time_ago(resolved_at, now) if resolved_at else ""
        recent.append({
            "series": r.get("series_title", ""),
            "episode": f"S{r.get('season_number', 0) or 0:02d}E{r.get('episode_number', 0) or 0:02d}",
            "result": r.get("result", ""),
            "when": when_str,
        })

    return {"queue": queue, "recent": recent, "counts": counts, "upgrade_stats": upgrade_stats}


@router.get("/api/activity")
async def get_activity(request: Request):
    """Get live activity data — Sonarr queue + recent upgrade events."""
    cfg = await get_effective_settings()
    if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
        return JSONResponse({"queue": [], "recent": [], "stats": {"downloading": 0, "completed": 0, "warning": 0, "pending_total": 0}})
    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    settings = get_settings()
    db = await get_db(settings.DB_PATH)

    try:
        data = await _build_activity_data(sonarr, db)
        counts = data["counts"]
        return JSONResponse({
            "queue": data["queue"],
            "recent": data["recent"],
            "stats": {
                "downloading": counts["downloading"],
                "completed": counts["completed"],
                "warning": counts["warning"],
                "pending_total": data["upgrade_stats"].get("pending", 0),
            },
        })
    except Exception as e:
        logger.exception("Activity feed error: %s", e)
        return JSONResponse({"error": "Activity feed unavailable. Check server logs."}, status_code=500)
    finally:
        await sonarr.close()
        await db.close()


@router.get("/api/activity/html")
async def get_activity_html(request: Request):
    """Return activity feed as an HTML partial for HTMX."""
    cfg = await get_effective_settings()
    if not cfg.get("SONARR_URL") or not cfg.get("SONARR_API_KEY"):
        return HTMLResponse('<div class="empty-activity">Sonarr is not configured.</div>')
    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    settings = get_settings()
    db = await get_db(settings.DB_PATH)

    try:
        data = await _build_activity_data(sonarr, db)
        pending = data["upgrade_stats"].get("pending", 0)
        return _templates(request).TemplateResponse(
            request,
            "partials/activity_feed.html",
            {
                "queue": data["queue"],
                "recent": data["recent"],
                "counts": data["counts"],
                "pending": pending,
            },
        )
    except Exception:
        logger.exception("Activity HTML feed error")
        return HTMLResponse(
            '<div class="flash flash-error">Could not load activity. Check server logs.</div>'
        )
    finally:
        await sonarr.close()
        await db.close()


def _fmt_bytes(b: int | float) -> str:
    """Format bytes to human-readable GB/MB."""
    if b <= 0:
        return "0 B"
    if b >= 1_073_741_824:
        return f"{b / 1_073_741_824:.1f} GB"
    if b >= 1_048_576:
        return f"{b / 1_048_576:.0f} MB"
    if b >= 1024:
        return f"{b / 1024:.0f} KB"
    return f"{b:.0f} B"


def _fmt_episode(item: dict) -> str:
    """Format episode code from a Sonarr queue item."""
    ep = item.get("episode") or {}
    s = ep.get("seasonNumber", 0) or 0
    e = ep.get("episodeNumber", 0) or 0
    return f"S{s:02d}E{e:02d}"


def _time_ago(ts_str: str, now=None) -> str:
    """Convert ISO timestamp string to '5 min ago' style."""
    from datetime import datetime, timezone
    if now is None:
        now = datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = now - dt
        secs = int(delta.total_seconds())
        if secs < 60:
            return "just now"
        if secs < 3600:
            m = secs // 60
            return f"{m} min ago"
        if secs < 86400:
            h = secs // 3600
            return f"{h}h ago"
        d = secs // 86400
        return f"{d}d ago"
    except (ValueError, TypeError):
        return ""


@router.get("/dubs")
async def dubs_page(request: Request):
    tab = request.query_params.get("tab", "recent")
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        recently_dubbed = await models.get_recently_dubbed_series(db, days=30)
        dub_expected = await models.get_dub_expected_series(db)
        no_dub_count = await models.get_no_dub_count(db)
        no_dub_series = await models.get_no_dub_series(db)
    finally:
        await db.close()

    return _templates(request).TemplateResponse(
        request,
        "dubs.html",
        {
            "tab": tab,
            "recently_dubbed": recently_dubbed,
            "dub_expected": dub_expected,
            "no_dub_count": no_dub_count,
            "no_dub_series": no_dub_series,
        },
    )


@router.post("/api/lookup-dubs")
async def lookup_dubs(request: Request):
    """Start a dub availability lookup for all sub-only and partial series.

    Deliberately not awaited: MyAnimeList has to be polled one series at a
    time with a courtesy delay, so a few hundred series is several minutes —
    far longer than any browser or reverse proxy will hold a request open.
    """
    from src.scanner.dub_lookup import run_dub_lookup

    async def _run():
        try:
            summary = await run_dub_lookup()
            logger.info("Dub lookup finished: %s", summary)
        except Exception:
            logger.exception("Dub lookup failed")

    _spawn(_run())
    return HTMLResponse(
        '<div class="flash flash-success">Dub lookup started. It runs in the '
        'background — results appear on this page as series are checked, and '
        'the run is recorded in History.</div>'
    )


@router.post("/api/webhook/sonarr")
async def sonarr_webhook(request: Request):
    """Handle Sonarr webhook events for instant upgrade detection."""
    import hmac

    # Check webhook secret if configured (env var or DB-configured override)
    cfg = await get_effective_settings()
    webhook_key = cfg.get("WEBHOOK_SECRET", "")

    if webhook_key:
        provided = request.query_params.get("apikey", "") or request.headers.get("x-api-key", "")
        # Encoded first: compare_digest raises TypeError on a str holding
        # non-ASCII, which turned a bad key into a 500 instead of a 401.
        if not hmac.compare_digest(provided.encode("utf-8"), str(webhook_key).encode("utf-8")):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    event_type = payload.get("eventType", "")
    logger.info("Sonarr webhook: %s", event_type)

    # Only handle Download/Import events
    if event_type not in ("Download", "EpisodeFileDelete", "SeriesAdd"):
        return JSONResponse({"status": "ignored", "event": event_type})

    if event_type == "Download":
        # A file was downloaded/imported — check if it's Babel-tracked
        series_data = payload.get("series", {})
        episode_data = payload.get("episodes", [{}])
        episode_file = payload.get("episodeFile", {})

        series_id = series_data.get("id")
        series_title = series_data.get("title", "")

        if not series_id:
            return JSONResponse({"status": "no series id"})

        # Re-check audio for the affected episodes
        settings = get_settings()
        db = await get_db(settings.DB_PATH)
        target_lang = cfg.get("TARGET_LANGUAGE", "eng")
        from src.scanner import ffprobe

        try:
            # Check if this series is in our DB
            series = await models.get_series(db, series_id)
            if not series:
                return JSONResponse({"status": "series not tracked"})

            # For each episode in the webhook, update its status
            updated = 0
            unreadable = 0
            for ep in episode_data:
                ep_id = ep.get("id")
                if not ep_id:
                    continue

                # Prefer the absolute path Sonarr reports; relativePath alone
                # would corrupt file-change detection and defeat path-based lookups.
                file_path = episode_file.get("path") or episode_file.get("relativePath", "")
                file_size = episode_file.get("size", 0)

                # Update the episode in DB
                await models.upsert_episode(
                    db, ep_id, series_id,
                    ep.get("seasonNumber"), ep.get("episodeNumber"),
                    ep.get("title"), file_path, file_size,
                )

                # Check audio via ffprobe directly — a single-file probe is fast
                # and avoids triggering a full Plex library index rebuild per episode.
                tracks = None
                if file_path:
                    local_path = translate_path(file_path, "local", cfg)
                    tracks = await ffprobe.get_audio_tracks(local_path)
                    if tracks is not None:
                        for t in tracks:
                            t["source"] = "ffprobe"

                if not tracks:
                    # ffprobe could not read the file — most often because the
                    # media volume is not mounted into the container. Say so
                    # rather than reporting the episode as processed.
                    unreadable += 1
                    logger.warning(
                        "Webhook: could not read audio for %s S%02dE%02d (%s)",
                        series_title, ep.get("seasonNumber", 0) or 0,
                        ep.get("episodeNumber", 0) or 0, file_path or "no path",
                    )
                    continue

                if tracks and len(tracks) > 0:
                    await models.replace_audio_tracks(db, ep_id, tracks)
                    languages = {t["language"] for t in tracks}
                    status = "DUBBED" if target_lang in languages else "SUB_ONLY"
                    await models.update_episode_status(db, ep_id, status)

                    # Resolve any pending upgrade
                    if status == "DUBBED":
                        await models.resolve_upgrade(db, ep_id, file_size, status, "success")
                        logger.info("Webhook: %s S%02dE%02d upgraded to DUBBED",
                                    series_title, ep.get("seasonNumber", 0), ep.get("episodeNumber", 0))
                    else:
                        await models.resolve_upgrade(db, ep_id, file_size, status, "failed")

                updated += 1

            # Update series counts
            await models.update_series_counts(db, series_id)
        finally:
            await db.close()

        return JSONResponse({
            "status": "processed",
            "episodes_updated": updated,
            "episodes_unreadable": unreadable,
        })

    return JSONResponse({"status": "ok"})


def _db_size(db_path: str) -> int:
    """stat() the DB file. Called via a thread — never inline on the loop."""
    try:
        return os.path.getsize(db_path)
    except OSError:
        return 0


@router.get("/api/health")
async def health_check():
    from src.scanner.engine import is_scan_running
    from src.watchdog import loop_lag_seconds

    settings = get_settings()
    db_path = settings.DB_PATH
    db_size = await asyncio.to_thread(_db_size, db_path)

    # The library counts are a convenience on this endpoint, not its purpose:
    # Docker polls it every 30s purely to learn whether the process is alive.
    # A locked database during a write-heavy scan must not fail the probe, and
    # must not silently report zeroes either.
    stats: dict = {}
    upgrade_stats: dict = {}
    stats_ok = True
    try:
        db = await get_db(settings.DB_PATH)
        try:
            stats = await models.get_overview_stats(db)
            upgrade_stats = await models.get_upgrade_stats(db)
        finally:
            await db.close()
    except Exception:
        stats_ok = False
        logger.warning("Health check could not read library stats", exc_info=True)

    from src import __version__, __revision__

    # A stalled event loop cannot answer this request at all, so reaching here
    # already proves the loop is turning. Reporting the lag still matters: it
    # catches a loop that is degraded rather than fully wedged, and makes the
    # Docker healthcheck fail *before* the stall grows long enough to start
    # leaking unreaped children.
    lag = loop_lag_seconds()
    healthy = lag <= settings.WATCHDOG_UNHEALTHY_LAG

    return JSONResponse(status_code=200 if healthy else 503, content={
        "status": "ok" if healthy else "degraded",
        "version": __version__,
        "revision": __revision__,
        "loopLagSeconds": round(lag, 3),
        "scanning": is_scan_running(),
        "statsAvailable": stats_ok,
        "lastScan": _to_utc_display(stats.get("last_scan_time")),
        "nextScan": _to_utc_display(get_next_run_time()),
        "dbSizeBytes": db_size,
        "series": stats.get("total_series") if stats_ok else None,
        "dubbed": stats.get("fully_dubbed") if stats_ok else None,
        "subOnly": stats.get("sub_only") if stats_ok else None,
        "partial": stats.get("partially_dubbed") if stats_ok else None,
        "unknown": stats.get("unknown") if stats_ok else None,
        "empty": stats.get("empty") if stats_ok else None,
        "pendingUpgrades": upgrade_stats.get("pending") if stats_ok else None,
        "successfulUpgrades": upgrade_stats.get("success") if stats_ok else None,
    })


@router.get("/logs")
async def logs_page(request: Request):
    return _templates(request).TemplateResponse(request, "logs.html", {})


# Reading the whole rotating log (up to 5 MB) to show the last screenful was
# wasteful on every poll, and `?lines=0` — or any negative value — turned
# `all_lines[-lines:]` into "the entire file".
_LOG_TAIL_BYTES = 512 * 1024
_MAX_LOG_LINES = 2000


@router.get("/api/logs")
async def get_logs(request: Request, lines: int = 200, level: str = ""):
    """Return the last N lines of the log file."""
    from pathlib import Path
    log_file = Path(__file__).resolve().parent.parent.parent / "data" / "babel.log"

    lines = max(1, min(int(lines), _MAX_LOG_LINES))

    def _read() -> list[str] | None:
        try:
            with open(log_file, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size - _LOG_TAIL_BYTES))
                chunk = f.read()
        except FileNotFoundError:
            return None
        text = chunk.decode("utf-8", errors="replace")
        if size > _LOG_TAIL_BYTES:
            # The first line in the window is probably truncated mid-line.
            text = text.split("\n", 1)[-1]
        return text.splitlines()

    # Off the loop: this opens and reads a file, and no filesystem call belongs
    # on the event loop thread.
    all_lines = await asyncio.to_thread(_read)
    if all_lines is None:
        return HTMLResponse('<p class="text-muted">No log file found.</p>')

    # Filter by level if specified
    if level:
        level_upper = level.upper()
        all_lines = [line for line in all_lines if level_upper in line]

    recent = all_lines[-lines:]

    # Render as HTML
    html = '<div style="font-family:\'JetBrains Mono\',monospace;font-size:0.72rem;line-height:1.5;">'
    for line in recent:
        line = line.strip()
        color = "var(--text-muted)"
        if "ERROR" in line:
            color = "var(--red)"
        elif "WARNING" in line:
            color = "var(--yellow)"
        elif "INFO" in line:
            color = "var(--text-primary)"
        html += f'<div style="color:{color};border-bottom:1px solid rgba(255,255,255,0.03);padding:1px 0;">{esc(line)}</div>'
    html += '</div>'
    return HTMLResponse(html)


@router.get("/favicon.ico")
async def favicon():
    svg = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">'
           '<defs><linearGradient id="g" x1="16" y1="1" x2="16" y2="30" gradientUnits="userSpaceOnUse">'
           '<stop offset="0%" stop-color="#a855f7"/><stop offset="100%" stop-color="#2dd4bf"/></linearGradient></defs>'
           '<polygon points="16,1 18.5,7 13.5,7" fill="url(#g)"/>'
           '<polygon points="13.5,8.5 18.5,8.5 19.5,13 12.5,13" fill="url(#g)"/>'
           '<polygon points="12,14.5 20,14.5 21.5,19.5 10.5,19.5" fill="url(#g)"/>'
           '<polygon points="10,21 22,21 24,26.5 8,26.5" fill="url(#g)"/>'
           '<rect x="6" y="27.5" width="20" height="2" rx="0.5" fill="url(#g)" opacity="0.5"/>'
           '</svg>')
    return HTMLResponse(content=svg, media_type="image/svg+xml")
