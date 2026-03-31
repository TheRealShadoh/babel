"""
Babel web routes — FastAPI router for dashboard pages and API endpoints.
"""

import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.config import get_settings, get_effective_settings
from src.db.database import get_db
from src.db import models
from src.scanner.engine import run_scan, check_download_status, resolve_stuck_imports
from src.scanner.dub_lookup import lookup_dub_info, bulk_lookup
from src.scanner.sonarr import SonarrClient
from src.scanner.plex import PlexClient
from src.scheduler import get_next_run_time

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SETTING_KEYS = (
    "SONARR_URL",
    "SONARR_API_KEY",
    "PLEX_URL",
    "PLEX_TOKEN",
    "SCAN_INTERVAL_HOURS",
    "TARGET_LANGUAGE",
    "SEARCH_COOLDOWN_DAYS",
    "SEARCH_RATE_LIMIT",
    "SONARR_PATH_PREFIX",
    "LOCAL_PATH_PREFIX",
    "PLEX_PATH_PREFIX",
    "ANIME_FILTER",
    "LOG_LEVEL",
    "SHOW_THUMBNAILS",
    "MAX_SEARCH_ATTEMPTS",
    "AUTO_TAG_SONARR",
    "DISCORD_WEBHOOK_URL",
    "AUTO_COLLECTIONS_PLEX",
    "AUTO_RESOLVE_IMPORTS",
    "WEBHOOK_SECRET",
)


def _templates(request: Request):
    """Shortcut to the Jinja2Templates instance stored on the app."""
    return request.app.state.templates


def _build_sonarr_url(base_url: str, series: dict) -> str | None:
    """Build a Sonarr series page URL from the series path."""
    import re
    if not base_url or not series.get("sonarr_path"):
        return None
    # Use the folder name as a slug approximation
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

    # Format next scan time to human-readable
    next_scan = get_next_run_time()
    if next_scan:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(next_scan)
            next_scan = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            pass

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
        all_series, total_count = await models.get_series_filtered(
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
    show_thumbnails = show_thumbs_val != "false"  # default true

    total_pages = max(1, math.ceil(total_count / per_page))
    # Clamp page to valid range
    if page > total_pages:
        page = total_pages

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
         "sonarr_url": _build_sonarr_url(settings.SONARR_URL, series) if series["id"] > 0 else None},
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
    for key in SETTING_KEYS:
        env_val = getattr(settings, key, "")
        current[key] = db_settings.get(key, str(env_val))

    flash_message = "Settings saved successfully." if saved else None

    return _templates(request).TemplateResponse(
        request,
        "settings.html",
        {"settings": current, "flash_message": flash_message, "ignored_paths": ignored_paths},
    )


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


@router.post("/api/scan")
async def trigger_scan(request: Request):
    from src.scanner.engine import is_scan_running
    if is_scan_running():
        return HTMLResponse(
            '<div class="flash" style="background-color:rgba(245,158,11,0.15);border:1px solid #f59e0b;padding:0.75rem 1rem;border-radius:6px;color:#f59e0b;">'
            'A scan is already running.</div>'
        )
    asyncio.create_task(run_scan())
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
    cfg = await get_effective_settings()
    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    db = await get_db(cfg.get("DB_PATH", get_settings().DB_PATH))
    triggered = 0
    errors = 0
    try:
        episodes = await models.get_episodes_for_series(db, series_id)
        sub_only = [ep for ep in episodes if ep["dub_status"] == "SUB_ONLY"]

        for ep in sub_only:
            try:
                success = await sonarr.search_episodes([ep["id"]])
                if success:
                    await models.add_search_record(
                        db, ep["id"], trigger_source="manual_bulk"
                    )
                    triggered += 1
                else:
                    errors += 1
                # Rate limit between searches
                await asyncio.sleep(60.0 / max(int(cfg.get("SEARCH_RATE_LIMIT", 5)), 1))
            except Exception:
                logger.exception("Bulk search failed for episode %d", ep["id"])
                errors += 1
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
    url = form.get("SONARR_URL", "") or settings.SONARR_URL
    api_key = form.get("SONARR_API_KEY", "") or settings.SONARR_API_KEY

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
        message += " (saved)"

    color = "#2dd4bf" if ok else "#f43f5e"
    return HTMLResponse(f'<span style="color:{color}">{message}</span>')


@router.post("/api/test-plex")
async def test_plex(request: Request):
    form = await request.form()
    settings = get_settings()
    url = form.get("PLEX_URL", "") or settings.PLEX_URL
    token = form.get("PLEX_TOKEN", "") or settings.PLEX_TOKEN

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
        message += " (saved)"

    color = "#2dd4bf" if ok else "#f43f5e"
    return HTMLResponse(f'<span style="color:{color}">{message}</span>')


@router.post("/api/setup-sonarr-dub")
async def setup_sonarr_dub(request: Request):
    """Auto-configure Sonarr to prefer dubbed releases."""
    cfg = await get_effective_settings()
    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    try:
        result = await sonarr.ensure_dub_preference()
    finally:
        await sonarr.close()

    if result.get("error"):
        return HTMLResponse(f'<span style="color:#e17055">Error: {result["error"]}</span>')

    parts = []
    if result["format_created"]:
        parts.append(f'Created custom format: {result["format_name"]}')
    else:
        parts.append(f'Custom format already exists: {result["format_name"]}')
    if result["profiles_updated"]:
        parts.append(f'Updated profiles: {", ".join(result["profiles_updated"])}')
    if result["profiles_already_configured"]:
        parts.append(f'Already configured: {", ".join(result["profiles_already_configured"])}')

    return HTMLResponse(f'<span style="color:#00b894">{"<br>".join(parts)}</span>')


@router.post("/settings")
async def save_settings(request: Request):
    form = await request.form()
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        for key in SETTING_KEYS:
            value = form.get(key)
            if key in ("SHOW_THUMBNAILS", "AUTO_TAG_SONARR", "AUTO_COLLECTIONS_PLEX", "AUTO_RESOLVE_IMPORTS"):
                # Checkbox: present = "true", absent = "false"
                await models.set_setting(db, key, "true" if value else "false")
            elif value is not None:
                await models.set_setting(db, key, str(value))
    except Exception:
        logger.exception("Failed to save settings")
    finally:
        await db.close()

    return RedirectResponse(url="/settings?saved=1", status_code=303)


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
            f'<code style="flex:1;font-size:0.82rem;color:var(--text-primary);">{p["pattern"]}</code>'
            f'<span style="font-size:0.75rem;color:var(--text-muted);">{p.get("note") or ""}</span>'
            f'<button class="btn btn-sm" style="background:rgba(244,63,94,0.15);color:var(--red);border:1px solid rgba(244,63,94,0.3);padding:0.2rem 0.5rem;font-size:0.72rem;" '
            f'hx-post="/api/ignore-path/remove/{p["id"]}" hx-target="#ignore-list" hx-swap="innerHTML" '
            f'hx-confirm="Remove this pattern?">Remove</button>'
            f'</div>'
        )
    return HTMLResponse("".join(rows))


@router.get("/api/discover/plex")
async def discover_plex(request: Request):
    """Return Plex libraries as JSON for the settings UI."""
    cfg = await get_effective_settings()
    if not cfg.get("PLEX_URL") or not cfg.get("PLEX_TOKEN"):
        return JSONResponse({"error": "Plex not configured"}, status_code=400)

    plex = PlexClient(cfg["PLEX_URL"], cfg["PLEX_TOKEN"])
    try:
        libraries = await plex.get_libraries()
    except Exception as e:
        logger.exception("Failed to discover Plex libraries: %s", e)
        return JSONResponse({"error": "Failed to discover Plex libraries. Check server logs."}, status_code=500)
    finally:
        await plex.close()

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

    return JSONResponse({"libraries": libraries})


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
        pct = int(p["series_index"] / p["series_total"] * 100) if p["series_total"] else 0
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
            <span style="font-size:0.82rem;color:var(--text-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{detail}</span>
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


@router.get("/api/activity")
async def get_activity(request: Request):
    """Get live activity data — Sonarr queue + recent upgrade events."""
    from datetime import datetime, timezone

    cfg = await get_effective_settings()
    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    settings = get_settings()
    db = await get_db(settings.DB_PATH)

    try:
        # 1. Get Sonarr queue and Babel-tracked episode IDs
        queue_items = await sonarr.get_queue()
        pending_ids = await models.get_pending_episode_ids(db)
        upgrade_stats = await models.get_upgrade_stats(db)

        # 2. Filter queue to Babel-tracked and categorize
        queue = []
        counts = {"downloading": 0, "completed": 0, "warning": 0}
        for item in queue_items:
            ep_id = item.get("episodeId")
            if ep_id not in pending_ids:
                continue

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

            # Format sizes
            size_str = _fmt_bytes(size_total)
            downloaded_str = _fmt_bytes(size_total - size_left)

            # Build warning message if applicable
            warning_msg = ""
            if status_msgs:
                msgs = []
                for sm in status_msgs:
                    for m in sm.get("messages", []):
                        msgs.append(m)
                warning_msg = " | ".join(msgs)

            entry = {
                "title": item.get("title", ""),
                "series": (item.get("series", {}) or {}).get("title", ""),
                "episode": _fmt_episode(item),
                "status": cat,
                "progress": min(progress, 100),
                "size": size_str,
                "downloaded": downloaded_str,
                "eta": item.get("timeleft") or "",
                "message": warning_msg,
            }
            queue.append(entry)

        # Sort: downloading first (by progress desc), then completed, then warning
        order = {"downloading": 0, "completed": 1, "warning": 2}
        queue.sort(key=lambda x: (order.get(x["status"], 9), -x["progress"]))

        # 3. Recent resolved upgrades
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

        return JSONResponse({
            "queue": queue,
            "recent": recent,
            "stats": {
                "downloading": counts["downloading"],
                "completed": counts["completed"],
                "warning": counts["warning"],
                "pending_total": upgrade_stats.get("pending", 0),
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
    from datetime import datetime, timezone

    cfg = await get_effective_settings()
    sonarr = SonarrClient(cfg["SONARR_URL"], cfg["SONARR_API_KEY"])
    settings = get_settings()
    db = await get_db(settings.DB_PATH)

    try:
        queue_items = await sonarr.get_queue()
        pending_ids = await models.get_pending_episode_ids(db)
        upgrade_stats = await models.get_upgrade_stats(db)

        queue = []
        counts = {"downloading": 0, "completed": 0, "warning": 0}
        for item in queue_items:
            ep_id = item.get("episodeId")
            if ep_id not in pending_ids:
                continue

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

            size_str = _fmt_bytes(size_total)
            downloaded_str = _fmt_bytes(size_total - size_left)

            warning_msg = ""
            if status_msgs:
                msgs = []
                for sm in status_msgs:
                    for m in sm.get("messages", []):
                        msgs.append(m)
                warning_msg = " | ".join(msgs)

            # Try to get series poster from DB
            series_id = item.get("seriesId")
            poster_url = None
            if series_id:
                series_row = await models.get_series(db, series_id)
                if series_row:
                    poster_url = series_row.get("poster_url")

            queue.append({
                "title": item.get("title", ""),
                "series": (item.get("series", {}) or {}).get("title", ""),
                "episode": _fmt_episode(item),
                "status": cat,
                "progress": min(progress, 100),
                "size": size_str,
                "downloaded": downloaded_str,
                "eta": item.get("timeleft") or "",
                "message": warning_msg,
                "poster_url": poster_url,
            })

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

        # Build HTML
        html = _render_activity_html(queue, recent, counts, upgrade_stats)
        return HTMLResponse(html)

    except Exception as e:
        logger.exception("Activity HTML feed error")
        return HTMLResponse(
            f'<div class="flash flash-error">Error loading activity: {e}</div>'
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


def _render_activity_html(
    queue: list[dict],
    recent: list[dict],
    counts: dict,
    upgrade_stats: dict,
) -> str:
    """Render the full activity feed HTML partial."""
    total_queue = counts["downloading"] + counts["completed"] + counts["warning"]
    pending = upgrade_stats.get("pending", 0)

    # Stats bar
    html = '<div class="activity-stats">'
    html += f'<div class="stat-pill stat-blue"><span class="stat-num">{counts["downloading"]}</span> Downloading</div>'
    html += f'<div class="stat-pill stat-green"><span class="stat-num">{counts["completed"]}</span> Completed</div>'
    html += f'<div class="stat-pill stat-yellow"><span class="stat-num">{counts["warning"]}</span> Warning</div>'
    html += f'<div class="stat-pill stat-gray"><span class="stat-num">{pending}</span> Pending Total</div>'
    html += '</div>'

    # Active downloads section
    html += '<div class="activity-section">'
    html += '<h3 class="section-heading">Active Downloads</h3>'

    if not queue:
        html += '<div class="empty-activity">No Babel-tracked items in the Sonarr queue right now.</div>'
    else:
        html += '<div class="download-grid">'
        for item in queue:
            status = item["status"]
            badge_class = {"downloading": "badge-blue", "completed": "badge-green", "warning": "badge-yellow"}.get(status, "badge-gray")
            badge_label = status.upper()

            progress = item["progress"]
            bar_color = {"downloading": "var(--blue)", "completed": "var(--green)", "warning": "var(--yellow)"}.get(status, "var(--gray)")

            # Poster
            poster_html = ""
            if item.get("poster_url"):
                poster_html = f'<img src="{item["poster_url"]}" alt="" class="dl-poster" loading="lazy"/>'
            else:
                poster_html = '<div class="dl-poster dl-poster-placeholder"><svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="2" width="20" height="20" rx="2"/><circle cx="8" cy="8" r="2"/><path d="M21 15l-5-5L5 21"/></svg></div>'

            # ETA
            eta_html = ""
            if item.get("eta") and status == "downloading":
                eta_html = f'<span class="dl-eta">{item["eta"]}</span>'

            # Warning message
            warn_html = ""
            if item.get("message") and status == "warning":
                msg = item["message"]
                warn_html = f'<div class="dl-warning" style="word-break:break-word;">{msg}</div>'

            # Animated bar class
            bar_anim = ' bar-animated' if status == "downloading" else ''

            html += f'''<div class="dl-card">
                <div class="dl-card-inner">
                    {poster_html}
                    <div class="dl-info">
                        <div class="dl-header">
                            <span class="dl-series">{item["series"]}</span>
                            <span class="badge {badge_class}" style="font-size:0.65rem;padding:0.15rem 0.5rem;">{badge_label}</span>
                        </div>
                        <div class="dl-episode">{item["episode"]} &mdash; {item["title"][:60]}</div>
                        <div class="dl-progress-wrap">
                            <div class="dl-progress-bar{bar_anim}">
                                <div class="dl-progress-fill" style="width:{progress}%;background:{bar_color};"></div>
                            </div>
                            <span class="dl-pct">{progress}%</span>
                        </div>
                        <div class="dl-meta">
                            <span>{item["downloaded"]} / {item["size"]}</span>
                            {eta_html}
                        </div>
                        {warn_html}
                    </div>
                </div>
            </div>'''
        html += '</div>'  # download-grid

    html += '</div>'  # activity-section

    # Recently resolved section
    html += '<div class="activity-section">'
    html += '<h3 class="section-heading">Recently Resolved</h3>'

    if not recent:
        html += '<div class="empty-activity">No resolved upgrades yet.</div>'
    else:
        html += '<div class="recent-list">'
        for r in recent:
            if r["result"] == "success":
                icon = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="var(--green)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6L9 17l-5-5"/></svg>'
                result_class = "recent-success"
            else:
                icon = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="var(--red)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>'
                result_class = "recent-failed"

            html += f'''<div class="recent-item {result_class}">
                <span class="recent-icon">{icon}</span>
                <span class="recent-series">{r["series"]}</span>
                <span class="recent-ep">{r["episode"]}</span>
                <span class="recent-when">{r["when"]}</span>
            </div>'''
        html += '</div>'

    html += '</div>'  # activity-section

    return html


@router.get("/dubs")
async def dubs_page(request: Request):
    tab = request.query_params.get("tab", "recent")
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        recently_dubbed = await models.get_recently_dubbed_series(db, days=30)
        dub_expected = await models.get_dub_expected_series(db)
        no_dub_count = await models.get_no_dub_count(db)
        # Fetch actual no-dub series for the tab
        async with db.execute(
            "SELECT * FROM series WHERE dub_available = 'unlikely' ORDER BY title"
        ) as cur:
            from src.db.models import _rows_to_dicts
            no_dub_series = _rows_to_dicts(await cur.fetchall())
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
    """Trigger dub availability lookup for all sub-only and partial series."""
    from src.scanner.dub_lookup import run_dub_lookup
    result = await run_dub_lookup()

    if result["checked"] == 0:
        return HTMLResponse(
            '<div class="flash flash-success">All series already have dub availability info.</div>'
        )

    html = (
        f'<div class="flash flash-success">'
        f'Checked {result["checked"]} series. '
        f'<span style="color:var(--green);">{result["available"]} dub available</span> &middot; '
        f'<span style="color:var(--yellow);">{result["likely"]} dub likely</span> &middot; '
        f'<span style="color:var(--text-muted);">{result["unlikely"]} no known dub</span>'
        f'</div>'
    )
    return HTMLResponse(html)


@router.post("/api/webhook/sonarr")
async def sonarr_webhook(request: Request):
    """Handle Sonarr webhook events for instant upgrade detection."""
    # Check webhook secret if configured
    settings = get_settings()
    db = await get_db(settings.DB_PATH)
    try:
        webhook_key = await models.get_setting(db, "WEBHOOK_SECRET")
    finally:
        await db.close()

    if webhook_key:
        provided = request.query_params.get("apikey", "") or request.headers.get("x-api-key", "")
        if provided != webhook_key:
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
        cfg = await get_effective_settings()
        settings = get_settings()
        db = await get_db(settings.DB_PATH)
        try:
            # Check if this series is in our DB
            series = await models.get_series(db, series_id)
            if not series:
                return JSONResponse({"status": "series not tracked"})

            # For each episode in the webhook, update its status
            updated = 0
            for ep in episode_data:
                ep_id = ep.get("id")
                if not ep_id:
                    continue

                # Get the file path from the episode file
                file_path = episode_file.get("relativePath") or episode_file.get("path", "")
                file_size = episode_file.get("size", 0)

                # Update the episode in DB
                await models.upsert_episode(
                    db, ep_id, series_id,
                    ep.get("seasonNumber"), ep.get("episodeNumber"),
                    ep.get("title"), file_path, file_size,
                )

                # Check audio via Plex if available
                plex = None
                if cfg.get("PLEX_URL") and cfg.get("PLEX_TOKEN"):
                    plex = PlexClient(cfg["PLEX_URL"], cfg["PLEX_TOKEN"])

                target_lang = cfg.get("TARGET_LANGUAGE", "eng")
                from src.config import normalize_language

                tracks = None
                if plex and file_path:
                    # Try to get audio from Plex (may not be indexed yet)
                    tracks = await plex.get_audio_tracks(file_path)
                    if tracks is not None:
                        for t in tracks:
                            t["language"] = normalize_language(t["language"])
                            t["source"] = "plex"

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

            if plex:
                await plex.close()
        finally:
            await db.close()

        return JSONResponse({"status": "processed", "episodes_updated": updated})

    return JSONResponse({"status": "ok"})


@router.get("/api/health")
async def health_check():
    import os
    from src.scanner.engine import is_scan_running

    settings = get_settings()
    db_path = settings.DB_PATH
    db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0

    db = await get_db(settings.DB_PATH)
    try:
        stats = await models.get_overview_stats(db)
        upgrade_stats = await models.get_upgrade_stats(db)
    finally:
        await db.close()

    return JSONResponse({
        "status": "ok",
        "version": "1.1.0",
        "scanning": is_scan_running(),
        "lastScan": stats.get("last_scan_time"),
        "nextScan": get_next_run_time(),
        "dbSizeBytes": db_size,
        "series": stats.get("total_series", 0),
        "dubbed": stats.get("fully_dubbed", 0),
        "subOnly": stats.get("sub_only", 0),
        "pendingUpgrades": upgrade_stats.get("pending", 0),
        "successfulUpgrades": upgrade_stats.get("success", 0),
    })


@router.get("/logs")
async def logs_page(request: Request):
    return _templates(request).TemplateResponse(request, "logs.html", {})


@router.get("/api/logs")
async def get_logs(request: Request, lines: int = 200, level: str = ""):
    """Return last N lines of the log file."""
    from pathlib import Path
    log_file = Path(__file__).resolve().parent.parent.parent / "data" / "babel.log"
    if not log_file.exists():
        return HTMLResponse('<p class="text-muted">No log file found.</p>')

    with open(log_file) as f:
        all_lines = f.readlines()

    # Filter by level if specified
    if level:
        level_upper = level.upper()
        all_lines = [l for l in all_lines if level_upper in l]

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
        html += f'<div style="color:{color};border-bottom:1px solid rgba(255,255,255,0.03);padding:1px 0;">{line}</div>'
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
