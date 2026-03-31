"""
Babel SQL schema and async CRUD helpers.

Every helper takes an aiosqlite.Connection as its first argument.
Query helpers return plain dicts (or lists of dicts) for easy JSON serialisation.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import aiosqlite

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS series (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    sonarr_path TEXT,
    plex_rating_key TEXT,
    poster_url TEXT,
    total_episodes INTEGER DEFAULT 0,
    dubbed_count INTEGER DEFAULT 0,
    sub_only_count INTEGER DEFAULT 0,
    unknown_count INTEGER DEFAULT 0,
    dub_status TEXT DEFAULT 'UNKNOWN',
    search_excluded INTEGER DEFAULT 0,
    dub_available TEXT,
    dub_licensors TEXT,
    mal_id INTEGER,
    dub_checked_at TIMESTAMP,
    last_scan_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS episodes (
    id INTEGER PRIMARY KEY,
    series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    season_number INTEGER,
    episode_number INTEGER,
    title TEXT,
    file_path TEXT,
    file_size INTEGER,
    dub_status TEXT DEFAULT 'UNKNOWN',
    last_scan_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audio_tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id INTEGER NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    language TEXT NOT NULL,
    codec TEXT,
    source TEXT NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS search_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id INTEGER NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    trigger_source TEXT DEFAULT 'auto'
);

CREATE INDEX IF NOT EXISTS idx_search_history_cooldown
    ON search_history(episode_id, triggered_at);

CREATE INDEX IF NOT EXISTS idx_episodes_series
    ON episodes(series_id);

CREATE TABLE IF NOT EXISTS scan_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    episodes_checked INTEGER DEFAULT 0,
    searches_triggered INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running',
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ignored_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL DEFAULT 'manual',
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS upgrade_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id INTEGER NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    series_title TEXT,
    season_number INTEGER,
    episode_number INTEGER,
    old_file_size INTEGER,
    new_file_size INTEGER,
    triggered_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    result TEXT DEFAULT 'pending',
    old_status TEXT,
    new_status TEXT,
    attempts INTEGER DEFAULT 1,
    download_status TEXT
);

CREATE INDEX IF NOT EXISTS idx_upgrade_episode ON upgrade_tracking(episode_id, result);

CREATE INDEX IF NOT EXISTS idx_episodes_status ON episodes(series_id, dub_status);

CREATE INDEX IF NOT EXISTS idx_upgrade_result ON upgrade_tracking(result);
"""

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _row_to_dict(row: aiosqlite.Row | None) -> dict | None:
    """Convert an aiosqlite.Row to a plain dict, or return None."""
    if row is None:
        return None
    return dict(row)


def _rows_to_dicts(rows: list[aiosqlite.Row]) -> list[dict]:
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


async def get_setting(db: aiosqlite.Connection, key: str) -> str | None:
    async with db.execute(
        "SELECT value FROM settings WHERE key = ?", (key,)
    ) as cur:
        row = await cur.fetchone()
        return row["value"] if row else None


async def set_setting(db: aiosqlite.Connection, key: str, value: str) -> None:
    await db.execute(
        """INSERT INTO settings (key, value, updated_at)
           VALUES (?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value,
                                          updated_at = excluded.updated_at""",
        (key, value),
    )
    await db.commit()


async def get_all_settings(db: aiosqlite.Connection) -> dict:
    async with db.execute("SELECT key, value FROM settings") as cur:
        rows = await cur.fetchall()
        return {r["key"]: r["value"] for r in rows}


# ---------------------------------------------------------------------------
# Series
# ---------------------------------------------------------------------------


async def upsert_series(
    db: aiosqlite.Connection,
    id: int,
    title: str,
    sonarr_path: str | None = None,
    poster_url: str | None = None,
) -> None:
    await db.execute(
        """INSERT INTO series (id, title, sonarr_path, poster_url)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET title = excluded.title,
                                         sonarr_path = excluded.sonarr_path,
                                         poster_url = excluded.poster_url""",
        (id, title, sonarr_path, poster_url),
    )
    await db.commit()


async def get_all_series(db: aiosqlite.Connection) -> list[dict]:
    async with db.execute(
        "SELECT * FROM series ORDER BY title"
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def get_series_filtered(
    db: aiosqlite.Connection,
    status: str | None = None,
    search: str | None = None,
    page: int = 1,
    per_page: int = 30,
    sort: str | None = None,
) -> tuple[list[dict], int]:
    """Return (series_list, total_count) with optional filtering, sorting, and pagination."""
    conditions: list[str] = []
    params: list[Any] = []

    if status:
        conditions.append("dub_status = ?")
        params.append(status)
    if search:
        conditions.append("title LIKE ?")
        params.append(f"%{search}%")

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    # Total count
    async with db.execute(
        f"SELECT COUNT(*) AS c FROM series{where}", params
    ) as cur:
        row = await cur.fetchone()
        total_count = row["c"] if row else 0

    # Determine sort order
    sort_clauses = {
        "title": "title ASC",
        "title_desc": "title DESC",
        "dub_pct_desc": "CASE WHEN total_episodes > 0 THEN CAST(dubbed_count AS REAL) / total_episodes ELSE 0 END DESC, title ASC",
        "dub_pct_asc": "CASE WHEN total_episodes > 0 THEN CAST(dubbed_count AS REAL) / total_episodes ELSE 0 END ASC, title ASC",
        "sub_only_desc": "sub_only_count DESC, title ASC",
    }
    order_by = sort_clauses.get(sort, "title ASC")

    # Page of results
    params_page = params + [per_page, (page - 1) * per_page]
    async with db.execute(
        f"SELECT * FROM series{where} ORDER BY {order_by} LIMIT ? OFFSET ?",
        params_page,
    ) as cur:
        series_list = _rows_to_dicts(await cur.fetchall())

    return series_list, total_count


async def get_series(db: aiosqlite.Connection, series_id: int) -> dict | None:
    async with db.execute(
        "SELECT * FROM series WHERE id = ?", (series_id,)
    ) as cur:
        return _row_to_dict(await cur.fetchone())


async def update_series_counts(db: aiosqlite.Connection, series_id: int) -> None:
    """Recalculate dubbed/sub_only/unknown counts and dub_status from episodes."""
    async with db.execute(
        """SELECT
               COUNT(*) AS total,
               SUM(CASE WHEN dub_status = 'DUBBED' THEN 1 ELSE 0 END) AS dubbed,
               SUM(CASE WHEN dub_status = 'SUB_ONLY' THEN 1 ELSE 0 END) AS sub_only,
               SUM(CASE WHEN dub_status = 'UNKNOWN' THEN 1 ELSE 0 END) AS unknown,
               SUM(CASE WHEN dub_status = 'MISSING' THEN 1 ELSE 0 END) AS missing
           FROM episodes WHERE series_id = ?""",
        (series_id,),
    ) as cur:
        row = await cur.fetchone()

    total = row["total"] or 0
    dubbed = row["dubbed"] or 0
    sub_only = row["sub_only"] or 0
    unknown = row["unknown"] or 0
    missing = row["missing"] or 0

    # Series with no episodes or all episodes missing have no files at all
    if total == 0:
        dub_status = "EMPTY"
    elif total == missing:
        dub_status = "EMPTY"
    elif dubbed == (total - missing):
        dub_status = "DUBBED"
    elif dubbed > 0:
        dub_status = "PARTIAL"
    elif sub_only == (total - missing):
        dub_status = "SUB_ONLY"
    else:
        dub_status = "UNKNOWN"

    # If we have actual dubbed episodes, override dub_available to "available"
    # regardless of what MAL/Jikan says — the audio tracks don't lie
    dub_available_override = None
    if dubbed > 0:
        async with db.execute(
            "SELECT dub_available FROM series WHERE id = ?", (series_id,)
        ) as cur:
            s_row = await cur.fetchone()
        if s_row and s_row["dub_available"] in (None, "unknown", "unlikely"):
            dub_available_override = "available"

    await db.execute(
        """UPDATE series
           SET total_episodes = ?, dubbed_count = ?, sub_only_count = ?,
               unknown_count = ?, dub_status = ?
           WHERE id = ?""",
        (total, dubbed, sub_only, unknown, dub_status, series_id),
    )
    if dub_available_override:
        await db.execute(
            "UPDATE series SET dub_available = ? WHERE id = ?",
            (dub_available_override, series_id),
        )
    await db.commit()


async def delete_series_not_in(db: aiosqlite.Connection, sonarr_ids: set[int]) -> None:
    """Delete series whose IDs are not in the given set (orphan cleanup)."""
    if not sonarr_ids:
        await db.execute("DELETE FROM series")
    else:
        placeholders = ",".join("?" for _ in sonarr_ids)
        await db.execute(
            f"DELETE FROM series WHERE id NOT IN ({placeholders})",
            tuple(sonarr_ids),
        )
    await db.commit()


# ---------------------------------------------------------------------------
# Episodes
# ---------------------------------------------------------------------------


async def upsert_episode(
    db: aiosqlite.Connection,
    id: int,
    series_id: int,
    season: int | None,
    episode: int | None,
    title: str | None,
    file_path: str | None,
    file_size: int | None,
) -> None:
    await db.execute(
        """INSERT INTO episodes (id, series_id, season_number, episode_number,
                                 title, file_path, file_size)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET series_id = excluded.series_id,
                                         season_number = excluded.season_number,
                                         episode_number = excluded.episode_number,
                                         title = excluded.title,
                                         file_path = excluded.file_path,
                                         file_size = excluded.file_size""",
        (id, series_id, season, episode, title, file_path, file_size),
    )
    await db.commit()


async def update_episode_status(
    db: aiosqlite.Connection, episode_id: int, dub_status: str
) -> None:
    await db.execute(
        """UPDATE episodes
           SET dub_status = ?, last_scan_at = CURRENT_TIMESTAMP
           WHERE id = ?""",
        (dub_status, episode_id),
    )
    await db.commit()


async def get_episode(db: aiosqlite.Connection, episode_id: int) -> dict | None:
    """Fetch a single episode by its Sonarr ID."""
    async with db.execute(
        "SELECT * FROM episodes WHERE id = ?", (episode_id,)
    ) as cur:
        return _row_to_dict(await cur.fetchone())


async def get_episodes_for_series(
    db: aiosqlite.Connection, series_id: int
) -> list[dict]:
    async with db.execute(
        """SELECT * FROM episodes
           WHERE series_id = ?
           ORDER BY season_number, episode_number""",
        (series_id,),
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def delete_episodes_not_in(
    db: aiosqlite.Connection, series_id: int, sonarr_episode_ids: set[int]
) -> None:
    """Delete episodes for a series whose IDs are not in the given set."""
    if not sonarr_episode_ids:
        await db.execute("DELETE FROM episodes WHERE series_id = ?", (series_id,))
    else:
        placeholders = ",".join("?" for _ in sonarr_episode_ids)
        await db.execute(
            f"DELETE FROM episodes WHERE series_id = ? AND id NOT IN ({placeholders})",
            (series_id, *sonarr_episode_ids),
        )
    await db.commit()


# ---------------------------------------------------------------------------
# Audio tracks
# ---------------------------------------------------------------------------


async def replace_audio_tracks(
    db: aiosqlite.Connection, episode_id: int, tracks: list[dict]
) -> None:
    """Delete existing tracks for the episode and insert new ones.

    Each dict in *tracks* must have keys: language, codec, source.
    """
    await db.execute("DELETE FROM audio_tracks WHERE episode_id = ?", (episode_id,))
    for t in tracks:
        await db.execute(
            """INSERT INTO audio_tracks (episode_id, language, codec, source)
               VALUES (?, ?, ?, ?)""",
            (episode_id, t["language"], t.get("codec"), t["source"]),
        )
    await db.commit()


async def get_audio_tracks(
    db: aiosqlite.Connection, episode_id: int
) -> list[dict]:
    async with db.execute(
        "SELECT * FROM audio_tracks WHERE episode_id = ?", (episode_id,)
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def get_audio_tracks_for_series(db: aiosqlite.Connection, series_id: int) -> dict[int, list[dict]]:
    """Get all audio tracks for all episodes in a series, keyed by episode_id."""
    async with db.execute(
        "SELECT at.* FROM audio_tracks at JOIN episodes e ON e.id = at.episode_id WHERE e.series_id = ?",
        (series_id,)
    ) as cur:
        rows = await cur.fetchall()
    result: dict[int, list[dict]] = {}
    for r in rows:
        r = dict(r)
        eid = r["episode_id"]
        if eid not in result:
            result[eid] = []
        result[eid].append(r)
    return result


# ---------------------------------------------------------------------------
# Search history
# ---------------------------------------------------------------------------


async def add_search_record(
    db: aiosqlite.Connection, episode_id: int, trigger_source: str = "auto"
) -> None:
    await db.execute(
        """INSERT INTO search_history (episode_id, trigger_source)
           VALUES (?, ?)""",
        (episode_id, trigger_source),
    )
    await db.commit()


async def get_last_search_time(
    db: aiosqlite.Connection, episode_id: int
) -> datetime | None:
    async with db.execute(
        """SELECT MAX(triggered_at) AS last_triggered
           FROM search_history WHERE episode_id = ?""",
        (episode_id,),
    ) as cur:
        row = await cur.fetchone()
        val = row["last_triggered"] if row else None
        if val is None:
            return None
        if isinstance(val, str):
            dt = datetime.fromisoformat(val)
            if dt.tzinfo is None:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        return val


async def get_last_search_times_for_series(
    db: aiosqlite.Connection, series_id: int
) -> dict[int, datetime | None]:
    """Get last search time per episode for a series."""
    async with db.execute(
        """SELECT sh.episode_id, MAX(sh.triggered_at) as last_search
           FROM search_history sh JOIN episodes e ON e.id = sh.episode_id
           WHERE e.series_id = ? GROUP BY sh.episode_id""",
        (series_id,)
    ) as cur:
        rows = await cur.fetchall()
    result: dict[int, datetime | None] = {}
    for r in rows:
        val = r["last_search"]
        if isinstance(val, str):
            try:
                val = datetime.fromisoformat(val)
            except (ValueError, TypeError):
                pass
        result[r["episode_id"]] = val
    return result


async def get_search_history(
    db: aiosqlite.Connection, limit: int = 100
) -> list[dict]:
    async with db.execute(
        """SELECT sh.id, sh.episode_id, sh.triggered_at, sh.trigger_source,
                  e.title AS episode_title, e.season_number, e.episode_number,
                  s.title AS series_title
           FROM search_history sh
           JOIN episodes e ON e.id = sh.episode_id
           JOIN series s ON s.id = e.series_id
           ORDER BY sh.triggered_at DESC
           LIMIT ?""",
        (limit,),
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


# ---------------------------------------------------------------------------
# Scan log
# ---------------------------------------------------------------------------


async def start_scan_log(db: aiosqlite.Connection) -> int:
    """Insert a new scan_log row and return its ID."""
    async with db.execute(
        """INSERT INTO scan_log (started_at, status)
           VALUES (CURRENT_TIMESTAMP, 'running')"""
    ) as cur:
        scan_id = cur.lastrowid
    await db.commit()
    return scan_id


async def complete_scan_log(
    db: aiosqlite.Connection,
    scan_id: int,
    episodes_checked: int,
    searches_triggered: int,
    errors: int,
    status: str,
    error_message: str | None = None,
) -> None:
    await db.execute(
        """UPDATE scan_log
           SET completed_at = CURRENT_TIMESTAMP,
               episodes_checked = ?,
               searches_triggered = ?,
               errors = ?,
               status = ?,
               error_message = ?
           WHERE id = ?""",
        (episodes_checked, searches_triggered, errors, status, error_message, scan_id),
    )
    await db.commit()


async def get_scan_logs(
    db: aiosqlite.Connection, limit: int = 50
) -> list[dict]:
    async with db.execute(
        "SELECT * FROM scan_log ORDER BY started_at DESC LIMIT ?", (limit,)
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def get_scan_detail(db: aiosqlite.Connection, scan_id: int) -> dict | None:
    """Get detailed breakdown for a specific scan.

    Returns a dict with keys:
        scan: the scan_log row
        series_summary: per-series aggregates (title, dubbed, sub_only, searched)
        searches: individual search records with upgrade status
    Returns None if scan_id not found.
    """
    # 1. Fetch the scan_log row
    async with db.execute(
        "SELECT * FROM scan_log WHERE id = ?", (scan_id,)
    ) as cur:
        scan_row = await cur.fetchone()
    if scan_row is None:
        return None
    scan = dict(scan_row)

    # 2. Find searches triggered during this scan window
    started = scan["started_at"]
    completed = scan["completed_at"] or "9999-12-31"

    async with db.execute(
        """SELECT sh.id AS search_id, sh.episode_id, sh.triggered_at, sh.trigger_source,
                  e.season_number, e.episode_number, e.title AS episode_title,
                  e.dub_status AS current_dub_status,
                  s.id AS series_id, s.title AS series_title,
                  s.dubbed_count, s.sub_only_count
           FROM search_history sh
           JOIN episodes e ON e.id = sh.episode_id
           JOIN series s ON s.id = e.series_id
           WHERE sh.triggered_at >= ? AND sh.triggered_at <= ?
           ORDER BY s.title, e.season_number, e.episode_number""",
        (started, completed),
    ) as cur:
        search_rows = _rows_to_dicts(await cur.fetchall())

    # 3. For each search, look up upgrade_tracking result
    searches = []
    for sr in search_rows:
        async with db.execute(
            """SELECT result FROM upgrade_tracking
               WHERE episode_id = ? AND triggered_at >= ? AND triggered_at <= ?
               ORDER BY triggered_at DESC LIMIT 1""",
            (sr["episode_id"], started, completed),
        ) as cur:
            upgrade_row = await cur.fetchone()

        status = "NOT_FOUND"
        if upgrade_row:
            status = (upgrade_row["result"] or "pending").upper()
        searches.append({
            "series_title": sr["series_title"],
            "season_number": sr["season_number"],
            "episode_number": sr["episode_number"],
            "episode_title": sr["episode_title"],
            "episode_code": f"S{sr['season_number'] or 0:02d}E{sr['episode_number'] or 0:02d}",
            "trigger_source": sr["trigger_source"],
            "status": status,
        })

    # 4. Build per-series summary
    series_map: dict[str, dict] = {}
    for sr in search_rows:
        title = sr["series_title"]
        if title not in series_map:
            series_map[title] = {
                "title": title,
                "dubbed": sr["dubbed_count"] or 0,
                "sub_only": sr["sub_only_count"] or 0,
                "searched": 0,
            }
        series_map[title]["searched"] += 1

    series_summary = sorted(series_map.values(), key=lambda x: x["title"])

    return {
        "scan": scan,
        "series_summary": series_summary,
        "searches": searches,
    }


# ---------------------------------------------------------------------------
# Overview / stats
# ---------------------------------------------------------------------------


async def get_overview_stats(db: aiosqlite.Connection) -> dict:
    """Return high-level dashboard statistics."""
    async with db.execute(
        """SELECT
               COUNT(*) AS total_series,
               SUM(CASE WHEN dub_status = 'DUBBED' THEN 1 ELSE 0 END) AS fully_dubbed,
               SUM(CASE WHEN dub_status = 'PARTIAL' THEN 1 ELSE 0 END) AS partially_dubbed,
               SUM(CASE WHEN dub_status = 'SUB_ONLY' THEN 1 ELSE 0 END) AS sub_only,
               SUM(CASE WHEN dub_status = 'EMPTY' THEN 1 ELSE 0 END) AS empty,
               SUM(CASE WHEN dub_status = 'UNKNOWN' THEN 1 ELSE 0 END) AS unknown
           FROM series"""
    ) as cur:
        row = await cur.fetchone()

    async with db.execute(
        "SELECT MAX(started_at) AS last_scan FROM scan_log"
    ) as cur:
        scan_row = await cur.fetchone()

    return {
        "total_series": row["total_series"] or 0,
        "fully_dubbed": row["fully_dubbed"] or 0,
        "partially_dubbed": row["partially_dubbed"] or 0,
        "sub_only": row["sub_only"] or 0,
        "empty": row["empty"] or 0,
        "unknown": row["unknown"] or 0,
        "last_scan_time": scan_row["last_scan"] if scan_row else None,
    }


# ---------------------------------------------------------------------------
# Ignored paths
# ---------------------------------------------------------------------------


async def add_ignored_path(
    db: aiosqlite.Connection, pattern: str, source: str = "manual", note: str = None
) -> None:
    await db.execute(
        "INSERT OR IGNORE INTO ignored_paths (pattern, source, note) VALUES (?, ?, ?)",
        (pattern, source, note),
    )
    await db.commit()


async def remove_ignored_path(db: aiosqlite.Connection, path_id: int) -> None:
    await db.execute("DELETE FROM ignored_paths WHERE id = ?", (path_id,))
    await db.commit()


async def get_ignored_paths(db: aiosqlite.Connection) -> list[dict]:
    async with db.execute(
        "SELECT * FROM ignored_paths ORDER BY pattern"
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def cleanup_old_records(db: aiosqlite.Connection, days: int = 30) -> dict:
    """Prune old search_history, resolved upgrades, and old scan logs."""
    cutoff = f"-{days} days"
    deleted = {}
    for table, where in [
        ("search_history", "triggered_at < datetime('now', ?)"),
        ("upgrade_tracking", "result != 'pending' AND resolved_at IS NOT NULL AND resolved_at < datetime('now', ?)"),
        ("scan_log", "started_at < datetime('now', ?) AND status != 'running'"),
    ]:
        async with db.execute(f"DELETE FROM {table} WHERE {where}", (cutoff,)) as cur:
            deleted[table] = cur.rowcount
    await db.commit()
    return deleted


async def is_path_ignored(db: aiosqlite.Connection, path: str) -> bool:
    """Check if a path matches any ignored pattern (case-insensitive substring match)."""
    async with db.execute("SELECT pattern FROM ignored_paths") as cur:
        patterns = await cur.fetchall()
    path_lower = path.lower()
    for row in patterns:
        if row["pattern"].lower() in path_lower:
            return True
    return False


# ---------------------------------------------------------------------------
# Upgrade tracking
# ---------------------------------------------------------------------------


async def create_upgrade_record(
    db: aiosqlite.Connection,
    episode_id: int,
    series_title: str,
    season: int,
    episode: int,
    old_file_size: int,
) -> int:
    """Create a pending upgrade record when a search is triggered. Returns ID."""
    async with db.execute(
        """INSERT INTO upgrade_tracking
           (episode_id, series_title, season_number, episode_number,
            old_file_size, triggered_at, result, old_status)
           VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'pending', 'SUB_ONLY')""",
        (episode_id, series_title, season, episode, old_file_size),
    ) as cur:
        rec_id = cur.lastrowid
    await db.commit()
    return rec_id


async def get_pending_upgrades(db: aiosqlite.Connection) -> list[dict]:
    """Get all pending upgrade records (searches triggered, awaiting file change)."""
    async with db.execute(
        "SELECT * FROM upgrade_tracking WHERE result = 'pending' ORDER BY triggered_at DESC"
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def resolve_upgrade(
    db: aiosqlite.Connection,
    episode_id: int,
    new_file_size: int,
    new_status: str,
    result: str,
) -> None:
    """Resolve a pending upgrade: 'success' if dubbed, 'failed' if still sub-only."""
    await db.execute(
        """UPDATE upgrade_tracking
           SET resolved_at = CURRENT_TIMESTAMP,
               new_file_size = ?,
               new_status = ?,
               result = ?
           WHERE episode_id = ? AND result = 'pending'""",
        (new_file_size, new_status, result, episode_id),
    )
    await db.commit()


async def increment_upgrade_attempts(db: aiosqlite.Connection, episode_id: int) -> None:
    """Increment attempts on a failed upgrade and reset to pending for retry."""
    await db.execute(
        """UPDATE upgrade_tracking
           SET result = 'pending',
               resolved_at = NULL,
               triggered_at = CURRENT_TIMESTAMP,
               attempts = attempts + 1
           WHERE episode_id = ? AND result = 'failed'""",
        (episode_id,),
    )
    await db.commit()


async def get_upgrade_history(db: aiosqlite.Connection, limit: int = 100) -> list[dict]:
    """Get all upgrade records with most recent first."""
    async with db.execute(
        "SELECT * FROM upgrade_tracking ORDER BY triggered_at DESC LIMIT ?",
        (limit,),
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def update_download_status(db: aiosqlite.Connection, episode_id: int, download_status: str) -> None:
    """Update the download_status on the pending upgrade record for an episode."""
    await db.execute(
        """UPDATE upgrade_tracking
           SET download_status = ?
           WHERE episode_id = ? AND result = 'pending'""",
        (download_status, episode_id),
    )
    await db.commit()


async def get_recent_resolved_upgrades(db: aiosqlite.Connection, limit: int = 20) -> list[dict]:
    """Get most recently resolved upgrades (success + failed), ordered by resolved_at DESC."""
    async with db.execute(
        """SELECT * FROM upgrade_tracking
           WHERE result IN ('success', 'failed') AND resolved_at IS NOT NULL
           ORDER BY resolved_at DESC LIMIT ?""",
        (limit,),
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def get_pending_episode_ids(db: aiosqlite.Connection) -> set[int]:
    """Get the set of episode IDs that have pending upgrades."""
    async with db.execute(
        "SELECT episode_id FROM upgrade_tracking WHERE result = 'pending'"
    ) as cur:
        rows = await cur.fetchall()
        return {r["episode_id"] for r in rows}


async def get_upgrade_stats(db: aiosqlite.Connection) -> dict:
    """Get upgrade attempt statistics."""
    async with db.execute(
        """SELECT
               COUNT(*) AS total,
               SUM(CASE WHEN result = 'pending' THEN 1 ELSE 0 END) AS pending,
               SUM(CASE WHEN result = 'success' THEN 1 ELSE 0 END) AS success,
               SUM(CASE WHEN result = 'failed' THEN 1 ELSE 0 END) AS failed
           FROM upgrade_tracking"""
    ) as cur:
        row = await cur.fetchone()
    return {
        "total": row["total"] or 0,
        "pending": row["pending"] or 0,
        "success": row["success"] or 0,
        "failed": row["failed"] or 0,
    }


# ---------------------------------------------------------------------------
# Search count helpers
# ---------------------------------------------------------------------------


async def get_search_count(db: aiosqlite.Connection, episode_id: int, days: int = 7) -> int:
    """Count searches for this episode within the last N days.
    Resets naturally over time so episodes get retried for later dub releases."""
    async with db.execute(
        "SELECT COUNT(*) AS cnt FROM search_history WHERE episode_id = ? AND triggered_at > datetime('now', ?)",
        (episode_id, f"-{days} days"),
    ) as cur:
        row = await cur.fetchone()
        return row["cnt"] if row else 0


# ---------------------------------------------------------------------------
# Series search exclusion
# ---------------------------------------------------------------------------


async def set_series_excluded(db: aiosqlite.Connection, series_id: int, excluded: bool) -> None:
    """Set the search_excluded flag on a series."""
    await db.execute(
        "UPDATE series SET search_excluded = ? WHERE id = ?",
        (1 if excluded else 0, series_id),
    )
    await db.commit()


async def is_series_excluded(db: aiosqlite.Connection, series_id: int) -> bool:
    """Check if a series is excluded from dub searches."""
    async with db.execute(
        "SELECT search_excluded FROM series WHERE id = ?", (series_id,)
    ) as cur:
        row = await cur.fetchone()
        return bool(row["search_excluded"]) if row else False


# ---------------------------------------------------------------------------
# Dub availability
# ---------------------------------------------------------------------------


async def update_dub_availability(
    db: aiosqlite.Connection,
    series_id: int,
    dub_status: str,
    licensors: str,
    mal_id: int = None,
) -> None:
    """Update dub availability info from Jikan/MAL lookup."""
    await db.execute(
        "UPDATE series SET dub_available = ?, dub_licensors = ?, mal_id = ?, dub_checked_at = CURRENT_TIMESTAMP WHERE id = ?",
        (dub_status, licensors, mal_id, series_id),
    )
    await db.commit()


async def get_series_needing_dub_lookup(db: aiosqlite.Connection, recheck_days: int = 30) -> list[dict]:
    """Get series that need dub availability lookup.

    Includes:
    - Series never checked (dub_available IS NULL or 'unknown')
    - Series marked 'unlikely' that haven't been checked in recheck_days
      (a licensor might pick them up later)
    """
    async with db.execute(
        """SELECT * FROM series
           WHERE dub_status IN ('SUB_ONLY', 'PARTIAL')
             AND (
               dub_available IS NULL
               OR dub_available = 'unknown'
               OR (dub_available = 'unlikely' AND (
                   dub_checked_at IS NULL
                   OR dub_checked_at < datetime('now', ?)
               ))
             )
           ORDER BY title""",
        (f"-{recheck_days} days",),
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def get_recently_dubbed_series(db: aiosqlite.Connection, days: int = 30) -> list[dict]:
    """Get series that had successful upgrades in the last N days."""
    async with db.execute(
        """SELECT s.*,
                  COUNT(ut.id) AS upgraded_count,
                  MAX(ut.resolved_at) AS last_upgrade_at
           FROM series s
           JOIN upgrade_tracking ut ON ut.series_title = s.title
           WHERE ut.result = 'success'
             AND ut.resolved_at >= datetime('now', ?)
           GROUP BY s.id
           ORDER BY last_upgrade_at DESC""",
        (f"-{days} days",),
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def get_dub_expected_series(db: aiosqlite.Connection) -> list[dict]:
    """Get sub-only series where dub is available or likely."""
    async with db.execute(
        """SELECT * FROM series
           WHERE dub_available IN ('available', 'likely')
             AND dub_status IN ('SUB_ONLY', 'PARTIAL')
           ORDER BY title"""
    ) as cur:
        return _rows_to_dicts(await cur.fetchall())


async def get_no_dub_count(db: aiosqlite.Connection) -> int:
    """Count series where no dub is available."""
    async with db.execute(
        "SELECT COUNT(*) AS c FROM series WHERE dub_available = 'unlikely'"
    ) as cur:
        row = await cur.fetchone()
        return row["c"] if row else 0
