"""
Babel database manager — aiosqlite connection factory and initialization.
"""

from contextlib import asynccontextmanager

import aiosqlite

from src.db.models import SCHEMA_SQL, POST_MIGRATION_INDEX_SQL


async def _table_columns(db: aiosqlite.Connection, table: str) -> set[str]:
    async with db.execute(f"PRAGMA table_info({table})") as cur:
        rows = await cur.fetchall()
    return {row[1] for row in rows}


async def _add_column_if_missing(
    db: aiosqlite.Connection, table: str, column: str, col_type: str
) -> bool:
    """Add *column* to *table* if not already present. Returns True if added."""
    existing = await _table_columns(db, table)
    if column in existing:
        return False
    await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    await db.commit()
    return True


async def init_db(db_path: str) -> None:
    """Create all tables if they don't exist, enable WAL mode and foreign keys."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.executescript(SCHEMA_SQL)

        await _add_column_if_missing(db, "upgrade_tracking", "download_status", "TEXT")
        await _add_column_if_missing(db, "series", "missing_count", "INTEGER DEFAULT 0")
        await _add_column_if_missing(db, "scan_log", "kind", "TEXT DEFAULT 'scan'")
        await _add_column_if_missing(db, "scan_log", "episodes_seen", "INTEGER DEFAULT 0")
        await _add_column_if_missing(db, "scan_log", "undetermined", "INTEGER DEFAULT 0")
        await _add_column_if_missing(db, "series", "search_excluded", "INTEGER DEFAULT 0")
        await _add_column_if_missing(db, "series", "title_slug", "TEXT")
        await _add_column_if_missing(db, "search_history", "scan_id", "INTEGER")
        await _add_column_if_missing(db, "upgrade_tracking", "scan_id", "INTEGER")
        for col_name, col_type in [
            ("dub_available", "TEXT"),
            ("dub_licensors", "TEXT"),
            ("mal_id", "INTEGER"),
            ("dub_checked_at", "TIMESTAMP"),
        ]:
            await _add_column_if_missing(db, "series", col_name, col_type)

        added_series_id = await _add_column_if_missing(db, "upgrade_tracking", "series_id", "INTEGER")
        if added_series_id:
            # Backfill from the old title-based join for existing rows.
            await db.execute(
                """UPDATE upgrade_tracking
                   SET series_id = (
                       SELECT s.id FROM series s WHERE s.title = upgrade_tracking.series_title
                   )
                   WHERE series_id IS NULL"""
            )
            await db.commit()

        # Indexes on migration-added columns — must run after the ALTER
        # TABLE calls above have actually added those columns.
        await db.executescript(POST_MIGRATION_INDEX_SQL)

        # Backfill kind for rows written before the column existed. Dub-lookup
        # runs are identifiable by the summary they stored in error_message.
        await db.execute(
            """UPDATE scan_log SET kind = 'dub_lookup'
               WHERE error_message LIKE 'Dub lookup%'"""
        )
        await db.execute("UPDATE scan_log SET kind = 'scan' WHERE kind IS NULL")

        # Clean up stale 'running' scans from previous container restarts
        await db.execute(
            """UPDATE scan_log SET status = 'interrupted', error_message = 'Container restarted'
               WHERE status = 'running'"""
        )
        await db.commit()


async def get_db(db_path: str) -> aiosqlite.Connection:
    """Return an aiosqlite connection. Caller is responsible for closing it.

    Enables foreign keys and sets row_factory to aiosqlite.Row for
    dict-like access on every connection.
    """
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute("PRAGMA busy_timeout=5000")
    return db


@asynccontextmanager
async def get_db_ctx(db_path: str):
    """Async context manager version of get_db — automatically closes the connection."""
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys=ON")
    await db.execute("PRAGMA busy_timeout=5000")
    try:
        yield db
    finally:
        await db.close()
