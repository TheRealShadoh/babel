"""
Babel database manager — aiosqlite connection factory and initialization.
"""

from contextlib import asynccontextmanager

import aiosqlite

from src.db.models import SCHEMA_SQL


async def init_db(db_path: str) -> None:
    """Create all tables if they don't exist, enable WAL mode and foreign keys."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.executescript(SCHEMA_SQL)
        # Migrate: add download_status column if missing
        try:
            await db.execute(
                "ALTER TABLE upgrade_tracking ADD COLUMN download_status TEXT"
            )
            await db.commit()
        except Exception:
            pass  # column already exists
        # Migrate: add search_excluded column to series if missing
        try:
            await db.execute(
                "ALTER TABLE series ADD COLUMN search_excluded INTEGER DEFAULT 0"
            )
            await db.commit()
        except Exception:
            pass  # column already exists
        # Migrate: add dub availability columns to series if missing
        for col_def in [
            "dub_available TEXT",
            "dub_licensors TEXT",
            "mal_id INTEGER",
            "dub_checked_at TIMESTAMP",
        ]:
            try:
                await db.execute(f"ALTER TABLE series ADD COLUMN {col_def}")
                await db.commit()
            except Exception:
                pass  # column already exists
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
    return db


@asynccontextmanager
async def get_db_ctx(db_path: str):
    """Async context manager version of get_db — automatically closes the connection."""
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys=ON")
    try:
        yield db
    finally:
        await db.close()
