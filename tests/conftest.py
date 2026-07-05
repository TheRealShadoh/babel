import asyncio
import os

from src.db.database import init_db

# Point the app at an isolated per-test DB and keep Sonarr/Plex unconfigured
# at the env level so nothing in the test suite ever makes a real network call.
os.environ.setdefault("SONARR_URL", "")
_ENV_DB_PATH = "/tmp/babel_test_env.db"
os.environ.setdefault("DB_PATH", _ENV_DB_PATH)

# _scan_with_sonarr's post-scan hooks (check_download_status,
# resolve_stuck_imports) read DB_PATH via get_effective_settings() rather
# than the connection passed into the function under test, so this file
# needs a real schema even though the tests themselves use per-test tmp DBs.
asyncio.run(init_db(_ENV_DB_PATH))
