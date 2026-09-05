import logging
import time
from pathlib import Path
from functools import lru_cache

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    model_config = {"env_prefix": ""}

    SONARR_URL: str = ""
    SONARR_API_KEY: str = ""
    PLEX_URL: str = ""
    PLEX_TOKEN: str = ""
    JELLYFIN_URL: str = ""
    JELLYFIN_API_KEY: str = ""
    # "auto" picks whichever of Plex/Jellyfin is configured (Plex first when
    # both are); "plex"/"jellyfin" pin one; "none" disables the media server
    # and leaves audio detection to ffprobe.
    MEDIA_SERVER: str = "auto"
    SCAN_INTERVAL_HOURS: int = 6
    TARGET_LANGUAGE: str = "eng"
    SEARCH_COOLDOWN_DAYS: int = 7
    SEARCH_RATE_LIMIT: int = 5
    SONARR_PATH_PREFIX: str = ""
    LOCAL_PATH_PREFIX: str = "/media"
    PLEX_PATH_PREFIX: str = ""
    JELLYFIN_PATH_PREFIX: str = ""
    ANIME_FILTER: str = "type"
    LOG_LEVEL: str = "INFO"
    DB_PATH: str = "/app/data/babel.db"
    WEBHOOK_SECRET: str = ""
    AUTH_USERNAME: str = ""
    AUTH_PASSWORD: str = ""
    AUTH_PASSWORD_HASH: str = ""
    SHOW_THUMBNAILS: str = "true"
    MAX_SEARCH_ATTEMPTS: int = 3
    AUTO_TAG_SONARR: str = "true"
    DISCORD_WEBHOOK_URL: str = ""
    AUTO_COLLECTIONS_PLEX: str = "true"
    AUTO_RESOLVE_IMPORTS: str = "true"
    # On by default, like Babel's other automations. It only ever *adds*
    # monitoring, and only to sub-only episodes of series Babel is already
    # searching for a dub — the same episodes it asks Sonarr to search. Turn
    # it off if you keep episodes deliberately unmonitored.
    AUTO_MONITOR_DUBS: str = "true"
    # Ask Anime News Network about the titles MyAnimeList cannot settle. Two
    # extra requests per unsettled title, paced at ANN's ~1/second guidance.
    DUB_LOOKUP_ANN: str = "true"
    STUCK_IMPORT_DRY_RUN: str = "false"

    # --- ffprobe / hung-mount hardening -------------------------------------
    # Media paths may live on a network share or ZFS pool that can stall
    # indefinitely. Every one of these bounds an unbounded wait.
    FFPROBE_TIMEOUT: float = 30.0          # per-probe wall clock
    FFPROBE_KILL_GRACE: float = 5.0        # wait after SIGTERM/SIGKILL before escalating
    FFPROBE_MAX_CONCURRENT: int = 4        # caps processes stuck on a hung mount
    FFPROBE_SLOT_TIMEOUT: float = 15.0     # give up rather than queue behind a hung mount

    # --- event-loop watchdog -------------------------------------------------
    # A blocked event loop stops serving HTTP *and*, under uvloop, stops reaping
    # child processes. Docker reports the container unhealthy but will not
    # restart it, so the process has to notice and exit on its own.
    WATCHDOG_INTERVAL: float = 1.0
    WATCHDOG_UNHEALTHY_LAG: float = 15.0   # /api/health starts returning 503
    WATCHDOG_ABORT_LAG: float = 300.0      # hard-exit so the restart policy kicks in; 0 disables


ISO_639_MAP: dict[str, str] = {
    "en": "eng",
    "ja": "jpn",
    "es": "spa",
    "fr": "fra",
    "de": "deu",
    "pt": "por",
    "it": "ita",
    "ko": "kor",
    "zh": "zho",
    "ru": "rus",
    "ar": "ara",
    "hi": "hin",
}


def _has_path_prefix(path: str, prefix: str) -> bool:
    """True if *prefix* is a leading path component of *path*.

    A plain startswith would let "/tv" rewrite "/tv-anime/…" and "/data"
    rewrite "/database/…", producing paths that do not exist.
    """
    return path == prefix or path.startswith(prefix + "/")


def translate_path(sonarr_path: str, target: str, cfg: dict) -> str:
    """Rewrite a Sonarr-reported path to the equivalent local or Plex path.

    *target* is "local", "plex" or "jellyfin"; *cfg* is an effective-settings
    dict (see get_effective_settings) so that DB-configured path prefixes take
    effect without a restart.
    """
    prefix = (cfg.get("SONARR_PATH_PREFIX", "") or "").rstrip("/")
    if not prefix or not _has_path_prefix(sonarr_path, prefix):
        return sonarr_path
    if target == "plex":
        replacement = cfg.get("PLEX_PATH_PREFIX", "") or cfg.get("LOCAL_PATH_PREFIX", "/media")
    elif target == "jellyfin":
        # Jellyfin usually sees the same paths as Plex when both run on the
        # same host, so its own prefix is optional and falls back to Plex's.
        replacement = (
            cfg.get("JELLYFIN_PATH_PREFIX", "")
            or cfg.get("PLEX_PATH_PREFIX", "")
            or cfg.get("LOCAL_PATH_PREFIX", "/media")
        )
    else:
        replacement = cfg.get("LOCAL_PATH_PREFIX", "/media")
    return replacement.rstrip("/") + sonarr_path[len(prefix):]


def log_file_path(db_path: str) -> Path:
    """The application log lives next to the database.

    Deriving it from DB_PATH keeps both inside whatever directory the operator
    mounted; a fixed /app/data would lose the log for anyone who moved the DB.
    """
    return Path(db_path).resolve().parent / "babel.log"


def cfg_bool(value, default: bool = True) -> bool:
    """Interpret a settings value as a bool.

    Settings booleans are stored as the strings "true"/"false" (env vars and
    the settings DB table are both plain text), so every call site used to
    repeat its own `cfg.get("X", "true") != "false"` string comparison.
    Centralizing it here means a missing/None value falls back to *default*
    consistently instead of each site re-deriving that behavior.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() != "false"


def normalize_language(code: str) -> str:
    code = code.lower()
    if len(code) == 3:
        return code
    return ISO_639_MAP.get(code, code)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


_effective_settings_cache: dict | None = None
_effective_settings_cache_at: float = 0.0
_EFFECTIVE_SETTINGS_TTL = 5.0  # seconds


def invalidate_effective_settings_cache() -> None:
    """Force the next get_effective_settings() call to re-read the DB.

    Called after Settings are saved so changes apply immediately instead of
    waiting out the TTL.
    """
    global _effective_settings_cache
    _effective_settings_cache = None


async def get_effective_settings() -> dict:
    """Return settings dict with DB overrides applied on top of env defaults.

    Result is cached for a few seconds — this is called on every request
    (including frequent activity/scan-progress polling), and re-opening a
    SQLite connection just to read the settings table each time is wasteful.
    """
    global _effective_settings_cache, _effective_settings_cache_at

    now = time.monotonic()
    if (
        _effective_settings_cache is not None
        and (now - _effective_settings_cache_at) < _EFFECTIVE_SETTINGS_TTL
    ):
        return _effective_settings_cache

    from src.db.database import get_db
    from src.db.models import get_all_settings

    settings = get_settings()
    result = {}
    for field in type(settings).model_fields:
        result[field] = getattr(settings, field)

    db = await get_db(settings.DB_PATH)
    try:
        db_settings = await get_all_settings(db)
        for key, value in db_settings.items():
            upper_key = key.upper()
            if upper_key not in result:
                continue
            # Cast to the same type as the env default. Both int and float
            # matter: the ffprobe and watchdog timeouts are floats, and a
            # string reaching them fails at the comparison, not at load.
            original = result[upper_key]
            caster = None
            if isinstance(original, bool):
                caster = None
            elif isinstance(original, int):
                caster = int
            elif isinstance(original, float):
                caster = float
            if caster is not None:
                try:
                    value = caster(value)
                except (TypeError, ValueError):
                    logger.warning(
                        "Ignoring unusable value for %s (%r is not a %s); "
                        "keeping %r", upper_key, value, caster.__name__, original,
                    )
                    continue
            result[upper_key] = value
    finally:
        await db.close()

    _effective_settings_cache = result
    _effective_settings_cache_at = now
    return result
