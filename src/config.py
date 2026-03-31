from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": ""}

    SONARR_URL: str
    SONARR_API_KEY: str = ""
    PLEX_URL: str = ""
    PLEX_TOKEN: str = ""
    SCAN_INTERVAL_HOURS: int = 6
    TARGET_LANGUAGE: str = "eng"
    SEARCH_COOLDOWN_DAYS: int = 7
    SEARCH_RATE_LIMIT: int = 5
    SONARR_PATH_PREFIX: str = ""
    LOCAL_PATH_PREFIX: str = "/media"
    PLEX_PATH_PREFIX: str = ""
    ANIME_FILTER: str = "type"
    WEB_PORT: int = 8686
    LOG_LEVEL: str = "INFO"
    DB_PATH: str = "/app/data/babel.db"


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


def translate_path(sonarr_path: str, target: str = "local") -> str:
    settings = get_settings()
    if not settings.SONARR_PATH_PREFIX:
        return sonarr_path
    if not sonarr_path.startswith(settings.SONARR_PATH_PREFIX):
        return sonarr_path
    if target == "plex":
        replacement = settings.PLEX_PATH_PREFIX or settings.LOCAL_PATH_PREFIX
    else:
        replacement = settings.LOCAL_PATH_PREFIX
    return sonarr_path.replace(settings.SONARR_PATH_PREFIX, replacement, 1)


def normalize_language(code: str) -> str:
    code = code.lower()
    if len(code) == 3:
        return code
    return ISO_639_MAP.get(code, code)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


async def get_effective_settings() -> dict:
    """Return settings dict with DB overrides applied on top of env defaults."""
    from src.db.database import get_db
    from src.db.models import get_all_settings

    settings = get_settings()
    result = {}
    for field in settings.model_fields:
        result[field] = getattr(settings, field)

    db = await get_db(settings.DB_PATH)
    try:
        db_settings = await get_all_settings(db)
        for key, value in db_settings.items():
            upper_key = key.upper()
            if upper_key in result:
                # Cast to the same type as the env default
                original = result[upper_key]
                if isinstance(original, int):
                    try:
                        value = int(value)
                    except ValueError:
                        continue
                result[upper_key] = value
    finally:
        await db.close()

    return result
