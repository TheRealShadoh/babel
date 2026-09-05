"""Media-server selection.

Babel can read audio tracks from Plex or Jellyfin. Both clients expose the
same surface to the scan engine, so the only thing that varies is which one
gets built — that decision lives here rather than being repeated at every
call site.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

PLEX = "plex"
JELLYFIN = "jellyfin"
NONE = "none"


def configured_servers(cfg: dict) -> list[str]:
    """Which media servers have both a URL and a credential configured."""
    servers = []
    if cfg.get("PLEX_URL") and cfg.get("PLEX_TOKEN"):
        servers.append(PLEX)
    if cfg.get("JELLYFIN_URL") and cfg.get("JELLYFIN_API_KEY"):
        servers.append(JELLYFIN)
    return servers


def select_media_server(cfg: dict) -> str:
    """Resolve MEDIA_SERVER to a concrete choice.

    "auto" (the default) uses whichever one is configured, preferring Plex
    when both are — that keeps existing installs on exactly the server they
    were already using after they add Jellyfin credentials.
    """
    preference = str(cfg.get("MEDIA_SERVER", "auto") or "auto").strip().lower()
    available = configured_servers(cfg)

    if preference == NONE:
        return NONE
    if preference in (PLEX, JELLYFIN):
        if preference in available:
            return preference
        logger.warning(
            "MEDIA_SERVER is set to '%s' but it is not configured — "
            "falling back to ffprobe-only audio detection.", preference,
        )
        return NONE
    if preference not in ("auto", ""):
        logger.warning("Unknown MEDIA_SERVER value %r — treating it as 'auto'", preference)

    return available[0] if available else NONE


def create_media_client(cfg: dict):
    """Build the configured media-server client, or return (None, "none")."""
    choice = select_media_server(cfg)
    if choice == PLEX:
        from src.scanner.plex import PlexClient
        return PlexClient(cfg["PLEX_URL"], cfg["PLEX_TOKEN"]), PLEX
    if choice == JELLYFIN:
        from src.scanner.jellyfin import JellyfinClient
        return JellyfinClient(cfg["JELLYFIN_URL"], cfg["JELLYFIN_API_KEY"]), JELLYFIN
    return None, NONE


def server_label(kind: str) -> str:
    return {PLEX: "Plex", JELLYFIN: "Jellyfin"}.get(kind, "media server")
