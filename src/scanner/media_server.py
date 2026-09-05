"""Media-server selection and fan-out.

Babel can read audio tracks from Plex, from Jellyfin, or from both at once.
Plex and Jellyfin are configured independently — each has its own URL,
credential and path prefix — and `MEDIA_SERVER` only decides which of the
configured ones a scan is allowed to use.

When more than one is in play, `MediaServerGroup` presents them to the scan
engine as a single client: a lookup tries each server in turn (with that
server's own path translation) and the first one that knows the file wins, so
a library only half-indexed by one server is covered by the other.
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath

from src.config import normalize_language, translate_path

logger = logging.getLogger(__name__)

PLEX = "plex"
JELLYFIN = "jellyfin"

_LABELS = {PLEX: "Plex", JELLYFIN: "Jellyfin"}


def server_label(kind: str) -> str:
    return _LABELS.get(kind, "media server")


def configured_servers(cfg: dict) -> list[str]:
    """Which media servers have both a URL and a credential configured."""
    servers = []
    if cfg.get("PLEX_URL") and cfg.get("PLEX_TOKEN"):
        servers.append(PLEX)
    if cfg.get("JELLYFIN_URL") and cfg.get("JELLYFIN_API_KEY"):
        servers.append(JELLYFIN)
    return servers


def select_media_servers(cfg: dict) -> list[str]:
    """Resolve MEDIA_SERVER against what is actually configured.

    "auto" (the default) uses every configured server; "plex"/"jellyfin"
    restrict a scan to one of them even when both are set up; "none" turns
    the media server off entirely and leaves detection to ffprobe.
    """
    preference = str(cfg.get("MEDIA_SERVER", "auto") or "auto").strip().lower()
    available = configured_servers(cfg)

    if preference == "none":
        return []
    if preference in (PLEX, JELLYFIN):
        if preference in available:
            return [preference]
        logger.warning(
            "MEDIA_SERVER is set to '%s' but it is not configured — "
            "falling back to ffprobe-only audio detection.", preference,
        )
        return []
    if preference not in ("auto", "both", ""):
        logger.warning("Unknown MEDIA_SERVER value %r — treating it as 'auto'", preference)

    return available


def build_client(kind: str, cfg: dict):
    """Construct one media-server client."""
    if kind == PLEX:
        from src.scanner.plex import PlexClient
        return PlexClient(cfg["PLEX_URL"], cfg["PLEX_TOKEN"])
    if kind == JELLYFIN:
        from src.scanner.jellyfin import JellyfinClient
        return JellyfinClient(cfg["JELLYFIN_URL"], cfg["JELLYFIN_API_KEY"])
    raise ValueError(f"Unknown media server kind: {kind!r}")


def create_media_clients(cfg: dict) -> list[tuple[object, str]]:
    """Build a client for each selected media server, in preference order."""
    return [(build_client(kind, cfg), kind) for kind in select_media_servers(cfg)]


def create_media_group(cfg: dict) -> "MediaServerGroup | None":
    """Build the group of media servers a scan should use, or None."""
    members = create_media_clients(cfg)
    if not members:
        return None
    return MediaServerGroup(members, cfg)


def client_label(client) -> str:
    """Human-readable name for whatever media client the engine was handed."""
    label = getattr(client, "label", None)
    if label:
        return label
    return server_label(getattr(client, "path_target", PLEX))


class _FanoutEvent:
    """Cancellation flag that mirrors itself onto every member client."""

    def __init__(self, events):
        self._events = list(events)

    def set(self) -> None:
        for event in self._events:
            event.set()

    def is_set(self) -> bool:
        return any(event.is_set() for event in self._events)


def _norm(path: str) -> str:
    return str(PurePosixPath((path or "").replace("\\", "/"))).lower()


class MediaServerGroup:
    """One or more media-server clients behind the single-client interface.

    Path translation happens here rather than in the engine: each member sees
    the library at its own mount point, so the same Sonarr path has to be
    rewritten differently per server. `path_target = None` tells the engine
    to hand over the untranslated Sonarr path and let the group do it.
    """

    path_target = None

    def __init__(self, members: list[tuple[object, str]], cfg: dict):
        self.members = list(members)
        self.cfg = cfg
        self._cancel = _FanoutEvent([m._cancel for m, _ in self.members])

    # -- identity ------------------------------------------------------

    @property
    def kinds(self) -> list[str]:
        return [kind for _, kind in self.members]

    @property
    def label(self) -> str:
        return " + ".join(server_label(kind) for kind in self.kinds) or "media server"

    def __len__(self) -> int:
        return len(self.members)

    @property
    def partial(self) -> bool:
        """True if any member's last library read dropped something."""
        return any(getattr(client, "partial", False) for client, _ in self.members)

    # -- connection ----------------------------------------------------

    async def test_connection(self) -> tuple[bool, str]:
        """Test every member and drop the ones that do not answer.

        A scan with Plex up and Jellyfin down should run against Plex rather
        than fail, so an unreachable member is removed here instead of
        breaking every later lookup.
        """
        reachable: list[tuple[object, str]] = []
        messages: list[str] = []
        for client, kind in self.members:
            try:
                ok, message = await client.test_connection()
            except Exception as e:  # a client that cannot even be built
                ok, message = False, str(e)
            if ok:
                reachable.append((client, kind))
                messages.append(message)
            else:
                logger.warning("%s unreachable: %s", server_label(kind), message)
                try:
                    await client.close()
                except Exception:
                    logger.debug("Closing unreachable %s client failed", kind, exc_info=True)

        self.members = reachable
        self._cancel = _FanoutEvent([m._cancel for m, _ in self.members])
        if not reachable:
            return False, "No media server is reachable"
        return True, "; ".join(messages)

    # -- index ---------------------------------------------------------

    def is_indexed(self) -> bool:
        return bool(self.members) and all(client.is_indexed() for client, _ in self.members)

    async def build_index(self, ignored_patterns: list[str] | None = None) -> int:
        total = 0
        for client, kind in self.members:
            try:
                total += await client.build_index(ignored_patterns=ignored_patterns)
            except Exception:
                logger.warning("%s index build failed", server_label(kind), exc_info=True)
        return total

    async def get_audio_tracks(self, sonarr_path: str) -> list[dict] | None:
        """Ask each server about *sonarr_path*, translated for that server.

        Returns None only when no server knows the file at all, which is what
        sends the engine on to its ffprobe fallback.
        """
        empty_hit = False
        for client, kind in self.members:
            path = translate_path(sonarr_path, kind, self.cfg)
            try:
                tracks = await client.get_audio_tracks(path)
            except Exception:
                logger.warning("%s audio lookup failed for %s", server_label(kind), path, exc_info=True)
                continue
            if tracks is None:
                continue
            if not tracks:
                # The server has the file but reports no audio streams. Another
                # server may have indexed it properly, so keep looking and only
                # fall back to this answer if nobody does better.
                empty_hit = True
                continue
            for track in tracks:
                track["language"] = normalize_language(track["language"])
                track["source"] = kind
            return tracks
        return [] if empty_hit else None

    # -- library -------------------------------------------------------

    async def get_library_data(self, target_lang: str = "eng") -> list[dict]:
        """Merge every server's view of the library into one list.

        Shows are matched by title and episodes by file path, so a library
        both servers can see is reported once rather than twice.
        """
        merged: dict[str, dict] = {}
        seen_paths: dict[str, set[str]] = {}

        for client, kind in self.members:
            try:
                shows = await client.get_library_data(target_lang)
            except Exception:
                logger.warning("%s library read failed", server_label(kind), exc_info=True)
                continue
            for show in shows:
                key = (show.get("title") or "").strip().lower()
                if not key:
                    continue
                existing = merged.get(key)
                if existing is None:
                    merged[key] = show
                    seen_paths[key] = {_norm(ep["file_path"]) for ep in show["episodes"]}
                    continue
                for episode in show["episodes"]:
                    path = _norm(episode["file_path"])
                    if path in seen_paths[key]:
                        continue
                    seen_paths[key].add(path)
                    existing["episodes"].append(episode)

        return list(merged.values())

    # -- collections ---------------------------------------------------

    async def sync_collections(self, series_data: list[dict], changed: bool = True) -> dict:
        updated = 0
        skipped = True
        for client, kind in self.members:
            try:
                result = await client.sync_collections(series_data, changed=changed)
            except Exception:
                logger.warning("%s collection sync failed", server_label(kind), exc_info=True)
                continue
            updated += result.get("collections_updated", 0)
            skipped = skipped and result.get("skipped", False)
        return {"collections_updated": updated, "skipped": skipped}

    # -- misc ----------------------------------------------------------

    async def get_libraries(self) -> list[dict]:
        """Every member's libraries, each tagged with the server it came from."""
        libraries: list[dict] = []
        for client, kind in self.members:
            try:
                for library in await client.get_libraries():
                    libraries.append({**library, "server": server_label(kind)})
            except Exception:
                logger.warning("%s library listing failed", server_label(kind), exc_info=True)
        return libraries

    def get_sample_paths(self, count: int = 5) -> list[str]:
        samples: list[str] = []
        for client, _ in self.members:
            samples.extend(client.get_sample_paths(count))
        return samples[:count]

    def get_index_progress(self) -> dict:
        """Progress of whichever member is still working, else the last one."""
        progress = {"current": 0, "total": 0, "section": ""}
        for client, kind in self.members:
            member = client.get_index_progress()
            progress = {**member, "section": f"{server_label(kind)}: {member.get('section', '')}".strip()}
            if member.get("total", 0) and member["current"] < member["total"]:
                break
        return progress

    def get_match_stats(self) -> dict:
        stats = {"hit_path": 0, "hit_name": 0, "miss": 0}
        for client, _ in self.members:
            member = client.get_match_stats()
            for key in stats:
                stats[key] += member.get(key, 0)
        return stats

    async def close(self) -> None:
        for client, kind in self.members:
            try:
                await client.close()
            except Exception:
                logger.debug("Closing %s client failed", kind, exc_info=True)
