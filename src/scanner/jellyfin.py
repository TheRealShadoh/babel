"""Jellyfin media-server client.

Mirrors the surface PlexClient exposes to the scan engine — connection test,
library listing, an audio-track index keyed by file path, a full library read
for media-server-only mode, and collection sync — so either server can be
dropped into the same scan without the engine caring which one it got.

Jellyfin's API is plain HTTP/JSON, so unlike the Plex client (which wraps the
blocking PlexAPI library in threads) everything here is natively async.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from pathlib import PurePosixPath

import httpx

logger = logging.getLogger(__name__)

# Jellyfin paginates /Items. Large enough that a 10k-episode library is a
# handful of requests, small enough that one response stays a sane size.
_PAGE_SIZE = 500

_EPISODE_FIELDS = "Path,MediaSources,ParentId"
_SERIES_FIELDS = "Path"


def stable_key(item_id: str) -> int:
    """Map a Jellyfin GUID to a stable positive integer.

    The engine keys media-server-only rows by a numeric ID (it negates them to
    keep them clear of Sonarr's). Jellyfin identifies items by GUID, so it
    needs a deterministic number: the same GUID must produce the same key on
    every scan or every pass would look like a brand new library.
    """
    digest = hashlib.blake2b(str(item_id).encode("utf-8"), digest_size=6).digest()
    return int.from_bytes(digest, "big")


class JellyfinClient:
    # Which path prefix the engine should translate Sonarr paths into before
    # asking this client about a file.
    path_target = "jellyfin"

    def __init__(self, url: str, api_key: str):
        self.url = url.rstrip("/")
        self.api_key = api_key
        self.client = httpx.AsyncClient(
            base_url=self.url,
            headers={
                "X-Emby-Token": api_key,
                "Authorization": f'MediaBrowser Token="{api_key}"',
                "Accept": "application/json",
            },
            timeout=30.0,
        )
        self._path_index: dict[str, list[dict]] | None = None
        self._name_index: dict[str, list[dict]] | None = None
        self._stats = {"hit_path": 0, "hit_name": 0, "miss": 0}
        # The engine cancels a running scan through this event (it is a
        # threading.Event on the Plex client, which runs in worker threads);
        # the same object type works fine for polling from async code.
        self._cancel = threading.Event()
        self._index_progress = {"current": 0, "total": 0, "section": ""}
        self._user_id: str | None = None
        self._user_id_resolved = False
        # Set when a paginated read stopped early; see PlexClient.partial.
        self.partial = False

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def test_connection(self) -> tuple[bool, str]:
        try:
            resp = await self.client.get("/System/Info")
            resp.raise_for_status()
            info = resp.json()
            name = info.get("ServerName") or "Jellyfin"
            version = info.get("Version", "unknown")
            return True, f"Connected to {name} (Jellyfin v{version})"
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                return False, "Jellyfin rejected the API key"
            return False, f"Jellyfin returned {e.response.status_code}"
        except httpx.HTTPError as e:
            return False, f"Cannot reach Jellyfin: {e}"

    async def _resolve_user_id(self) -> str | None:
        """Find a user to query items as.

        Jellyfin 10.9+ serves /Items to an API key with no user context, but
        older servers return an empty set unless a userId is supplied. Asking
        once and reusing the answer keeps both working.
        """
        if self._user_id_resolved:
            return self._user_id
        self._user_id_resolved = True
        try:
            resp = await self.client.get("/Users")
            resp.raise_for_status()
            users = resp.json()
        except httpx.HTTPError as e:
            logger.debug("Could not list Jellyfin users (%s); querying without one", e)
            return None
        admins = [u for u in users if u.get("Policy", {}).get("IsAdministrator")]
        chosen = (admins or users or [None])[0]
        if chosen:
            self._user_id = chosen.get("Id")
        return self._user_id

    async def _get_items(self, params: dict) -> list[dict]:
        """Fetch every page of /Items for *params*."""
        user_id = await self._resolve_user_id()
        items: list[dict] = []
        start = 0
        while True:
            if self._cancel.is_set():
                logger.info("Jellyfin item fetch cancelled after %d items", len(items))
                break
            page_params = dict(params, startIndex=start, limit=_PAGE_SIZE)
            if user_id:
                page_params["userId"] = user_id
            try:
                resp = await self.client.get("/Items", params=page_params)
                resp.raise_for_status()
                data = resp.json()
            except httpx.HTTPError as e:
                logger.error("Failed to fetch Jellyfin items (startIndex=%d): %s", start, e)
                self.partial = True
                break
            page = data.get("Items", [])
            items.extend(page)
            total = data.get("TotalRecordCount", len(items))
            self._index_progress["current"] = len(items)
            self._index_progress["total"] = total
            if not page or len(items) >= total:
                break
            start += len(page)
        return items

    # ------------------------------------------------------------------
    # Libraries
    # ------------------------------------------------------------------

    async def get_libraries(self) -> list[dict]:
        """Return TV libraries, one entry per configured location.

        Shaped like PlexClient.get_libraries so the settings UI can render
        either server's libraries with the same code.
        """
        try:
            resp = await self.client.get("/Library/VirtualFolders")
            resp.raise_for_status()
            folders = resp.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch Jellyfin libraries: %s", e)
            return []

        results: list[dict] = []
        for folder in folders:
            if folder.get("CollectionType") not in ("tvshows", None):
                continue
            locations = folder.get("Locations") or []
            count = await self._count_episodes(folder.get("ItemId"))
            if not locations:
                results.append({
                    "id": folder.get("ItemId", ""),
                    "title": folder.get("Name", ""),
                    "type": "show",
                    "path": "",
                    "count": count,
                })
            for loc in locations:
                results.append({
                    "id": folder.get("ItemId", ""),
                    "title": folder.get("Name", ""),
                    "type": "show",
                    "path": str(loc),
                    "count": count,
                })
        return results

    async def _count_episodes(self, parent_id: str | None) -> int:
        if not parent_id:
            return 0
        params = {
            "recursive": "true",
            "includeItemTypes": "Episode",
            "parentId": parent_id,
            "limit": 0,
        }
        user_id = await self._resolve_user_id()
        if user_id:
            params["userId"] = user_id
        try:
            resp = await self.client.get("/Items", params=params)
            resp.raise_for_status()
            return resp.json().get("TotalRecordCount", 0)
        except httpx.HTTPError:
            return 0

    # ------------------------------------------------------------------
    # Audio track index
    # ------------------------------------------------------------------

    def is_indexed(self) -> bool:
        return self._path_index is not None

    async def build_index(self, ignored_patterns: list[str] | None = None) -> int:
        """Index every episode's audio tracks by file path and by filename."""
        ignored = [p.lower() for p in (ignored_patterns or [])]
        self._path_index = {}
        self._name_index = {}
        self._index_progress = {"current": 0, "total": 0, "section": "Jellyfin"}

        episodes = await self._get_items({
            "recursive": "true",
            "includeItemTypes": "Episode",
            "fields": _EPISODE_FIELDS,
            "enableImages": "false",
        })

        count = 0
        skipped = 0
        for item in episodes:
            path = item.get("Path") or ""
            if not path:
                skipped += 1
                continue
            if ignored and any(pat in path.lower() for pat in ignored):
                continue
            tracks = self._extract_audio(item)
            norm_path = self._normalize_path(path)
            self._path_index[norm_path] = tracks
            self._name_index[norm_path.rsplit("/", 1)[-1]] = tracks
            count += 1

        if skipped:
            logger.warning("Skipped %d Jellyfin episodes with no file path", skipped)
        logger.info("Jellyfin index built: %d episode files indexed", count)
        return count

    async def get_audio_tracks(self, file_path: str) -> list[dict] | None:
        if self._path_index is None:
            await self.build_index()

        normalized = self._normalize_path(file_path)
        tracks = self._path_index.get(normalized)
        if tracks is not None:
            self._stats["hit_path"] += 1
            return [dict(t) for t in tracks]

        filename = normalized.rsplit("/", 1)[-1]
        tracks = self._name_index.get(filename)
        if tracks is not None:
            self._stats["hit_name"] += 1
            return [dict(t) for t in tracks]

        self._stats["miss"] += 1
        return None

    # ------------------------------------------------------------------
    # Full library read (media-server-only mode)
    # ------------------------------------------------------------------

    async def get_library_data(self, target_lang: str = "eng") -> list[dict]:
        """Return every show with its episodes and audio tracks.

        The dicts use the same keys the Plex client returns (including
        "plex_key", which the engine treats as an opaque numeric ID) so
        media-server-only scans work identically against either server.
        """
        from src.config import normalize_language

        try:
            series_items = await self._get_items({
                "recursive": "true",
                "includeItemTypes": "Series",
                "fields": _SERIES_FIELDS,
            })
            episode_items = await self._get_items({
                "recursive": "true",
                "includeItemTypes": "Episode",
                "fields": _EPISODE_FIELDS,
                "enableImages": "false",
            })
        except Exception as e:
            logger.error("Failed to scan Jellyfin library: %s", e)
            return []

        by_series: dict[str, dict] = {}
        for s in series_items:
            sid = s.get("Id")
            if not sid:
                continue
            by_series[sid] = {
                "title": s.get("Name", ""),
                "plex_key": stable_key(sid),
                "path": s.get("Path") or "",
                "poster_url": self._poster_url(s),
                "episodes": [],
            }

        orphans = 0
        for ep in episode_items:
            series_entry = by_series.get(ep.get("SeriesId"))
            if series_entry is None:
                orphans += 1
                continue
            path = ep.get("Path") or ""
            if not path:
                continue
            tracks = [
                {
                    "language": normalize_language(t["language"]),
                    "codec": t["codec"],
                    "source": "jellyfin",
                }
                for t in self._extract_audio(ep)
            ]
            languages = {t["language"] for t in tracks}
            if target_lang in languages:
                dub_status = "DUBBED"
            elif tracks:
                dub_status = "SUB_ONLY"
            else:
                dub_status = "UNKNOWN"

            series_entry["episodes"].append({
                "plex_key": stable_key(ep.get("Id", path)),
                "season": ep.get("ParentIndexNumber") or 0,
                "episode": ep.get("IndexNumber") or 0,
                "title": ep.get("Name", ""),
                "file_path": path,
                "file_size": self._file_size(ep),
                "audio_tracks": tracks,
                "dub_status": dub_status,
            })

        if orphans:
            logger.warning("%d Jellyfin episodes had no matching series and were skipped", orphans)

        result = [s for s in by_series.values() if s["episodes"]]
        logger.info("Jellyfin library scan complete: %d shows with episodes", len(result))
        return result

    def _poster_url(self, item: dict) -> str | None:
        tags = item.get("ImageTags") or {}
        if "Primary" not in tags:
            return None
        return f"{self.url}/Items/{item['Id']}/Images/Primary?maxHeight=450"

    # ------------------------------------------------------------------
    # Collections
    # ------------------------------------------------------------------

    async def sync_collections(self, series_data: list[dict], changed: bool = True) -> dict:
        """Put each show into the collection matching its dub status."""
        if not changed:
            logger.info("No dub status changed this scan — skipping Jellyfin collection sync")
            return {"collections_updated": 0, "skipped": True}

        collection_map = {
            "DUBBED": "Dubbed Anime",
            "PARTIAL": "Partially Dubbed Anime",
            "SUB_ONLY": "Sub-Only Anime",
        }
        title_to_status = {
            s["title"].lower(): s["dub_status"]
            for s in series_data
            if s.get("dub_status") in collection_map and s.get("title")
        }
        if not title_to_status:
            return {"collections_updated": 0, "skipped": False}

        series_items = await self._get_items({
            "recursive": "true",
            "includeItemTypes": "Series",
        })
        groups: dict[str, list[str]] = {status: [] for status in collection_map}
        for item in series_items:
            status = title_to_status.get((item.get("Name") or "").lower())
            if status and item.get("Id"):
                groups[status].append(item["Id"])

        existing = await self._get_collections()
        updated = 0
        for status, ids in groups.items():
            if not ids:
                continue
            name = collection_map[status]
            try:
                collection_id = existing.get(name.lower())
                if collection_id is None:
                    collection_id = await self._create_collection(name, ids)
                    if collection_id is None:
                        continue
                    existing[name.lower()] = collection_id
                else:
                    current = {
                        i["Id"] for i in await self._get_items({
                            "parentId": collection_id, "recursive": "false",
                        }) if i.get("Id")
                    }
                    to_add = [i for i in ids if i not in current]
                    to_remove = [i for i in current if i not in set(ids)]
                    if to_add:
                        await self._collection_items("POST", collection_id, to_add)
                    if to_remove:
                        await self._collection_items("DELETE", collection_id, to_remove)
                updated += len(ids)
                logger.info("Jellyfin collection '%s': %d shows", name, len(ids))
            except httpx.HTTPError as e:
                logger.warning("Failed to sync Jellyfin collection '%s': %s", name, e)

        return {"collections_updated": updated, "skipped": False}

    async def _get_collections(self) -> dict[str, str]:
        items = await self._get_items({
            "recursive": "true",
            "includeItemTypes": "BoxSet",
        })
        return {i["Name"].lower(): i["Id"] for i in items if i.get("Name") and i.get("Id")}

    async def _create_collection(self, name: str, ids: list[str]) -> str | None:
        first, rest = ids[:self._COLLECTION_BATCH], ids[self._COLLECTION_BATCH:]
        try:
            resp = await self.client.post(
                "/Collections", params={"name": name, "ids": ",".join(first)}
            )
            resp.raise_for_status()
            collection_id = resp.json().get("Id")
            if collection_id and rest:
                await self._collection_items("POST", collection_id, rest)
            return collection_id
        except httpx.HTTPError as e:
            logger.warning("Failed to create Jellyfin collection '%s': %s", name, e)
            return None

    # Item IDs travel in the query string; a few hundred GUIDs overrun the
    # server's request-line limit and every sync of a big collection 414s.
    _COLLECTION_BATCH = 50

    async def _collection_items(self, method: str, collection_id: str, ids: list[str]) -> None:
        for start in range(0, len(ids), self._COLLECTION_BATCH):
            batch = ids[start:start + self._COLLECTION_BATCH]
            resp = await self.client.request(
                method, f"/Collections/{collection_id}/Items", params={"ids": ",".join(batch)}
            )
            resp.raise_for_status()

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def get_sample_paths(self, count: int = 5) -> list[str]:
        if not self._path_index:
            return []
        return list(self._path_index.keys())[:count]

    def get_index_progress(self) -> dict:
        return dict(self._index_progress)

    def get_match_stats(self) -> dict:
        return dict(self._stats)

    @staticmethod
    def _extract_audio(item: dict) -> list[dict]:
        tracks = []
        for source in item.get("MediaSources") or []:
            for stream in source.get("MediaStreams") or []:
                if stream.get("Type") != "Audio":
                    continue
                tracks.append({
                    "language": stream.get("Language") or "und",
                    "codec": stream.get("Codec") or "unknown",
                })
            if tracks:
                break  # one media source per episode, same as the Plex client
        return tracks

    @staticmethod
    def _file_size(item: dict) -> int:
        for source in item.get("MediaSources") or []:
            size = source.get("Size")
            if size:
                return int(size)
        return 0

    @staticmethod
    def _normalize_path(path: str) -> str:
        return str(PurePosixPath(path.replace("\\", "/"))).lower()

    async def close(self):
        if self._path_index:
            stats = self.get_match_stats()
            logger.info(
                "Jellyfin match stats: %d path hits, %d filename hits, %d misses",
                stats["hit_path"], stats["hit_name"], stats["miss"],
            )
        self._path_index = None
        self._name_index = None
        await self.client.aclose()
