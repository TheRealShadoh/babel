import asyncio
import logging
import threading
from pathlib import PurePosixPath

from plexapi.server import PlexServer

logger = logging.getLogger(__name__)


class PlexClient:
    def __init__(self, url: str, token: str):
        self.url = url.rstrip("/")
        self.token = token
        self._server: PlexServer | None = None
        self._path_index: dict[str, list[dict]] | None = None
        self._name_index: dict[str, list[dict]] | None = None
        self._stats = {"hit_path": 0, "hit_name": 0, "miss": 0}
        self._cancel = threading.Event()
        self._index_progress = {"current": 0, "total": 0, "section": ""}

    def _connect(self) -> PlexServer:
        if self._server is None:
            self._server = PlexServer(self.url, self.token)
        return self._server

    async def test_connection(self) -> tuple[bool, str]:
        try:
            server = await asyncio.to_thread(self._connect)
            return True, f"Connected to Plex: {server.friendlyName}"
        except Exception as e:
            return False, f"Cannot reach Plex: {e}"

    async def get_libraries(self) -> list[dict]:
        """Get all Plex library sections with type='show'.

        Returns a list of dicts with id, title, type, path, and count for each
        show-type section.  Each section may have multiple locations (paths), so
        one entry is emitted per location.
        """
        def _fetch() -> list[dict]:
            server = self._connect()
            results: list[dict] = []
            for section in server.library.sections():
                if section.type != "show":
                    continue
                count = section.totalSize
                locations = section.locations if section.locations else []
                if not locations:
                    results.append({
                        "id": section.key,
                        "title": section.title,
                        "type": section.type,
                        "path": "",
                        "count": count,
                    })
                else:
                    for loc in locations:
                        results.append({
                            "id": section.key,
                            "title": section.title,
                            "type": section.type,
                            "path": str(loc),
                            "count": count,
                        })
            return results

        return await asyncio.to_thread(_fetch)

    async def build_index(self, ignored_patterns: list[str] = None) -> int:
        """Build file path AND filename indexes for fast lookup."""
        try:
            count = await asyncio.to_thread(self._build_indexes, ignored_patterns or [])
            logger.info("Plex index built: %d episode files indexed", count)
            return count
        except Exception as e:
            logger.error("Failed to build Plex index: %s", e)
            self._path_index = {}
            self._name_index = {}
            return 0

    def _build_indexes(self, ignored_patterns: list[str]) -> int:
        server = self._connect()
        self._path_index = {}
        self._name_index = {}
        count = 0
        skipped = 0

        for section in server.library.sections():
            if section.type != "show":
                continue

            # Skip sections whose locations all match ignored patterns
            if ignored_patterns and section.locations:
                all_ignored = all(
                    any(pat in str(loc).lower() for pat in ignored_patterns)
                    for loc in section.locations
                )
                if all_ignored:
                    logger.info("Skipping ignored section: %s (locations: %s)",
                                section.title, section.locations)
                    continue

            logger.info("Indexing Plex library section: %s", section.title)
            try:
                all_episodes = section.searchEpisodes()
                total = len(all_episodes)
                self._index_progress = {"current": 0, "total": total, "section": section.title}
                logger.info("  Found %d episodes to index (reloading each for audio streams)...", total)
                for idx, episode in enumerate(all_episodes):
                    self._index_progress["current"] = idx + 1
                    if self._cancel.is_set():
                        logger.info("  Plex indexing cancelled at %d/%d", idx, total)
                        return count

                    try:
                        episode.reload()
                    except Exception:
                        skipped += 1
                        continue

                    for media in episode.media:
                        for part in media.parts:
                            tracks = self._extract_audio(part)
                            norm_path = self._normalize_path(part.file)
                            self._path_index[norm_path] = tracks
                            filename = norm_path.rsplit("/", 1)[-1] if "/" in norm_path else norm_path
                            self._name_index[filename] = tracks
                            count += 1

                    if (idx + 1) % 500 == 0:
                        logger.info("  Indexed %d/%d episodes...", idx + 1, total)

            except Exception as e:
                logger.warning("Error indexing section '%s': %s", section.title, e)
                continue

        if skipped:
            logger.warning("Skipped %d episodes during indexing (reload failed)", skipped)
        return count

    async def get_audio_tracks(self, file_path: str) -> list[dict] | None:
        """Look up audio tracks from pre-built indexes."""
        if self._path_index is None:
            await self.build_index()

        normalized = self._normalize_path(file_path)

        # Try full path match first
        tracks = self._path_index.get(normalized)
        if tracks is not None:
            self._stats["hit_path"] += 1
            return tracks

        # Try filename-only match (handles folder name differences)
        filename = normalized.rsplit("/", 1)[-1] if "/" in normalized else normalized
        tracks = self._name_index.get(filename)
        if tracks is not None:
            self._stats["hit_name"] += 1
            return tracks

        self._stats["miss"] += 1
        return None

    async def get_library_data(self, target_lang: str = "eng") -> list[dict]:
        """Get all show series with episode-level audio data from Plex.

        Returns a list of series dicts, each with:
            title, plex_key, episodes: [{season, episode, title, file_path, file_size, audio_tracks, dub_status}]
        Used when Sonarr is unavailable (Plex-only mode).
        """
        try:
            return await asyncio.to_thread(self._scan_library, target_lang)
        except Exception as e:
            logger.error("Failed to scan Plex library: %s", e)
            return []

    def _scan_library(self, target_lang: str) -> list[dict]:
        from src.config import normalize_language

        server = self._connect()
        all_series = []

        for section in server.library.sections():
            if section.type != "show":
                continue
            logger.info("Scanning Plex section: %s", section.title)

            for show in section.all():
                if self._cancel.is_set():
                    logger.info("  Plex library scan cancelled")
                    return all_series

                series_data = {
                    "title": show.title,
                    "plex_key": show.ratingKey,
                    "path": show.locations[0] if show.locations else "",
                    "poster_url": getattr(show, "thumbUrl", None),
                    "episodes": [],
                }

                try:
                    for episode in show.episodes():
                        try:
                            episode.reload()
                        except Exception:
                            continue

                        for media in episode.media:
                            for part in media.parts:
                                tracks = self._extract_audio(part)
                                norm_tracks = []
                                for t in tracks:
                                    norm_tracks.append({
                                        "language": normalize_language(t["language"]),
                                        "codec": t["codec"],
                                        "source": "plex",
                                    })

                                languages = {t["language"] for t in norm_tracks}
                                if target_lang in languages:
                                    dub_status = "DUBBED"
                                elif norm_tracks:
                                    dub_status = "SUB_ONLY"
                                else:
                                    dub_status = "UNKNOWN"

                                series_data["episodes"].append({
                                    "season": episode.parentIndex or 0,
                                    "episode": episode.index or 0,
                                    "title": episode.title,
                                    "file_path": part.file,
                                    "file_size": part.size or 0,
                                    "audio_tracks": norm_tracks,
                                    "dub_status": dub_status,
                                })
                                break  # one part per episode
                        break  # one media per episode
                except Exception as e:
                    logger.warning("Error scanning show '%s': %s", show.title, e)
                    continue

                if series_data["episodes"]:
                    all_series.append(series_data)

                if len(all_series) % 50 == 0 and len(all_series) > 0:
                    logger.info("  Scanned %d shows so far...", len(all_series))

        logger.info("Plex library scan complete: %d shows with episodes", len(all_series))
        return all_series

    def get_sample_paths(self, count: int = 5) -> list[str]:
        if not self._path_index:
            return []
        return list(self._path_index.keys())[:count]

    def get_index_progress(self) -> dict:
        return dict(self._index_progress)

    def get_match_stats(self) -> dict:
        return dict(self._stats)

    @staticmethod
    def _extract_audio(part) -> list[dict]:
        tracks = []
        for stream in part.audioStreams():
            tracks.append({
                "language": stream.languageCode or "und",
                "codec": stream.codec or "unknown",
            })
        return tracks

    @staticmethod
    def _normalize_path(path: str) -> str:
        return str(PurePosixPath(path.replace("\\", "/"))).lower()

    async def sync_collections(self, series_data: list[dict]) -> dict:
        """Update Plex collections based on dub status.
        series_data: [{"title": "...", "dub_status": "DUBBED"}, ...]
        Returns {"collections_updated": N}
        """
        return await asyncio.to_thread(self._sync_collections_impl, series_data)

    def _sync_collections_impl(self, series_data: list[dict]) -> dict:
        server = self._connect()
        collection_map = {
            "DUBBED": "Dubbed Anime",
            "PARTIAL": "Partially Dubbed Anime",
            "SUB_ONLY": "Sub-Only Anime",
        }

        # Build title -> status lookup
        title_to_status = {}
        for s in series_data:
            if s.get("dub_status") in collection_map:
                title_to_status[s["title"].lower()] = s["dub_status"]

        updated = 0
        for section in server.library.sections():
            if section.type != "show":
                continue

            # Group shows by target collection
            groups = {"DUBBED": [], "PARTIAL": [], "SUB_ONLY": []}
            for show in section.all():
                status = title_to_status.get(show.title.lower())
                if status:
                    groups[status].append(show)

            for status, shows in groups.items():
                if not shows:
                    continue
                coll_name = collection_map[status]
                try:
                    # Find existing collection
                    existing = None
                    for c in section.collections():
                        if c.title == coll_name:
                            existing = c
                            break

                    if existing:
                        # Get current items and compute diff
                        current_keys = {item.ratingKey for item in existing.items()}
                        target_keys = {s.ratingKey for s in shows}
                        to_add = [s for s in shows if s.ratingKey not in current_keys]
                        to_remove = [item for item in existing.items() if item.ratingKey not in target_keys]
                        if to_add: existing.addItems(to_add)
                        if to_remove: existing.removeItems(to_remove)
                    else:
                        section.createCollection(coll_name, items=shows)

                    updated += len(shows)
                    logger.info("Plex collection '%s': %d shows", coll_name, len(shows))
                except Exception as e:
                    logger.warning("Failed to sync collection '%s': %s", coll_name, e)

        return {"collections_updated": updated}

    async def close(self):
        if self._path_index:
            stats = self.get_match_stats()
            logger.info("Plex match stats: %d path hits, %d filename hits, %d misses",
                        stats["hit_path"], stats["hit_name"], stats["miss"])
        self._path_index = None
        self._name_index = None
