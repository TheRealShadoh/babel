import logging
import re

import httpx

logger = logging.getLogger(__name__)


class SonarrClient:
    def __init__(self, url: str, api_key: str):
        self.url = url.rstrip("/")
        self.api_key = api_key
        self.client = httpx.AsyncClient(
            base_url=f"{self.url}/api/v3",
            headers={"X-Api-Key": self.api_key},
            timeout=30.0,
        )
        self._tags_cache: list[dict] | None = None

    async def test_connection(self) -> tuple[bool, str]:
        try:
            resp = await self.client.get("/system/status")
            resp.raise_for_status()
            version = resp.json().get("version", "unknown")
            return True, f"Connected to Sonarr v{version}"
        except httpx.HTTPStatusError as e:
            return False, f"Sonarr returned {e.response.status_code}"
        except httpx.RequestError as e:
            return False, f"Cannot reach Sonarr: {e}"

    async def get_root_folders(self) -> list[dict]:
        """GET /api/v3/rootfolder -- return all root folders."""
        try:
            resp = await self.client.get("/rootfolder")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch root folders: %s", e)
            return []

        return [
            {
                "id": rf["id"],
                "path": rf.get("path", ""),
                "freeSpace": rf.get("freeSpace", 0),
                "totalSpace": rf.get("totalSpace", 0),
            }
            for rf in resp.json()
        ]

    async def get_tags(self) -> list[dict]:
        """GET /api/v3/tag -- return all tags (cached after first call)."""
        if self._tags_cache is not None:
            return self._tags_cache

        try:
            resp = await self.client.get("/tag")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch tags: %s", e)
            return []

        self._tags_cache = [
            {"id": tag["id"], "label": tag.get("label", "")}
            for tag in resp.json()
        ]
        return self._tags_cache

    async def get_series_by_root_folder(self) -> dict[str, int]:
        """Get count of series per root folder path.

        Fetches all series, derives root folder from each series path, and
        groups by that root folder.
        """
        try:
            resp = await self.client.get("/series")
            resp.raise_for_status()
            all_series = resp.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch series for root folder counts: %s", e)
            return {}

        try:
            rf_resp = await self.client.get("/rootfolder")
            rf_resp.raise_for_status()
            root_folders = sorted(
                [rf.get("path", "") for rf in rf_resp.json()],
                key=len,
                reverse=True,
            )
        except httpx.HTTPError as e:
            logger.error("Failed to fetch root folders for matching: %s", e)
            return {}

        counts: dict[str, int] = {}
        for s in all_series:
            series_path = s.get("path", "")
            matched_root = ""
            for rf_path in root_folders:
                if series_path.startswith(rf_path):
                    matched_root = rf_path
                    break
            if matched_root:
                counts[matched_root] = counts.get(matched_root, 0) + 1

        return counts

    async def get_anime_series(self, filter_mode: str = "type") -> list[dict]:
        try:
            resp = await self.client.get("/series")
            resp.raise_for_status()
            all_series = resp.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch series: %s", e)
            return []

        if filter_mode == "type":
            return [
                self._slim_series(s)
                for s in all_series
                if s.get("seriesType") == "anime"
            ]

        if filter_mode.startswith("tag:"):
            tag_name = filter_mode[4:]
            tag_id = await self._resolve_tag(tag_name)
            if tag_id is None:
                logger.warning("Tag '%s' not found in Sonarr", tag_name)
                return []
            return [
                self._slim_series(s)
                for s in all_series
                if tag_id in s.get("tags", [])
            ]

        logger.warning("Unknown filter_mode '%s', returning all series", filter_mode)
        return [self._slim_series(s) for s in all_series]

    async def get_episodes(self, series_id: int) -> list[dict]:
        try:
            resp = await self.client.get("/episode", params={"seriesId": series_id})
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch episodes for series %d: %s", series_id, e)
            return []

        return [
            {
                "id": ep["id"],
                "seasonNumber": ep.get("seasonNumber"),
                "episodeNumber": ep.get("episodeNumber"),
                "title": ep.get("title"),
                "hasFile": ep.get("hasFile", False),
                "episodeFileId": ep.get("episodeFileId", 0),
            }
            for ep in resp.json()
        ]

    async def get_episode_files(self, series_id: int) -> list[dict]:
        try:
            resp = await self.client.get(
                "/episodefile", params={"seriesId": series_id}
            )
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error(
                "Failed to fetch episode files for series %d: %s", series_id, e
            )
            return []

        return [
            {
                "id": ef["id"],
                "path": ef.get("path"),
                "size": ef.get("size"),
                "quality": ef.get("quality", {}).get("quality", {}).get("name"),
            }
            for ef in resp.json()
        ]

    async def search_episodes(self, episode_ids: list[int]) -> bool:
        try:
            resp = await self.client.post(
                "/command",
                json={"name": "EpisodeSearch", "episodeIds": episode_ids},
            )
            resp.raise_for_status()
            logger.info("Episode search triggered for %d episodes", len(episode_ids))
            return True
        except httpx.HTTPError as e:
            logger.error("Failed to trigger episode search: %s", e)
            return False

    async def get_queue(self) -> list[dict]:
        """GET /api/v3/queue with pagination to get ALL items."""
        all_records = []
        page = 1
        page_size = 200
        while True:
            try:
                resp = await self.client.get("/queue", params={
                    "pageSize": page_size, "page": page,
                    "includeEpisode": "true"
                })
                resp.raise_for_status()
                data = resp.json()
                records = data.get("records", [])
                all_records.extend(records)
                total = data.get("totalRecords", 0)
                if len(all_records) >= total or not records:
                    break
                page += 1
            except httpx.HTTPError as e:
                logger.error("Failed to fetch queue page %d: %s", page, e)
                break
        return all_records

    async def get_stuck_imports(self, min_age_minutes: int = 30) -> list[dict]:
        """Find queue items that are stuck (import warning/failed).
        Returns items with: id, episodeId, title, status, statusMessages, downloadId, outputPath
        Only returns items older than min_age_minutes.
        """
        from datetime import datetime, timezone, timedelta
        queue = await self.get_queue()
        stuck = []
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=min_age_minutes)

        for item in queue:
            status = item.get("trackedDownloadStatus", "").lower()
            state = item.get("trackedDownloadState", "").lower()
            if status not in ("warning", "error"):
                continue

            # Check age
            added = item.get("added", "")
            if added:
                try:
                    item_time = datetime.fromisoformat(added.replace("Z", "+00:00"))
                    if item_time > cutoff:
                        continue  # Too recent, still processing
                except (ValueError, TypeError):
                    pass

            stuck.append({
                "queue_id": item.get("id"),
                "episode_id": item.get("episodeId"),
                "series_id": item.get("seriesId"),
                "title": item.get("title", ""),
                "status": status,
                "state": state,
                "status_messages": item.get("statusMessages", []),
                "download_id": item.get("downloadId", ""),
                "output_path": item.get("outputPath", ""),
                "size": item.get("size", 0),
            })

        return stuck

    async def retry_import(self, download_path: str) -> bool:
        """Trigger Sonarr to re-scan a download folder for import."""
        try:
            resp = await self.client.post("/command", json={
                "name": "DownloadedEpisodesScan",
                "path": download_path,
            })
            resp.raise_for_status()
            logger.info("Triggered import rescan for: %s", download_path)
            return True
        except httpx.HTTPError as e:
            logger.error("Failed to trigger import rescan: %s", e)
            return False

    async def force_manual_import(self, queue_id: int, series_id: int, episode_ids: list[int]) -> bool:
        """Force import a stuck queue item by overriding the series/episode mapping.
        This handles the 'matched by ID' error by explicitly telling Sonarr which
        series and episodes to import the file as.
        """
        try:
            # First get the queue item details
            resp = await self.client.get(f"/queue/{queue_id}")
            if resp.status_code == 404:
                logger.debug("Queue item %d no longer exists (already resolved)", queue_id)
                return False
            resp.raise_for_status()
            queue_item = resp.json()

            download_id = queue_item.get("downloadId", "")
            output_path = queue_item.get("outputPath", "")

            if not output_path:
                return False

            # Get manual import candidates for this path
            resp = await self.client.get("/manualimport", params={
                "downloadId": download_id,
                "seriesId": series_id,
                "filterExistingFiles": "true",
            })
            resp.raise_for_status()
            candidates = resp.json()

            if not candidates:
                return False

            # Build import commands — override with the correct series/episodes
            imports = []
            for candidate in candidates:
                imports.append({
                    "path": candidate.get("path"),
                    "seriesId": series_id,
                    "episodeIds": episode_ids if episode_ids else candidate.get("episodes", []),
                    "quality": candidate.get("quality"),
                    "languages": candidate.get("languages", []),
                    "downloadId": download_id,
                })

            if not imports:
                return False

            resp = await self.client.post("/command", json={
                "name": "ManualImport",
                "files": imports,
                "importMode": "auto",
            })
            resp.raise_for_status()
            logger.info("Forced manual import for queue item %d (%d files)", queue_id, len(imports))
            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.debug("Queue item %d gone during import (already resolved)", queue_id)
            else:
                logger.warning("Manual import failed for queue item %d: HTTP %d", queue_id, e.response.status_code)
            return False
        except httpx.HTTPError as e:
            logger.warning("Manual import failed for queue item %d: %s", queue_id, type(e).__name__)
            return False

    async def remove_from_queue(self, queue_id: int, blocklist: bool = False) -> bool:
        """Remove an item from Sonarr's queue.
        blocklist=True adds the release to blocklist so it won't be grabbed again.
        """
        try:
            resp = await self.client.delete(
                f"/queue/{queue_id}",
                params={"removeFromClient": "true", "blocklist": str(blocklist).lower()},
            )
            if resp.status_code == 404:
                logger.debug("Queue item %d already gone", queue_id)
                return True  # Already removed = success
            resp.raise_for_status()
            logger.info("Removed queue item %d (blocklist=%s)", queue_id, blocklist)
            return True
        except httpx.HTTPError as e:
            logger.warning("Failed to remove queue item %d: %s", queue_id, type(e).__name__)
            return False

    async def get_history_for_episode(self, episode_id: int, limit: int = 5) -> list[dict]:
        """GET /api/v3/history?episodeId={id}&pageSize={limit}&sortKey=date&sortDirection=descending
        Return recent history events for an episode.
        """
        try:
            resp = await self.client.get(
                "/history",
                params={
                    "episodeId": episode_id,
                    "pageSize": limit,
                    "sortKey": "date",
                    "sortDirection": "descending",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("records", [])
        except httpx.HTTPError as e:
            logger.error("Failed to fetch history for episode %d: %s", episode_id, e)
            return []

    async def get_recent_history(self, limit: int = 50) -> list[dict]:
        """GET /api/v3/history?pageSize={limit}&sortKey=date&sortDirection=descending
        Return most recent history events, filtered to grabbed/imported/failed.
        """
        try:
            resp = await self.client.get(
                "/history",
                params={
                    "pageSize": limit,
                    "sortKey": "date",
                    "sortDirection": "descending",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            relevant_types = {"grabbed", "downloadFolderImported", "downloadFailed"}
            return [
                r for r in data.get("records", [])
                if r.get("eventType") in relevant_types
            ]
        except httpx.HTTPError as e:
            logger.error("Failed to fetch recent history: %s", e)
            return []

    async def ensure_tag(self, label: str) -> int:
        """Get or create a tag by label. Returns tag ID."""
        tags = await self.get_tags()
        for t in tags:
            if t["label"].lower() == label.lower():
                return t["id"]
        try:
            resp = await self.client.post("/tag", json={"label": label})
            resp.raise_for_status()
            self._tags_cache = None  # Invalidate cache after creating a new tag
            return resp.json()["id"]
        except httpx.HTTPError as e:
            logger.error("Failed to create tag '%s': %s", label, e)
            return -1

    async def set_series_tags(self, series_id: int, tag_ids_to_add: list[int], tag_ids_to_remove: list[int]) -> bool:
        """Update tags on a series. Fetches full series, modifies tags, PUTs back."""
        try:
            resp = await self.client.get(f"/series/{series_id}")
            resp.raise_for_status()
            series = resp.json()

            current_tags = set(series.get("tags", []))
            current_tags.update(tag_ids_to_add)
            current_tags -= set(tag_ids_to_remove)
            series["tags"] = list(current_tags)

            resp = await self.client.put(f"/series/{series_id}", json=series)
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.error("Failed to update tags for series %d: %s", series_id, e)
            return False

    async def sync_dub_tags(self, series_statuses: list[dict]) -> dict:
        """Sync dub status tags for multiple series.

        series_statuses: [{"sonarr_id": 123, "dub_status": "DUBBED"}, ...]

        Creates tags: babel:dubbed, babel:partial-dub, babel:sub-only
        For each series, ensures the correct tag is applied and others removed.

        Returns: {"tagged": N, "errors": N}
        """
        # Define tag mapping
        tag_map = {
            "DUBBED": "babel:dubbed",
            "PARTIAL": "babel:partial-dub",
            "SUB_ONLY": "babel:sub-only",
        }

        # Ensure all tags exist and get their IDs
        tag_ids = {}
        for status, label in tag_map.items():
            tid = await self.ensure_tag(label)
            if tid > 0:
                tag_ids[status] = tid

        all_babel_tag_ids = set(tag_ids.values())
        tagged = 0
        errors = 0

        for entry in series_statuses:
            sid = entry["sonarr_id"]
            status = entry["dub_status"]

            if sid < 0:  # Plex-only series (negative IDs), skip
                continue

            correct_tag_id = tag_ids.get(status)
            tags_to_add = [correct_tag_id] if correct_tag_id else []
            tags_to_remove = list(all_babel_tag_ids - set(tags_to_add))

            if tags_to_add or tags_to_remove:
                success = await self.set_series_tags(sid, tags_to_add, tags_to_remove)
                if success:
                    tagged += 1
                else:
                    errors += 1

        return {"tagged": tagged, "errors": errors}

    async def close(self):
        await self.client.aclose()

    async def _resolve_tag(self, tag_name: str) -> int | None:
        try:
            resp = await self.client.get("/tag")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch tags: %s", e)
            return None

        for tag in resp.json():
            if tag.get("label", "").lower() == tag_name.lower():
                return tag["id"]
        return None

    # ------------------------------------------------------------------
    # Custom Format / Dub Preference helpers
    # ------------------------------------------------------------------

    async def get_custom_formats(self) -> list[dict]:
        """GET /api/v3/customformat -- return all custom formats."""
        try:
            resp = await self.client.get("/customformat")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch custom formats: %s", e)
            return []

    async def get_quality_profiles(self) -> list[dict]:
        """GET /api/v3/qualityprofile -- return all quality profiles."""
        try:
            resp = await self.client.get("/qualityprofile")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch quality profiles: %s", e)
            return []

    async def check_dub_format_exists(self) -> dict:
        """Check if a 'Dual Audio' or 'English Audio' custom format exists.

        Returns:
            {"exists": bool, "format_id": int|None, "format_name": str|None,
             "profiles_using": list[str]}
        """
        result: dict = {
            "exists": False,
            "format_id": None,
            "format_name": None,
            "profiles_using": [],
        }

        title_pattern = re.compile(
            r"(dual[._\-\s]?audio|multi[._\-\s]?audio|english[._\-\s]?dub|eng[._\-\s]?dub)",
            re.IGNORECASE,
        )

        formats = await self.get_custom_formats()
        matched_format: dict | None = None

        for cf in formats:
            for spec in cf.get("specifications", []):
                impl = spec.get("implementation", "")
                if impl == "ReleaseTitleSpecification":
                    for field in spec.get("fields", []):
                        if field.get("name") == "value" and isinstance(field.get("value"), str):
                            if title_pattern.search(field["value"]):
                                matched_format = cf
                                break
                elif impl == "LanguageSpecification":
                    for field in spec.get("fields", []):
                        if field.get("name") == "value" and field.get("value") == 1:
                            matched_format = cf
                            break
                if matched_format:
                    break
            if matched_format:
                break

        if not matched_format:
            return result

        result["exists"] = True
        result["format_id"] = matched_format["id"]
        result["format_name"] = matched_format.get("name")

        # Find which quality profiles reference this format
        profiles = await self.get_quality_profiles()
        for profile in profiles:
            for item in profile.get("formatItems", []):
                if item.get("format") == matched_format["id"] and item.get("score", 0) > 0:
                    result["profiles_using"].append(profile.get("name", f"ID {profile['id']}"))
                    break

        return result

    async def create_dub_custom_format(self) -> dict | None:
        """Create a custom format named 'Babel: Dual Audio / English Dub'.

        Returns the created format dict, or None on failure.
        """
        payload = {
            "name": "Babel: Dual Audio / English Dub",
            "includeCustomFormatWhenRenaming": False,
            "specifications": [
                {
                    "name": "Dual Audio",
                    "implementation": "ReleaseTitleSpecification",
                    "negate": False,
                    "required": False,
                    "fields": [{"name": "value", "value": "\\b(dual[._-]?audio|multi[._-]?audio)\\b"}],
                },
                {
                    "name": "English Dub",
                    "implementation": "ReleaseTitleSpecification",
                    "negate": False,
                    "required": False,
                    "fields": [{"name": "value", "value": "\\b(english[._-]?dub|eng[._-]?dub|dubbed)\\b"}],
                },
                {
                    "name": "English Language",
                    "implementation": "LanguageSpecification",
                    "negate": False,
                    "required": False,
                    "fields": [{"name": "value", "value": 1}],
                },
            ],
        }
        try:
            resp = await self.client.post("/customformat", json=payload)
            resp.raise_for_status()
            created = resp.json()
            logger.info("Created custom format '%s' (id=%s)", created.get("name"), created.get("id"))
            return created
        except httpx.HTTPError as e:
            logger.error("Failed to create dub custom format: %s", e)
            return None

    async def assign_format_to_anime_profiles(self, format_id: int, score: int = 1000) -> list[str]:
        """Assign a custom format to quality profiles used by anime series.

        For each quality profile, check if it is used by anime series.
        If the format is not already assigned with a positive score, add it.

        Returns list of profile names that were updated.
        """
        updated: list[str] = []
        anime_profile_ids: set[int] = set()

        # Fetch full series list to get quality profile IDs for anime
        try:
            resp = await self.client.get("/series")
            resp.raise_for_status()
            all_series = resp.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch series for profile assignment: %s", e)
            return updated

        for s in all_series:
            if s.get("seriesType") == "anime":
                pid = s.get("qualityProfileId")
                if pid is not None:
                    anime_profile_ids.add(pid)

        if not anime_profile_ids:
            logger.info("No anime series found; skipping profile assignment")
            return updated

        profiles = await self.get_quality_profiles()
        for profile in profiles:
            if profile["id"] not in anime_profile_ids:
                continue

            format_items = profile.get("formatItems", [])

            # Check if already assigned with a positive score
            already_set = False
            for item in format_items:
                if item.get("format") == format_id and item.get("score", 0) > 0:
                    already_set = True
                    break

            if already_set:
                continue

            # Remove any existing entry for this format (score <= 0)
            format_items = [fi for fi in format_items if fi.get("format") != format_id]
            format_items.append({"format": format_id, "score": score})
            profile["formatItems"] = format_items

            try:
                resp = await self.client.put(f"/qualityprofile/{profile['id']}", json=profile)
                resp.raise_for_status()
                profile_name = profile.get("name", f"ID {profile['id']}")
                updated.append(profile_name)
                logger.info("Assigned dub format (id=%d, score=%d) to profile '%s'", format_id, score, profile_name)
            except httpx.HTTPError as e:
                logger.error("Failed to update profile '%s': %s", profile.get("name"), e)

        return updated

    async def ensure_dub_preference(self) -> dict:
        """Main entry: check if dub format exists, create if not, assign to profiles.

        Returns summary dict with keys:
            format_existed, format_created, format_id, format_name,
            profiles_updated, profiles_already_configured, error
        """
        summary: dict = {
            "format_existed": False,
            "format_created": False,
            "format_id": None,
            "format_name": None,
            "profiles_updated": [],
            "profiles_already_configured": [],
            "error": None,
        }

        try:
            check = await self.check_dub_format_exists()

            if check["exists"]:
                summary["format_existed"] = True
                summary["format_id"] = check["format_id"]
                summary["format_name"] = check["format_name"]
                summary["profiles_already_configured"] = check["profiles_using"]
            else:
                created = await self.create_dub_custom_format()
                if created is None:
                    summary["error"] = "Failed to create custom format in Sonarr"
                    return summary
                summary["format_created"] = True
                summary["format_id"] = created["id"]
                summary["format_name"] = created.get("name")

            updated = await self.assign_format_to_anime_profiles(summary["format_id"])
            summary["profiles_updated"] = updated

        except Exception as e:
            logger.exception("Error in ensure_dub_preference")
            summary["error"] = str(e)

        return summary

    @staticmethod
    def _slim_series(s: dict) -> dict:
        poster_url = None
        for img in s.get("images", []):
            if img.get("coverType") == "poster":
                poster_url = img.get("remoteUrl")
                break
        return {
            "id": s["id"],
            "title": s.get("title"),
            "path": s.get("path"),
            "seriesType": s.get("seriesType"),
            "tags": s.get("tags", []),
            "poster_url": poster_url,
            "titleSlug": s.get("titleSlug", ""),
        }
