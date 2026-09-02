"""Minimal fakes for SonarrClient/PlexClient used to test the scan engine
without a live Sonarr/Plex server."""


class FakeSonarr:
    def __init__(self, series=None, episodes_by_series=None, files_by_series=None,
                 stuck_imports=None, fail_series=False, fail_episodes_for=(),
                 fail_files_for=()):
        self._series = series or []
        self._episodes = episodes_by_series or {}
        self._files = files_by_series or {}
        self._stuck = stuck_imports or []
        # None means "the request failed", which is different from an empty
        # list and must never license a delete.
        self._fail_series = fail_series
        self._fail_episodes_for = set(fail_episodes_for)
        self._fail_files_for = set(fail_files_for)
        self.searched_ids: list[list[int]] = []
        self.sync_dub_tags_calls = []
        self.removed_from_queue: list[tuple[int, bool]] = []
        self.force_manual_imports: list[tuple[int, int, list[int]]] = []
        self.retried_imports: list[str] = []

    async def get_anime_series(self, filter_mode):
        if self._fail_series:
            return None
        return self._series

    async def get_episode_files(self, series_id):
        if series_id in self._fail_files_for:
            return None
        return self._files.get(series_id, [])

    async def get_episodes(self, series_id):
        if series_id in self._fail_episodes_for:
            return None
        return self._episodes.get(series_id, [])

    async def search_episodes(self, episode_ids):
        self.searched_ids.append(list(episode_ids))
        return True

    async def sync_dub_tags(self, statuses):
        self.sync_dub_tags_calls.append(statuses)
        return {"tagged": len(statuses), "errors": 0}

    async def get_stuck_imports(self, min_age_minutes=30):
        return self._stuck

    async def remove_from_queue(self, queue_id, blocklist=False):
        self.removed_from_queue.append((queue_id, blocklist))
        return True

    async def force_manual_import(self, queue_id, series_id, episode_ids):
        self.force_manual_imports.append((queue_id, series_id, list(episode_ids)))
        return True

    async def retry_import(self, output_path):
        self.retried_imports.append(output_path)
        return True

    async def close(self):
        pass


def stuck_item(queue_id, episode_id, series_id, title, messages, output_path=""):
    return {
        "queue_id": queue_id,
        "episode_id": episode_id,
        "series_id": series_id,
        "title": title,
        "status": "warning",
        "state": "importPending",
        "status_messages": [{"title": title, "messages": messages}],
        "download_id": f"dl-{queue_id}",
        "output_path": output_path,
        "size": 1000,
    }


class FakePlex:
    def __init__(self, tracks_by_path=None, library_data=None):
        self.tracks_by_path = tracks_by_path or {}
        self.library_data = library_data or []
        self.build_index_calls = 0
        self._indexed = False
        self._cancel = _NullEvent()

    def is_indexed(self):
        return self._indexed

    async def build_index(self, ignored_patterns=None):
        self.build_index_calls += 1
        self._indexed = True
        return len(self.tracks_by_path)

    async def get_audio_tracks(self, file_path):
        tracks = self.tracks_by_path.get(file_path)
        # Return a copy so callers mutating dicts in-place (adding "source")
        # don't corrupt the fixture across calls.
        return [dict(t) for t in tracks] if tracks is not None else None

    def get_sample_paths(self, n):
        return list(self.tracks_by_path.keys())[:n]

    async def sync_collections(self, series_data, changed=True):
        self.sync_collections_calls = getattr(self, "sync_collections_calls", 0) + 1
        if not changed:
            return {"collections_updated": 0, "skipped": True}
        return {"collections_updated": 0, "skipped": False}

    async def get_library_data(self, target_lang):
        return self.library_data

    async def close(self):
        pass


class _NullEvent:
    def is_set(self):
        return False

    def set(self):
        pass


def base_cfg(**overrides):
    cfg = {
        "SONARR_URL": "",
        "SONARR_API_KEY": "",
        "PLEX_URL": "",
        "PLEX_TOKEN": "",
        "TARGET_LANGUAGE": "eng",
        "SEARCH_COOLDOWN_DAYS": 7,
        "SEARCH_RATE_LIMIT": 1000,
        "SONARR_PATH_PREFIX": "",
        "LOCAL_PATH_PREFIX": "/media",
        "PLEX_PATH_PREFIX": "",
        "ANIME_FILTER": "type",
        "MAX_SEARCH_ATTEMPTS": 3,
        "AUTO_TAG_SONARR": "false",
        "AUTO_COLLECTIONS_PLEX": "false",
        "AUTO_RESOLVE_IMPORTS": "false",
        "DISCORD_WEBHOOK_URL": "",
        "DB_PATH": "/tmp/babel_test_env.db",
    }
    cfg.update(overrides)
    return cfg


def sonarr_episode(id, season, episode, title, has_file=True, episode_file_id=None):
    return {
        "id": id,
        "seasonNumber": season,
        "episodeNumber": episode,
        "title": title,
        "hasFile": has_file,
        "episodeFileId": episode_file_id if episode_file_id is not None else (id if has_file else 0),
    }


def sonarr_file(id, path, size):
    return {"id": id, "path": path, "size": size, "quality": "1080p"}


def plex_show(plex_key, title, path, episodes):
    return {"title": title, "plex_key": plex_key, "path": path, "poster_url": None, "episodes": episodes}


def plex_episode(plex_key, season, ep, title, file_path, file_size, audio_tracks, dub_status):
    return {
        "plex_key": plex_key,
        "season": season,
        "episode": ep,
        "title": title,
        "file_path": file_path,
        "file_size": file_size,
        "audio_tracks": audio_tracks,
        "dub_status": dub_status,
    }
