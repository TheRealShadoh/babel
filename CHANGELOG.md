# Changelog

All notable changes to Babel. Versions are git tags (`vX.Y.Z`); the Docker
image is tagged to match, plus `latest` for the current master.

## 1.2.1

Fixes from a production review of 1.2.0.

### Data safety
- A configured-but-unreachable Sonarr no longer falls back to media-server-only
  mode. That fallback reconciled the library against media-server IDs and
  deleted every Sonarr-keyed series (with search history and upgrade records)
  whenever Sonarr was restarting at scan time. The pass now fails and retries.
- The media-server-only pass can only prune rows it created; Sonarr rows are
  never candidates.
- A restarting Sonarr reporting "no files" (or files without episodes, or no
  episodes for a known series) is treated as a transient answer rather than
  applied, and a pass in which most of the library "loses" its files at once
  is refused.
- A replaced file no longer inherits the old file's cached audio tracks when
  the first probe fails; an unreadable replacement stays a pending upgrade
  instead of being written off as failed and re-searched immediately.
- The initial scan retries with backoff while services come up; scheduled
  runs survive event-loop stalls; the daily dub lookup runs on containers
  that restart daily; `SCAN_INTERVAL_HOURS` is bounded below by one hour.

### Security
- The stored API key/token is only used by a connection test against the URL
  it was saved for; testing a new URL requires the key in the form.
- ffprobe refuses anything that is not an absolute local path, so a forged
  webhook cannot hand it a URL.
- Library and tag names from Sonarr/Plex/Jellyfin are rendered with DOM APIs
  on the Settings page, never as HTML.
- Password verification runs off the event loop with a short verified-
  credential cache; `AUTH_USERNAME` from the environment now truly wins over
  the Settings page, as documented.
- The Discord webhook URL is treated as a secret.

### Correctness and performance
- Download-status checks look at one pending record per episode and only at
  history since the search was made; previously every search read as
  "imported" and the check issued one Sonarr request per accumulated record.
- Path prefixes match on path boundaries (`/tv` no longer rewrites
  `/tv-anime`).
- MyAnimeList matches require a close title; the first search hit is no
  longer taken blindly.
- Jellyfin collection updates are batched; large collections no longer 414.
- A media-server library read that dropped items does not prune.
- Writes commit periodically inside a long series, so the UI and webhook no
  longer hit "database is locked" during a slow probe run.
- Discord notifications respect embed limits and log rejections.
- The per-episode Search button works (it returned 500).
- The Logs level filter no longer reverts on refresh; the ignore-pattern
  inputs no longer submit Save All; inputs have labels.

### Ops and docs
- `entrypoint.sh` no longer exits when `/app/data` cannot be chowned, and
  rejects non-numeric `PUID`/`PGID` with a message.
- CI: least-privilege token, concurrency group per ref.
- README documents every setting, the media mount, path mapping, and the
  full API; the Unraid template and Docker Hub page match.
- The log file is written next to the database (`DB_PATH`).
- Removed the dead `WEB_PORT` setting.

## 1.2.0

- Jellyfin support, alongside Plex or instead of it.
- Sonarr monitor-status sync when a dub is available.
- Diagnostics page and `GET /api/diagnostics`.
- Anime News Network as a second dub-availability source; streaming platforms
  recognised as dub licensors; airing shows no longer reported as "no dub".
- `scripts/check_dub_lookup.py` for verifying dub lookup from the container.
- Guard against a restarting Sonarr wiping cached classifications.

## 1.1.0

- Initial public release: Sonarr + Plex dub detection, upgrade tracking,
  stuck-import resolution, MyAnimeList dub lookup, Discord notifications,
  hung-mount hardening and the event-loop watchdog.
