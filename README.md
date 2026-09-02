<p align="center">
  <img src="icon.svg" alt="Babel" width="120" height="120">
</p>

<h1 align="center">Babel</h1>

<p align="center">
  <strong>Media Dub Detection & Upgrade Tool for Sonarr and Plex</strong>
</p>

<p align="center">
  <a href="https://hub.docker.com/r/therealshadoh/babel"><img src="https://img.shields.io/docker/pulls/therealshadoh/babel?style=flat-square&logo=docker&label=Docker%20Hub" alt="Docker Hub"></a>
  <a href="https://hub.docker.com/r/therealshadoh/babel"><img src="https://img.shields.io/docker/image-size/therealshadoh/babel/latest?style=flat-square" alt="Image Size"></a>
  <a href="https://github.com/TheRealShadoh/babel/releases"><img src="https://img.shields.io/github/v/tag/TheRealShadoh/babel?style=flat-square&label=version" alt="Version"></a>
</p>

<p align="center">
  Babel monitors your media library, detects sub-only episodes, and automatically searches for English dubbed versions through Sonarr. Named after the Tower of Babel — bridging the language gap in your media library.
</p>

---

<p align="center">
  <img src="screenshots/overview.png" alt="Babel Dashboard" width="800">
</p>

<p align="center">
  <img src="screenshots/series.png" alt="Series Library" width="800">
</p>

---

## Features

**Core**
- Automatic dub detection via Plex audio track analysis and ffprobe
- Smart upgrade searches with rate limiting, cooldowns, and max attempt caps
- Upgrade tracking — monitors downloads from search through import, auto-retries failures
- Stuck import resolution — detects and force-imports stuck Sonarr queue items (including ID mismatches)

**Intelligence**
- Dub availability lookup via MyAnimeList/Jikan — knows if a dub even exists before searching
- Dub status change notifications — alerts when a previously unlicensed show gets a dub
- Auto-overrides MAL when actual dubbed audio is detected in files

**Integrations**
- Sonarr: custom format creation, tag syncing, webhook support for instant upgrade detection
- Plex: audio indexing, collection management (Dubbed Anime, Sub-Only, etc.)
- Discord: webhook notifications for scan results and dub upgrades
- Sonarr webhook endpoint for real-time import awareness

**Dashboard**
- Polished dark-themed web UI with poster art and live scan progress
- Series browser with filter pills, search, sort, and pagination
- Activity feed with real-time download monitoring
- Dub Intelligence page with Recently Dubbed / Dub Expected / No Dub tabs
- Scan history with drilldown detail views
- Log viewer with level filtering
- All settings configurable via web UI

## Quick Start

```yaml
services:
  babel:
    image: therealshadoh/babel:latest
    container_name: babel
    ports:
      - "8686:8686"
    volumes:
      - babel-data:/app/data
    environment:
      - SONARR_URL=http://your-server:8989
      - SONARR_API_KEY=your-api-key
      - PLEX_URL=http://your-server:32400
      - PLEX_TOKEN=your-plex-token
    restart: unless-stopped

volumes:
  babel-data:
```

```bash
docker compose up -d
```

Open `http://localhost:8686` to access the dashboard.

## Configuration

All settings can be configured via environment variables or the web UI Settings page.

| Variable | Default | Description |
|---|---|---|
| `SONARR_URL` | | Sonarr server URL (optional — Babel can run Plex-only) |
| `SONARR_API_KEY` | | Sonarr API key (Settings > General) |
| `PLEX_URL` | | Plex server URL |
| `PLEX_TOKEN` | | Plex authentication token |
| `SCAN_INTERVAL_HOURS` | `6` | Hours between automatic scans |
| `TARGET_LANGUAGE` | `eng` | ISO 639-2 language code to search for |
| `SEARCH_COOLDOWN_DAYS` | `7` | Days before re-searching an episode |
| `SEARCH_RATE_LIMIT` | `5` | Max Sonarr searches per minute |
| `DISCORD_WEBHOOK_URL` | | Discord webhook for notifications |
| `WEBHOOK_SECRET` | | If set, the Sonarr webhook requires `?apikey=` (or `X-Api-Key` header) to match |
| `AUTH_USERNAME` / `AUTH_PASSWORD` | | If both are set, the whole dashboard requires HTTP Basic Auth |
| `PUID` / `PGID` | `1000` / `1000` | UID/GID the container runs as — match your host's media/data ownership |
| `FFPROBE_TIMEOUT` | `30` | Seconds a single ffprobe may run before it is killed |
| `FFPROBE_MAX_CONCURRENT` | `4` | Max simultaneous ffprobe children — caps what a hung mount can strand |
| `FFPROBE_SLOT_TIMEOUT` | `15` | Seconds to wait for a free probe slot before skipping the file |
| `WATCHDOG_UNHEALTHY_LAG` | `15` | Event-loop lag (s) above which `/api/health` returns 503 |
| `WATCHDOG_ABORT_LAG` | `300` | Event-loop lag (s) after which Babel exits so Docker restarts it; `0` disables |

### Unresponsive media mounts

Babel reads audio tracks with `ffprobe`, so it is exposed to whatever storage
holds your media. If that storage stops responding — a suspended ZFS pool, a
dead NFS/SMB share — reads block indefinitely, and an unbounded probe can take
the whole app with it.

Babel is built so that a hung mount degrades instead of wedging:

- No filesystem call runs on the event loop. Only the `ffprobe` child touches
  the media path, and it can be killed. A blocked event loop would stop
  serving HTTP *and*, under uvloop, stop reaping exited children — which is how
  a storage blip turns into thousands of zombie processes.
- Every probe is bounded by `FFPROBE_TIMEOUT`, and no more than
  `FFPROBE_MAX_CONCURRENT` may be in flight, so a dead mount strands a handful
  of processes rather than one per episode.
- Killed children are always reaped, including when a scan is cancelled or the
  app shuts down mid-probe.
- Affected episodes are recorded as `UNKNOWN` and retried on the next scan;
  the dashboard stays responsive throughout.
- If the loop does stall anyway, the watchdog logs it, `/api/health` starts
  returning 503, and past `WATCHDOG_ABORT_LAG` the process exits so
  `restart: unless-stopped` can recover it. Docker never restarts a container
  for being *unhealthy* — only for exiting — so this is what closes that gap.

`scripts/repro_hung_mount.py` exercises all of the above against a fake mount
that never responds; run it with `--legacy` to see the pre-1.1.1 behaviour.

### Security

Babel has no built-in accounts and, by default, no authentication — anyone who
can reach the port can view and change settings (including your Sonarr API
key and Plex token). For anything beyond a trusted home LAN, do one of:

- Set `AUTH_USERNAME` + `AUTH_PASSWORD` to put the dashboard behind HTTP Basic Auth, or
- Put Babel behind a reverse proxy (Caddy, Traefik, Nginx Proxy Manager, etc.)
  that handles authentication.

`GET /api/health` and `POST /api/webhook/sonarr` are always reachable without
Basic Auth credentials (health checks and Sonarr can't complete an interactive
login) — set `WEBHOOK_SECRET` to authenticate the webhook instead.

### Sonarr Webhook (Recommended)

For instant upgrade detection instead of waiting for scan cycles:

1. In Sonarr, go to **Settings > Connect > + > Webhook**
2. **Name:** Babel
3. **URL:** `http://your-babel-container:8686/api/webhook/sonarr` (append
   `?apikey=your-secret` if `WEBHOOK_SECRET` is set)
4. **Events:** Enable *On Import* and *On Upgrade*
5. Click **Save**

## Unraid Installation

1. In the Unraid web UI, go to **Docker > Add Container**
2. Set **Repository** to `therealshadoh/babel:latest`
3. Configure ports (8686), appdata path, and environment variables
4. Click **Apply**
5. Access the web UI at `http://your-server:8686`

## How It Works

```
Scan Cycle:
  Sonarr ──> Fetch anime series + episodes
  │
  For each episode:
    ├── Check DB cache — unchanged files skip straight to their known status
    ├── Otherwise check audio tracks (Plex → ffprobe fallback; Plex's
    │   library index is only built the first time a scan actually needs
    │   it, so a cycle where nothing changed never touches Plex at all)
    ├── Classify: DUBBED / SUB_ONLY / MISSING
    └── If SUB_ONLY → trigger Sonarr search
  │
  Post-scan:
    ├── Check download queue status
    ├── Resolve stuck imports
    ├── Sync Sonarr tags + Plex collections
    └── Send Discord notifications

Webhook (real-time):
  Sonarr import event → re-check audio via ffprobe → resolve upgrade

Note: Sonarr is optional — with only Plex configured, Babel runs in a
read-only "Plex-only" mode (detection and collections, no searches).
```

## API

| Endpoint | Description |
|---|---|
| `GET /api/health` | System status, version, stats |
| `GET /api/activity` | Live download queue + recent upgrades |
| `POST /api/scan` | Trigger manual scan |
| `POST /api/scan/stop` | Cancel the running scan after the current series |
| `POST /api/webhook/sonarr` | Sonarr webhook receiver (`?apikey=` if `WEBHOOK_SECRET` is set) |
| `POST /api/check-downloads` | Check pending upgrade status |
| `POST /api/resolve-imports` | Fix stuck Sonarr imports |
| `POST /api/lookup-dubs` | Run MAL dub availability check |
| `POST /api/setup-sonarr-dub` | Create/assign a Sonarr custom format that prefers dual-audio releases |

## Data & Backups

Everything Babel needs to keep is under `/app/data` (the `babel-data` volume
in the Quick Start example): the SQLite database (`babel.db`) and rotating
application logs (`babel.log*`). Back up that volume to preserve scan
history, upgrade tracking, and settings saved via the web UI — the container
itself is stateless otherwise.

## Links

- [Docker Hub](https://hub.docker.com/r/therealshadoh/babel)
- [GitHub](https://github.com/TheRealShadoh/babel)
- [Issues](https://github.com/TheRealShadoh/babel/issues)
