# Babel — Media Dub Detection & Upgrade Tool

Babel monitors your media library via **Sonarr** and **Plex**, detects sub-only episodes, and automatically searches for English dubbed versions. Named after the Tower of Babel — bridging the language gap in your media library.

## Features

- **Audio Track Detection** — Scans Plex for audio streams, identifies Japanese-only (sub) vs English (dubbed) episodes
- **Automatic Dub Searching** — Triggers Sonarr searches for dubbed releases when sub-only episodes are found
- **Upgrade Tracking** — Monitors if Sonarr downloads are actually dubbed, auto-retries failed upgrades
- **Dub Intelligence** — Queries MyAnimeList to check if English dubs even exist before searching
- **Sonarr Auto-Tagging** — Tags series as `babel:dubbed`, `babel:partial-dub`, `babel:sub-only`
- **Plex Collections** — Auto-creates "Dubbed Anime", "Sub-Only Anime" collections
- **Stuck Import Resolution** — Detects and force-imports stuck Sonarr downloads
- **Discord Notifications** — Webhook alerts when dubs are found
- **Beautiful Dashboard** — Dark-themed web UI with poster art, progress tracking, and live scan status

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

Open `http://your-server:8686` to access the dashboard.

## Configuration

All settings can be configured via environment variables or the web UI (Settings page).

| Variable | Default | Description |
|----------|---------|-------------|
| `SONARR_URL` | — | Sonarr server URL |
| `SONARR_API_KEY` | — | Sonarr API key |
| `PLEX_URL` | — | Plex server URL |
| `PLEX_TOKEN` | — | Plex authentication token |
| `SCAN_INTERVAL_HOURS` | `6` | Hours between automatic scans |
| `TARGET_LANGUAGE` | `eng` | ISO 639-2 language code |
| `SEARCH_COOLDOWN_DAYS` | `7` | Days between re-searching the same episode |
| `DISCORD_WEBHOOK_URL` | — | Discord webhook for notifications |

## Unraid

Install via Community Applications or manually using the template at:
`https://github.com/TheRealShadoh/babel/tree/master/unraid`

## Links

- **GitHub**: [TheRealShadoh/babel](https://github.com/TheRealShadoh/babel)
- **Issues**: [github.com/TheRealShadoh/babel/issues](https://github.com/TheRealShadoh/babel/issues)
