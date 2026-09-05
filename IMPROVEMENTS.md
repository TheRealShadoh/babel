# Babel — Code Review & Improvement Task List (v1.1.0 review, historical)

> This is the record of the review done against v1.1.0 and is kept for
> history. It is not current: see `CHANGELOG.md` for what has shipped
> since, including the 1.2.x production review that found and fixed the
> Sonarr-restart data loss, the media-only fallback pruning, and the
> issues listed there. The test suite is now 190+ tests across 20 files.

Ground-up review of v1.1.0 (7dc1768). Tasks are ordered by priority within each tier:
**P0** = bugs / data-loss risks, **P1** = security, **P2** = performance & robustness,
**P3** = architecture & code quality, **P4** = testing / CI / ops / docs.

Status: all items addressed as of the follow-up pass below (33 tests added,
see `tests/`). A few items were resolved with a different mechanism than
originally suggested, or partially scoped down — noted inline where that's
the case.

---

## P0 — Correctness bugs

- [x] **Cancelled Plex-only scan deletes unscanned series.** Fixed — `_scan_plex_only`
  now guards `delete_series_not_in`/`delete_episodes_not_in` behind `if not cancelled`,
  matching the Sonarr path. Regression-tested in `tests/test_scan_plex_only.py`.

- [x] **Sonarr webhook triggers a full Plex index rebuild per episode.** Fixed —
  the webhook handler no longer touches `PlexClient` at all; it checks audio via
  ffprobe directly (fast, single-file, no library crawl), consistent with the rest
  of the fallback chain.

- [x] **Webhook stores `relativePath` as the episode file path.** Fixed — prefers
  `episodeFile.path`, falling back to `relativePath` only if `path` is absent.
  Covered in `tests/test_webhook.py`.

- [x] **Scheduler ignores settings changed in the web UI.** Fixed — `start_scheduler()`
  now reads effective settings, and saving Settings calls `reschedule_scan()` to update
  the running APScheduler job without a restart. Verified live (boot test + settings
  save moved `nextScan`).

- [x] **App cannot boot without `SONARR_URL`.** Fixed — defaults to `""`; Plex-only
  mode boots cleanly (verified).

- [x] **`upgrade_tracking` joined to series by title string.** Fixed — added a
  `series_id` column (+ migration with title-based backfill for existing rows) and
  updated `get_recently_dubbed_series` to join on it.

- [x] **Plex-only episode IDs derived from `hash()`.** Fixed — uses the episode's own
  Plex `ratingKey` (deterministic, not `PYTHONHASHSEED`-dependent).

- [x] **`docker-compose.yml` ships broken defaults.** Fixed in both `docker-compose.yml`
  and `docker-compose.dev.yml`.

- [x] **Schema migrations swallow every exception.** Fixed — `init_db` now checks
  `PRAGMA table_info` per column and only adds what's missing, instead of a blanket
  `except Exception: pass`. (This surfaced a follow-on bug: two new indexes on
  migration-added columns were initially placed in the eagerly-run `SCHEMA_SQL`,
  which broke upgrading an *existing* DB — `CREATE INDEX` ran before the `ALTER TABLE`
  that added the column. Fixed by moving those indexes into a `POST_MIGRATION_INDEX_SQL`
  script run after migrations; verified against a hand-built pre-migration DB fixture.)

## P1 — Security

- [x] **No authentication anywhere.** Added optional HTTP Basic Auth middleware
  (`src/web/auth.py`), gated on `AUTH_USERNAME`/`AUTH_PASSWORD` env vars, exempting
  `/api/health`, `/api/webhook/sonarr`, and `/static/*`. Documented reverse-proxy
  alternative in the README.

- [x] **XSS via untrusted strings interpolated into raw HTML.** All identified spots
  (activity feed, `/api/logs`, ignore-list, scan-progress card, dub-format setup
  message) now escape via `markupsafe.escape()`; the activity feed additionally moved
  to a Jinja partial (auto-escaping). Regression-tested in
  `tests/test_activity_route.py::test_activity_html_escapes_untrusted_release_title`.

- [x] **Vendor htmx instead of loading from unpkg.** Downloaded to
  `src/web/static/htmx.min.js`, served from `/static`.

- [x] **Run the container as a non-root user + PUID/PGID.** Added an `entrypoint.sh`
  (gosu-based, matching the LinuxServer.io pattern) and a `babel` user in the
  Dockerfile. Verified: default run is UID 1000, `PUID=2000`/`PGID=2000` correctly
  reassigns ownership and the process UID.

- [x] **Webhook secret handling.** Now uses `hmac.compare_digest` and reads
  `WEBHOOK_SECRET` as a proper effective-settings field (env var *or* DB override),
  rather than only the DB value.

## P2 — Performance & robustness

- [x] **Plex index rebuild reloads every episode, every scan.** Addressed via the
  "only index what's needed" option from the two suggested — Plex's index is now
  built lazily on first actual need (`_ensure_plex_index`) instead of unconditionally
  before the series loop, so a scan where every episode is cache-hit never touches
  Plex at all (verified: `test_cache_hit_never_builds_plex_index`,
  `test_lazy_plex_index_builds_once_when_needed`). The other suggested option — a raw
  bulk `includeStreams=1` HTTP fetch bypassing PlexAPI's object model — was
  deliberately **not** attempted: PlexAPI's own docs confirm search results are
  "partial objects" that don't include Stream data, so that path needs a live Plex
  server to validate safely, which wasn't available here.

- [x] **Commit-per-statement DB writes.** Batched — hot-path `models.py` writes take
  an optional `commit: bool = True` param; the scan engine passes `commit=False`
  throughout a series' processing and commits once per series.

- [x] **`PRAGMA busy_timeout`.** Set to 5000ms on every connection (`get_db`,
  `get_db_ctx`, and the `init_db` connection).

- [x] **`/api/search-all/{series_id}` blocks the HTTP request for up to minutes.**
  Fixed — batches into one `search_episodes([...])` call (Sonarr already accepts a
  list), eliminating the per-episode sleep loop entirely rather than just
  backgrounding it.

- [x] **`get_effective_settings()` opens a fresh SQLite connection every call.**
  Cached with a 5s TTL; invalidated explicitly on settings save / connection-test
  auto-save.

- [x] **N+1 queries in hot paths.** `get_scan_detail` now uses a correlated subquery
  (one round trip); `get_activity_html`/`get_activity` batch-fetch poster URLs via
  `get_poster_urls_for_series`.

- [x] **Duplicate queue fetches & fragile scan-detail correlation.** The fragile
  correlation is fixed — `search_history` and `upgrade_tracking` now carry a real
  `scan_id` column, and `get_scan_detail` joins on it directly instead of a
  timestamp-window heuristic (regression-tested:
  `test_scan_detail_correlates_by_scan_id_not_timestamp_window`, which specifically
  covers the overlapping-manual-search case). **Deferred:** de-duplicating the
  Sonarr `/queue` fetch across `check_download_status` and the activity endpoints —
  each still fetches independently. Sonarr's queue endpoint is cheap; a shared cache
  here would add invalidation complexity disproportionate to the benefit.

- [x] **Harden endpoints when Sonarr/Plex are unconfigured.** `/api/activity`,
  `/api/activity/html`, `/api/search/{id}`, `/api/search-all/{id}`,
  `/api/setup-sonarr-dub` all short-circuit with a friendly message now (verified
  live with `SONARR_URL` unset).

- [x] **Stuck-import heuristics are string-matching Sonarr UI messages.** Added:
  unmatched-message logging (so new Sonarr wording surfaces instead of silently doing
  nothing), a `STUCK_IMPORT_DRY_RUN` setting (simulates categorization without calling
  any mutating Sonarr endpoint) exposed in the Settings UI, and fixture-based tests
  covering every category branch (`tests/test_stuck_imports.py`).

## P3 — Architecture & code quality

- [x] **Decompose the scan engine.** `_scan_with_sonarr` split into
  `_process_series_episodes` (per-series orchestration), `_classify_episode_audio`
  (cache/Plex/ffprobe classification), `_filter_max_attempts`, and `_ensure_plex_index`,
  backed by a `ScanStats` dataclass replacing 9 counter parameters that — since ints
  are immutable in Python — were silently dead (always reset to 0, never actually
  threaded through). Covered by 11 tests in `tests/test_scan_engine.py`.

- [x] **Remove the `locals().get("tracks_from_cache")` hack.** Gone — `tracks_from_cache`
  is now an ordinary variable local to `_classify_episode_audio`.

- [x] **Move Python-string HTML into Jinja partials.** Done for the activity feed
  (`src/web/templates/partials/activity_feed.html`, the largest and highest-risk one —
  it renders untrusted Sonarr release titles). `_render_ignore_list`, the
  scan-progress card, and flash-message strings remain Python f-strings; their XSS
  risk is already mitigated via explicit `esc()` calls from the P1 pass, so this is a
  cosmetic/duplication cleanup left as a follow-up rather than a correctness gap.

- [x] **Unify `/api/activity` (JSON) and `/api/activity/html`.** Extracted
  `_build_activity_data()`; both endpoints now share it (verified:
  `test_activity_json_matches_html_data`).

- [x] **Delete dead `config.translate_path`.** Consolidated — `config.translate_path`
  is now the one canonical cfg-dict-based implementation; `engine._translate_path`
  was deleted and all call sites (including the webhook handler) use the shared one.

- [x] **Single-source the version.** Added `src/__version__`; used in `/api/health`
  and the UI footer via a Jinja global (`babel_version`).

- [x] **Typed settings access.** Implemented as a `cfg_bool()` helper (centralizes the
  `"true"`/`"false"` string interpretation with a documented default) rather than a
  full typed-schema rewrite of `get_effective_settings()` — lower blast radius across
  the many `cfg.get(...)` call sites. This surfaced a real, previously-undiscovered bug
  in the process: `SHOW_THUMBNAILS`, `MAX_SEARCH_ATTEMPTS`, `AUTO_TAG_SONARR`,
  `DISCORD_WEBHOOK_URL`, `AUTO_COLLECTIONS_PLEX`, and `AUTO_RESOLVE_IMPORTS` were never
  declared as pydantic `Settings` fields, so `get_effective_settings()` silently
  dropped their DB-saved values — anyone who changed these via the Settings page was
  being ignored. Fixed by declaring them properly; verified with a live before/after
  reproduction.

- [x] **`series_detail` builds the Sonarr link from env-only settings.** Fixed — uses
  effective settings now. Also added a `title_slug` column (populated from Sonarr's
  real `titleSlug` at scan time, migrated + backfilled as `NULL` for pre-existing
  rows) so the link uses Sonarr's actual slug instead of a folder-name guess, falling
  back to the old guess only for rows scanned before this column existed.

- [x] **Logging setup duplicate handlers.** Guarded via a named handler check so
  repeated lifespan runs (`--reload`, multiple app instances in one process) don't
  stack duplicate `RotatingFileHandler`s.

- [x] **Global mutable scan state assumes one worker.** Documented in place
  (`src/scanner/engine.py`, above `_scan_lock`) rather than adding a runtime assertion,
  since the Dockerfile's CMD already hard-codes a single uvicorn process with no
  `--workers` flag.

## P4 — Testing, CI, ops, docs

- [x] **Add a test suite.** 33 tests across 6 files: scan classification/cooldown/
  upgrade logic with fake Sonarr/Plex (`test_scan_engine.py`, `test_scan_plex_only.py`),
  `models.py` CRUD (`test_models.py`), stuck-import categorization with fixture queue
  items (`test_stuck_imports.py`), webhook handler happy-path + auth
  (`test_webhook.py`), and the activity routes including an XSS regression test
  (`test_activity_route.py`). Uses `pytest` + `pytest-asyncio`; `respx` is available
  but wasn't needed since the fakes cover the Sonarr/Plex boundary directly.

- [x] **Make CI validate before publishing.** Added a `test` job (ruff + pytest) that
  the `build` job now `needs:`, gating the Docker push.

- [x] **Add `pyproject.toml`.** Project metadata, ruff config, pytest config
  (replacing the standalone `pytest.ini`), and a `dev` optional-dependencies group.

- [x] **Add a LICENSE file.** MIT.

- [x] **Multi-arch Docker builds.** `linux/amd64,linux/arm64` via
  `docker/setup-qemu-action` + `platforms:` in the existing buildx step.

- [x] **DB backup/ops niceties.** Documented in the README ("Data & Backups" section).
  **Not implemented:** a `VACUUM`/`PRAGMA optimize` pass after cleanup, and exposing
  scan/cleanup intervals in Settings — both were "consider" suggestions rather than
  bugs; left as future enhancements.

- [x] **README/API drift check.** Documented `WEBHOOK_SECRET`, the `?apikey=` query
  param, `AUTH_USERNAME`/`AUTH_PASSWORD`, `PUID`/`PGID`, and added the previously
  undocumented `/api/scan/stop` and `/api/setup-sonarr-dub` endpoints to the API table.

---

## Follow-up — uvicorn zombie processes under I/O stall (2026-09-02)

Production incident: 2,040 zombie processes accumulated over ~2.5 days, all with
`PPID` = the uvicorn process, while a ZFS pool under part of the media tree was
suspended. Docker reported the container `unhealthy` for the whole window and
nothing acted on it. Every zombie was reaped the instant I/O was unblocked.

### Verified in code

- **Only one subprocess spawn site exists** — `src/scanner/ffprobe.py`, reached
  from the scan engine (`_classify_episode_audio`) and the Sonarr webhook route.
  There was **no unconditional fire-and-forget spawn**: the normal and timeout
  paths both waited on the child.
- **`Path(file_path).exists()` ran inline on the event loop**, against the media
  path — the one blocking filesystem call in the probe path, and the only one
  anywhere in the app that touches a mounted path. `plex.py` already routed its
  blocking work through `asyncio.to_thread`.
- **`await proc.wait()` after `kill()` was unbounded.** Measured: with a 1s probe
  timeout, the call returned after **59.0s**, because `Process.wait()` resolves
  only once the process has exited *and* every pipe has closed.
- **`except asyncio.TimeoutError` did not catch `CancelledError`**, so a scan
  cancelled mid-probe (scheduler shutdown, client disconnect) propagated out
  leaving the child running with nobody to collect it — a fire-and-forget spawn
  on the cancellation path only.
- **uvicorn was PID 1** (`entrypoint.sh` ends in `exec gosu babel "$@"`).

### Verified experimentally

- Under **uvloop** (which `uvicorn[standard]` selects by default), a blocked
  event-loop thread stops child reaping entirely: 20 of 20 exited children stayed
  zombies for the whole stall and were reaped the moment the loop resumed. Under
  stdlib asyncio's `ThreadedChildWatcher` the same test leaks nothing. This
  reproduces the incident's signature, including the batch reap on recovery.
- A uvloop process that is PID 1 (or a child-subreaper) **never** reaps orphans
  reparented to it, even with the loop running normally.

### Corrections to the original hypothesis

- The blocked-loop theory is **confirmed**, with the added condition that it
  depends on uvloop; the same code on stdlib asyncio would not have leaked.
- **An init shim would not have prevented this leak.** The zombies' parent
  (uvicorn) never died, so they were never reparented to PID 1 and `tini` would
  never have seen them. `tini` is added for signal forwarding and genuine
  orphans, but it is not a backstop for this failure.
- The healthcheck was **not** broken: a stalled loop cannot answer HTTP, so
  `unhealthy` was correct. The gap was that nothing acts on `unhealthy` —
  `restart: unless-stopped` only reacts to the container exiting.

### Changes

- [x] `src/scanner/ffprobe.py` rewritten: no filesystem syscall on the loop, a
  bounded slot acquire, a bounded probe timeout, SIGTERM→SIGKILL escalation, a
  detached reaper that waits on `returncode` (not `wait()`, which is pinned by
  pipe holders), and reaping on the cancellation path.
- [x] `src/watchdog.py`: a loop-lag heartbeat plus a daemon thread that logs a
  stall, drives `/api/health` to 503, and hard-exits past `WATCHDOG_ABORT_LAG`.
- [x] `src/web/routes.py`: `/api/health` reports `loopLagSeconds` and returns 503
  when degraded; its `os.path.getsize`/`exists` and the log-tail `open()` moved
  to `asyncio.to_thread`.
- [x] `Dockerfile`: `tini` as PID 1; healthcheck gains `--max-time` and
  `--start-period`.
- [x] `docker-compose*.yml`: `init: true`, plus the new knobs documented.
- [x] `tests/test_ffprobe_hardening.py`, `tests/test_watchdog.py` — 10 tests.
  Four of the ffprobe tests fail against the pre-fix implementation.
- [x] `scripts/repro_hung_mount.py` — end-to-end repro against a fake mount that
  never responds. Passes on the fix; phases A and A2 fail with `--legacy`.

### Known limits

- The repro's "hung mount" is a set of FIFOs plus an injected blocking `stat()`.
  That reproduces indefinite blocking but not uninterruptible (D-state) sleep,
  which needs a real broken mount and privileges. The design covers D-state by
  bounding concurrency and detaching the reaper rather than waiting on it.
- The incident report noted `ffprobe` processes among the blocked set but did not
  confirm they belonged to Babel rather than Plex. Nothing here depends on that:
  the zombies' `PPID` was uvicorn's, which is sufficient on its own.

