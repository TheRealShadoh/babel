#!/usr/bin/env python3
"""Repro harness for the uvicorn zombie-process incident.

Runs the *real* Babel app under uvicorn+uvloop against a fake media "mount"
whose reads never return, then measures the two things that went wrong in
production:

  Phase A — hung mount: fire many probe-triggering webhooks at the app and
            watch the child-process table. Zombie count must stay flat and the
            number of stuck ffprobe children must stay under
            FFPROBE_MAX_CONCURRENT. Under the pre-fix code it climbs without
            bound.

  Phase B — stalled event loop: block the loop the way an inline stat() against
            a suspended pool does, and poll /api/health. It must go unhealthy
            and then recover on its own.

  Phase C — self-heal: with WATCHDOG_ABORT_LAG set, a loop stalled past the
            limit must make the process exit(70) so the container's restart
            policy can recover it. Docker never restarts a merely *unhealthy*
            container, which is why the 2.5-day outage went unattended.

Usage:
    python3 scripts/repro_hung_mount.py            # verify the fix
    python3 scripts/repro_hung_mount.py --legacy   # show the original failure

The "hung mount" is a directory of FIFOs: opening one for reading blocks until
a writer appears, which never happens. That reproduces an unresponsive mount
faithfully apart from being interruptible — a real D-state process cannot be
killed at all, which the code handles by bounding concurrency and detaching the
reaper rather than waiting on it.
"""

import argparse
import asyncio
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PORT = int(os.environ.get("REPRO_PORT", "8787"))
BASE = f"http://127.0.0.1:{PORT}"


# --------------------------------------------------------------------------
# fake environment
# --------------------------------------------------------------------------

def build_hung_mount(root: Path, count: int) -> list[Path]:
    """A directory whose files block forever on read()."""
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(count):
        p = root / f"show.s01e{i:02d}.mkv"
        if not p.exists():
            os.mkfifo(p)
        paths.append(p)
    return paths


def build_readable_files(root: Path, count: int) -> list[Path]:
    """Files on the same mount that read fine — so probes spawn AND exit."""
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(count):
        p = root / f"fast.s01e{i:02d}.mkv"
        p.write_bytes(b"x")
        paths.append(p)
    return paths


def install_stat_stall(mount: Path, every: int = 3, secs: float = 2.0) -> None:
    """Make stat() under *mount* block, the way a suspended ZFS pool does.

    Only some calls block: on a partially suspended pool a path whose dentry is
    still cached returns instantly while an uncached one blocks. That mix is
    what lets the app keep spawning probes *between* stalls, which is the
    precondition for children piling up unreaped.

    Installed by the harness, not by the app — it patches pathlib globally but
    only acts on paths under the fake mount.
    """
    import itertools
    import pathlib

    counter = itertools.count()
    real_exists = pathlib.Path.exists
    prefix = str(mount)

    def patched(self, *a, **k):
        if str(self).startswith(prefix) and next(counter) % every == 0:
            time.sleep(secs)     # blocks whichever thread called it
        return real_exists(self, *a, **k)

    pathlib.Path.exists = patched


def install_probe_stub(bindir: Path) -> str:
    """Use the real ffprobe when available; otherwise a stub that reads the
    file, which is the part that blocks on a hung mount."""
    real = shutil.which("ffprobe")
    if real:
        return f"real ffprobe ({real})"
    bindir.mkdir(parents=True, exist_ok=True)
    stub = bindir / "ffprobe"
    stub.write_text(
        "#!/bin/sh\n"
        "# Stand-in for ffprobe: the last argument is the media file, and the\n"
        "# blocking part of a real probe is reading it. REPRO_PROBE_SECONDS\n"
        "# gives the probe a known duration so Phase A2 can line up a child's\n"
        "# exit with a stalled event loop.\n"
        'for f; do :; done\n'
        'sleep "${REPRO_PROBE_SECONDS:-0}"\n'
        'cat "$f" >/dev/null 2>&1\n'
        'echo \'{"streams":[]}\'\n'
    )
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    os.environ["PATH"] = f"{bindir}{os.pathsep}{os.environ['PATH']}"
    return f"stub ffprobe ({stub})"


# --------------------------------------------------------------------------
# process inspection
# --------------------------------------------------------------------------

def child_states() -> dict[str, int]:
    me = os.getpid()
    out: dict[str, int] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as f:
                fields = f.read().rsplit(")", 1)[1].split()
            if int(fields[1]) == me:
                out[fields[0]] = out.get(fields[0], 0) + 1
        except (OSError, IndexError, ValueError):
            continue
    return out


def zombies() -> int:
    return child_states().get("Z", 0)


def live_children() -> int:
    s = child_states()
    return s.get("S", 0) + s.get("R", 0) + s.get("D", 0)


# --------------------------------------------------------------------------
# app under test
# --------------------------------------------------------------------------

_loop: asyncio.AbstractEventLoop | None = None


def start_app(db_path: str) -> threading.Thread:
    import uvicorn
    import uvloop
    from src.main import app

    def run():
        global _loop
        _loop = uvloop.new_event_loop()
        asyncio.set_event_loop(_loop)
        config = uvicorn.Config(
            app, host="127.0.0.1", port=PORT, log_level="warning", lifespan="on",
        )
        server = uvicorn.Server(config)
        server.install_signal_handlers = lambda: None   # not the main thread
        _loop.run_until_complete(server.serve())

    t = threading.Thread(target=run, name="babel-app", daemon=True)
    t.start()
    return t


def seed_db(db_path: str, files: list[Path]) -> None:
    from src.db.database import get_db, init_db
    from src.db import models

    async def _seed():
        await init_db(db_path)
        db = await get_db(db_path)
        try:
            await models.upsert_series(db, 1, "Repro Show")
            for i, f in enumerate(files):
                await models.upsert_episode(db, 100 + i, 1, 1, i, f"E{i}", str(f), 1)
        finally:
            await db.close()

    asyncio.run(_seed())


def wait_for_health(timeout: float = 20.0) -> None:
    import httpx
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{BASE}/api/health", timeout=2).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise SystemExit("app did not become healthy")


def probe_health() -> str:
    """'ok' / 'degraded' / 'unreachable' — what the Docker healthcheck sees."""
    import httpx
    try:
        r = httpx.get(f"{BASE}/api/health", timeout=2.0)
    except Exception:
        return "unreachable"
    return "ok" if r.status_code == 200 else f"degraded({r.status_code})"


# --------------------------------------------------------------------------
# phases
# --------------------------------------------------------------------------

def phase_a(files: list[Path], cap: int) -> bool:
    import httpx

    print("\n--- Phase A: probes against a mount that never responds ---")
    n = 40
    peak_z = peak_live = 0
    done = threading.Event()

    def sample():
        nonlocal peak_z, peak_live
        while not done.is_set():
            peak_z = max(peak_z, zombies())
            peak_live = max(peak_live, live_children())
            time.sleep(0.1)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()

    def fire(i: int):
        payload = {
            "eventType": "Download",
            "series": {"id": 1, "title": "Repro Show"},
            "episodes": [{"id": 100 + i, "seasonNumber": 1, "episodeNumber": i, "title": f"E{i}"}],
            "episodeFile": {"path": str(files[i % len(files)]), "size": 1},
        }
        try:
            httpx.post(f"{BASE}/api/webhook/sonarr", json=payload, timeout=60)
        except Exception:
            pass

    threads = [threading.Thread(target=fire, args=(i,), daemon=True) for i in range(n)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    health_during = []
    for _ in range(12):
        health_during.append(probe_health())
        time.sleep(1.0)
    for t in threads:
        t.join(timeout=30)
    done.set()
    sampler.join(timeout=2)

    # let detached reapers finish
    for _ in range(100):
        if zombies() == 0:
            break
        time.sleep(0.1)

    print(f"  {n} concurrent probes issued over {time.monotonic() - t0:.0f}s")
    print(f"  peak zombie children      : {peak_z}")
    print(f"  peak live probe children  : {peak_live}   (cap = {cap})")
    print(f"  zombies now               : {zombies()}")
    print(f"  health while mount hung   : {' '.join(sorted(set(health_during)))}")

    ok = peak_z <= 2 and peak_live <= cap + 1 and zombies() == 0 and \
        all(h == "ok" for h in health_during)
    print(f"  => {'PASS' if ok else 'FAIL'}: zombie count flat, concurrency capped, "
          f"app still serving")
    return ok


def phase_a2(files: list[Path], mount: Path) -> bool:
    """The production mechanism, end to end.

    Children that exit while the loop is stalled are the ones that pile up:
    under uvloop, reaping is driven by the loop, so nothing is collected until
    it turns again. The pre-fix code stalls the loop itself, on its inline
    Path.exists() against the media path.
    """
    import httpx

    print("\n--- Phase A2: probes while stat() on the mount intermittently blocks ---")
    # A known probe duration, shorter than a stall, so children reliably exit
    # while the loop is blocked — the exact condition that strands them.
    os.environ["REPRO_PROBE_SECONDS"] = "1"
    install_stat_stall(mount, every=3, secs=3.0)

    peak_z = 0
    done = threading.Event()

    def sample():
        nonlocal peak_z
        while not done.is_set():
            peak_z = max(peak_z, zombies())
            time.sleep(0.02)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()

    def fire(i: int):
        payload = {
            "eventType": "Download",
            "series": {"id": 1, "title": "Repro Show"},
            "episodes": [{"id": 200 + i, "seasonNumber": 1, "episodeNumber": i, "title": f"F{i}"}],
            "episodeFile": {"path": str(files[i % len(files)]), "size": 1},
        }
        try:
            httpx.post(f"{BASE}/api/webhook/sonarr", json=payload, timeout=90)
        except Exception:
            pass

    threads = [threading.Thread(target=fire, args=(i,), daemon=True) for i in range(60)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    done.set()
    sampler.join(timeout=2)

    for _ in range(100):
        if zombies() == 0:
            break
        time.sleep(0.1)

    print(f"  peak zombie children during stalls : {peak_z}")
    print(f"  zombies after the mount recovers   : {zombies()}")
    ok = peak_z <= 2
    print(f"  => {'PASS' if ok else 'FAIL'}: children are reaped even while the "
          f"mount misbehaves")
    return ok


def phase_b(stall: float) -> bool:
    print("\n--- Phase B: event loop stalled (as an inline stat() on a dead mount would) ---")
    assert _loop is not None
    _loop.call_soon_threadsafe(lambda: time.sleep(stall))
    time.sleep(0.3)

    seen = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < stall + 6:
        status = probe_health()          # a probe costs up to its own timeout
        seen.append(status)
        print(f"  t+{time.monotonic() - t0:5.1f}s  healthcheck: {status}")
        time.sleep(1.0)

    went_bad = any(s != "ok" for s in seen)
    recovered = seen[-1] == "ok"
    print(f"  => {'PASS' if went_bad and recovered else 'FAIL'}: "
          f"unhealthy during stall = {went_bad}, recovered = {recovered}")
    return went_bad and recovered


def phase_c() -> bool:
    print("\n--- Phase C: watchdog self-heal (WATCHDOG_ABORT_LAG) ---")
    env = dict(os.environ, WATCHDOG_ABORT_LAG="3", WATCHDOG_UNHEALTHY_LAG="1",
               REPRO_PORT=str(PORT + 1))
    proc = subprocess.run(
        [sys.executable, __file__, "--stall-and-die"],
        env=env, capture_output=True, text=True, timeout=120,
    )
    print("  " + (proc.stdout.strip().replace("\n", "\n  ") or "(no output)"))
    ok = proc.returncode == 70
    print(f"  process exit code = {proc.returncode} (expected 70)")
    print(f"  => {'PASS' if ok else 'FAIL'}: a wedged loop exits so the restart "
          f"policy can recover it")
    return ok


def stall_and_die() -> None:
    """Child mode for phase C: start the app, wedge the loop, never return."""
    tmp = tempfile.mkdtemp(prefix="babel-repro-c-")
    os.environ["DB_PATH"] = f"{tmp}/babel.db"
    seed_db(os.environ["DB_PATH"], [])
    start_app(os.environ["DB_PATH"])
    wait_for_health()
    print("app healthy; wedging the event loop", flush=True)
    assert _loop is not None
    _loop.call_soon_threadsafe(lambda: time.sleep(600))
    time.sleep(60)
    print("watchdog did NOT fire", flush=True)
    sys.exit(1)


# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--legacy", action="store_true",
                    help="run against the pre-fix ffprobe to show the original failure")
    ap.add_argument("--stall-and-die", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="babel-repro-"))
    cap = 4
    os.environ.setdefault("SONARR_URL", "")
    os.environ.setdefault("PLEX_URL", "")
    os.environ["FFPROBE_TIMEOUT"] = "4"
    os.environ["FFPROBE_KILL_GRACE"] = "1"
    os.environ["FFPROBE_MAX_CONCURRENT"] = str(cap)
    os.environ["FFPROBE_SLOT_TIMEOUT"] = "2"
    os.environ["WATCHDOG_UNHEALTHY_LAG"] = "2"
    os.environ.setdefault("WATCHDOG_ABORT_LAG", "0")   # phase B needs recovery
    os.environ["DB_PATH"] = str(tmp / "babel.db")

    which = install_probe_stub(tmp / "bin")

    if args.stall_and_die:
        stall_and_die()
        return 1

    if args.legacy:
        import src.scanner.ffprobe as ff
        from scripts import _legacy_ffprobe
        ff.get_audio_tracks = _legacy_ffprobe.get_audio_tracks
        print("!! running with the PRE-FIX ffprobe implementation")

    mount = tmp / "media"
    files = build_hung_mount(mount, 8)
    fast_files = build_readable_files(mount, 8)
    print(f"hung mount : {mount} ({len(files)} FIFOs that never return)")
    print(f"probe      : {which}")
    print(f"caps       : max_concurrent={cap} timeout=4s slot_timeout=2s")

    seed_db(os.environ["DB_PATH"], files)
    start_app(os.environ["DB_PATH"])
    wait_for_health()
    print("app is up and healthy")

    results = {
        "A hung mount / concurrency capped": phase_a(files, cap),
        "A2 stalling stat() / zombies flat": phase_a2(fast_files, mount),
        "B stall -> unhealthy -> recover": phase_b(8),
        "C watchdog self-heal": phase_c(),
    }

    print("\n================ SUMMARY ================")
    for name, ok in results.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    shutil.rmtree(tmp, ignore_errors=True)
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
