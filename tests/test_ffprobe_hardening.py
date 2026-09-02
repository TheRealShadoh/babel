"""Regression tests for the hung-mount / zombie-process hardening.

Background: Babel accumulated thousands of zombie ffprobe children while a ZFS
pool under the media tree was suspended. Two properties have to hold for that
not to recur:

* nothing in the probe path performs a filesystem syscall on the event loop —
  under uvloop a stalled loop stops reaping children entirely; and
* every spawned child's exit status is eventually collected, on the timeout
  path and on the cancellation path alike, without the caller waiting forever.
"""

import asyncio
import os
import stat
import sys
import textwrap
from pathlib import Path

import pytest

from src.scanner import ffprobe

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="zombie assertions read /proc"
)

FAST = dict(timeout=1.0, kill_grace=0.5, max_concurrent=4, slot_timeout=1.0)


def _install_stub(tmp_path, body: str) -> None:
    """Put a fake `ffprobe` at the front of PATH for this test."""
    stub = tmp_path / "ffprobe"
    stub.write_text(textwrap.dedent(body).lstrip())
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    os.environ["PATH"] = f"{tmp_path}{os.pathsep}{os.environ['PATH']}"


def child_states() -> dict[str, int]:
    """Process states of our direct children, e.g. {'Z': 2, 'S': 1}."""
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


async def _settle(seconds: float = 2.0) -> None:
    """Give detached reaper tasks a chance to finish."""
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        if zombies() == 0 and not ffprobe._reapers:
            return
        await asyncio.sleep(0.05)


@pytest.fixture(autouse=True)
def _clean_path():
    original = os.environ["PATH"]
    yield
    os.environ["PATH"] = original


async def test_probe_does_no_filesystem_syscall_on_the_loop(tmp_path, monkeypatch):
    """The old code called Path(...).exists() inline — a blocking stat against
    the very mount that hangs. Nothing in this path may touch the filesystem."""
    _install_stub(tmp_path, """
        #!/bin/sh
        echo '{"streams":[{"codec_name":"aac","tags":{"language":"eng"}}]}'
    """)

    # Record rather than raise: these are stdlib functions pytest itself calls,
    # and blowing up inside them derails the test reporter instead of the test.
    calls: list[str] = []

    def watched(name, real):
        def _watch(*a, **k):
            calls.append(f"{name}{a[1:] if name.startswith('Path.') else a}")
            return real(*a, **k)
        return _watch

    for name, target, attr in (
        ("Path.exists", Path, "exists"),
        ("Path.stat", Path, "stat"),
        ("Path.is_file", Path, "is_file"),
        ("os.stat", os, "stat"),
    ):
        monkeypatch.setattr(target, attr, watched(name, getattr(target, attr)))

    tracks = await ffprobe.get_audio_tracks("/media/anything.mkv", **FAST)

    assert tracks == [{"language": "eng", "codec": "aac"}]
    assert calls == [], f"blocking filesystem syscalls ran on the event loop: {calls}"


async def test_missing_file_returns_none_without_stat(tmp_path):
    """Dropping the exists() pre-check must not change the caller's contract."""
    _install_stub(tmp_path, """
        #!/bin/sh
        echo "No such file or directory" >&2
        exit 1
    """)
    assert await ffprobe.get_audio_tracks("/media/gone.mkv", **FAST) is None
    await _settle()
    assert zombies() == 0


async def test_timed_out_probe_is_killed_and_reaped(tmp_path):
    """A probe that hangs must not wedge the caller and must not leak a zombie,
    even when the child ignores SIGTERM."""
    _install_stub(tmp_path, """
        #!/bin/sh
        trap '' TERM
        sleep 60
    """)

    loop = asyncio.get_running_loop()
    started = loop.time()
    result = await ffprobe.get_audio_tracks("/media/hung.mkv", **FAST)
    elapsed = loop.time() - started

    assert result is None
    # Caller is bounded by `timeout`, not by how long the child hangs.
    assert elapsed < 5, f"caller blocked for {elapsed:.1f}s"

    await _settle()
    assert zombies() == 0, f"leaked zombies: {child_states()}"


async def test_cancellation_kills_the_child(tmp_path):
    """The old code caught only TimeoutError, so a cancelled scan left ffprobe
    running with nobody to collect it. Cancellation must still reap."""
    _install_stub(tmp_path, """
        #!/bin/sh
        sleep 60
    """)

    task = asyncio.ensure_future(ffprobe.get_audio_tracks("/media/x.mkv", **FAST))
    await asyncio.sleep(0.4)          # let the child actually spawn
    assert child_states().get("S", 0) + child_states().get("R", 0) >= 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await _settle()
    assert zombies() == 0, f"leaked zombies: {child_states()}"
    assert child_states().get("S", 0) == 0, "child survived cancellation"


async def test_concurrency_is_capped_on_a_hung_mount(tmp_path):
    """A hung mount must leak a bounded number of stuck processes, not one per
    episode. This is what turns 2,040 zombies into at most FFPROBE_MAX_CONCURRENT."""
    _install_stub(tmp_path, """
        #!/bin/sh
        trap '' TERM
        sleep 60
    """)
    slow = dict(FAST, timeout=30.0, kill_grace=0.3, slot_timeout=0.5, max_concurrent=3)

    tasks = [
        asyncio.ensure_future(ffprobe.get_audio_tracks(f"/media/{i}.mkv", **slow))
        for i in range(25)
    ]
    await asyncio.sleep(2.0)

    live = child_states()
    running = live.get("S", 0) + live.get("R", 0) + live.get("D", 0)
    assert running <= 3, f"spawned {running} concurrent probes, cap was 3"

    # Everything beyond the cap fails fast instead of queueing forever.
    done = [t for t in tasks if t.done()]
    assert len(done) >= 20
    assert all(t.result() is None for t in done)

    # Teardown: cancelling the callers hands each child to a reaper, which
    # escalates to SIGKILL. Let them finish rather than cancelling them, so the
    # loop closes with no children left behind.
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.gather(*list(ffprobe._reapers), return_exceptions=True)
    await _settle(5.0)
    assert zombies() == 0, f"leaked zombies: {child_states()}"


def test_stalled_uvloop_does_not_leak_children_forever(tmp_path):
    """Production runs uvloop (uvicorn[standard] default), where child reaping
    is driven by the loop. This pins the property that matters: after the loop
    resumes, every child we spawned has been reaped."""
    uvloop = pytest.importorskip("uvloop")
    _install_stub(tmp_path, """
        #!/bin/sh
        exit 0
    """)

    async def main():
        for _ in range(10):
            await ffprobe.get_audio_tracks("/media/ok.mkv", **FAST)
        await _settle()
        return zombies()

    try:
        assert uvloop.run(main()) == 0
    finally:
        os.environ["PATH"] = os.environ["PATH"]


async def test_slot_is_freed_when_the_child_dies_not_when_its_pipes_close(tmp_path):
    """A grandchild that inherited stdout must not pin a probe slot.

    asyncio's Process.wait() only resolves once the process has exited *and*
    every pipe has closed, so waiting on it would hold the slot for as long as
    some unrelated process keeps stdout open (measured: 59s for a `sleep 60`
    grandchild). The reaper waits on returncode instead.
    """
    _install_stub(tmp_path, """
        #!/bin/sh
        sleep 60 &
        sleep 60
    """)

    loop = asyncio.get_running_loop()
    assert await ffprobe.get_audio_tracks("/media/pipes.mkv", **FAST) is None

    started = loop.time()
    while ffprobe._reapers and loop.time() - started < 5:
        await asyncio.sleep(0.05)

    assert not ffprobe._reapers, (
        f"probe slot still pinned after {loop.time() - started:.1f}s by a "
        "process holding the child's pipes open"
    )
