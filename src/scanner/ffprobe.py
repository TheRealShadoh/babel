"""ffprobe wrapper.

Every probe here runs against a media path that may live on a network share or
a ZFS pool. When such a mount stalls, reads block in uninterruptible sleep, and
a naive implementation takes the whole process down with it:

* A synchronous ``stat()``/``exists()`` on the media path blocks the **event
  loop thread**. Under uvloop — which ``uvicorn[standard]`` selects by default —
  child-process reaping is driven entirely by the loop (libuv handles SIGCHLD
  from its own event callback), so a stalled loop stops reaping exited children
  and they accumulate as zombies until the loop turns again.
* ``proc.kill()`` followed by an unbounded ``await proc.wait()`` never returns
  for a child wedged in uninterruptible I/O: SIGKILL stays pending until the
  read returns, so the caller hangs indefinitely.

This module therefore:

1. performs **no filesystem syscall on the event loop** — the only thing that
   touches the media path is the ffprobe child, which we can kill;
2. bounds every wait the caller is exposed to;
3. never abandons a child without arranging for its exit status to be
   collected, including on cancellation; and
4. caps how many probes may be in flight, so a hung mount leaks a bounded
   number of stuck processes instead of an unbounded one.
"""

import asyncio
import json
import logging
import weakref

from src.config import get_settings, normalize_language

logger = logging.getLogger(__name__)

# Concurrency gates, one per event loop. Keyed weakly so the test suite (which
# builds and discards a loop per test) never reuses a semaphore whose waiters
# belong to a dead loop.
_slots: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = (
    weakref.WeakKeyDictionary()
)

# Strong references to detached reaper tasks, so they aren't garbage-collected
# mid-flight (asyncio only holds weak references to running tasks).
_reapers: set[asyncio.Task] = set()


def _get_slots(limit: int) -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    sem = _slots.get(loop)
    if sem is None:
        sem = asyncio.Semaphore(limit)
        _slots[loop] = sem
    return sem


async def _exited(proc, timeout: float | None) -> bool:
    """Wait until *proc* has actually been reaped, or *timeout* elapses.

    Deliberately polls ``returncode`` instead of awaiting ``proc.wait()``.
    ``Process.wait()`` resolves via the subprocess transport, which only
    finishes once the process has exited *and* every pipe has closed — so a
    grandchild that inherited stdout keeps it pending long after the child
    itself is dead and reaped. ``returncode`` is set the moment the child is
    waitpid()'d, which is exactly the condition we care about here.
    """
    deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
    delay = 0.02
    while proc.returncode is None:
        if deadline is not None and asyncio.get_running_loop().time() >= deadline:
            return False
        await asyncio.sleep(delay)
        delay = min(delay * 2, 1.0)
    return True


async def _reap(proc, file_path: str, sem: asyncio.Semaphore, grace: float) -> None:
    """Terminate *proc* and collect its exit status, then free its slot.

    Runs detached from the caller: a child blocked in uninterruptible I/O will
    not die until the mount responds, and no request or scan should wait on
    that. The slot stays held until the child is genuinely reaped, which is
    what bounds the number of wedged ffprobe processes.
    """
    try:
        for send_signal, name in ((proc.terminate, "SIGTERM"), (proc.kill, "SIGKILL")):
            if proc.returncode is not None:
                return
            try:
                send_signal()
            except ProcessLookupError:
                return  # already exited and reaped
            if await _exited(proc, grace):
                return
            logger.warning(
                "ffprobe pid %s did not exit within %ss of %s (%s)",
                proc.pid, grace, name, file_path,
            )

        # SIGKILL did not land within the grace period, so the child is almost
        # certainly stuck in uninterruptible I/O on a hung mount. Keep waiting
        # anyway: this is the only thing that will ever collect its exit status
        # once the mount recovers, and holding the slot is what caps how many
        # such processes can exist.
        logger.error(
            "ffprobe pid %s is unkillable — mount serving %s is likely hung; "
            "holding a probe slot until it recovers",
            proc.pid, file_path,
        )
        await _exited(proc, None)
        logger.info("ffprobe pid %s finally reaped", proc.pid)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("ffprobe reaper failed for %s", file_path)
    finally:
        # communicate() was cancelled, so its pipes were never drained to EOF.
        # Close the transport now that the child is gone, rather than leaving
        # the file descriptors to a later __del__.
        transport = getattr(proc, "_transport", None)
        if transport is not None and proc.returncode is not None:
            try:
                transport.close()
            except Exception:
                pass
        sem.release()


def _detach_reaper(proc, file_path: str, sem: asyncio.Semaphore, grace: float) -> None:
    task = asyncio.ensure_future(_reap(proc, file_path, sem, grace))
    _reapers.add(task)
    task.add_done_callback(_reapers.discard)


async def get_audio_tracks(
    file_path: str,
    *,
    timeout: float | None = None,
    kill_grace: float | None = None,
    max_concurrent: int | None = None,
    slot_timeout: float | None = None,
) -> list[dict] | None:
    """Return the audio tracks in *file_path*, or None if they can't be read.

    Returns None (rather than raising) for every expected failure — missing
    file, hung mount, unreadable container — because callers treat None as
    "audio undetermined" and carry on.
    """
    settings = get_settings()
    timeout = settings.FFPROBE_TIMEOUT if timeout is None else timeout
    kill_grace = settings.FFPROBE_KILL_GRACE if kill_grace is None else kill_grace
    max_concurrent = (
        settings.FFPROBE_MAX_CONCURRENT if max_concurrent is None else max_concurrent
    )
    slot_timeout = settings.FFPROBE_SLOT_TIMEOUT if slot_timeout is None else slot_timeout

    # Deliberately no exists()/stat() pre-check: that would be a blocking
    # syscall on the event loop against the very mount that may be hung. Let
    # ffprobe do the open() in a child we are able to kill; a missing file just
    # comes back as a non-zero exit.

    sem = _get_slots(max_concurrent)
    try:
        await asyncio.wait_for(sem.acquire(), slot_timeout)
    except asyncio.TimeoutError:
        logger.error(
            "ffprobe: all %d probe slots still held after %ss — media mount is "
            "likely hung; skipping %s",
            max_concurrent, slot_timeout, file_path,
        )
        return None

    # ffprobe treats a bare argument as an input URL and a leading dash as an
    # option. Every path Babel probes is a translated absolute local path, so
    # anything else (a URL from a forged webhook, "-foo") is refused outright
    # rather than handed to the binary.
    if not file_path.startswith("/") or "://" in file_path:
        logger.warning("Refusing to probe non-local path: %r", file_path[:120])
        return None

    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "a",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
    except FileNotFoundError:
        logger.error("ffprobe not found on PATH — cannot probe %s", file_path)
        sem.release()
        return None
    except asyncio.TimeoutError:
        logger.error("ffprobe timed out after %ss for %s — terminating", timeout, file_path)
        _detach_reaper(proc, file_path, sem, kill_grace)
        return None
    except BaseException:
        # Includes CancelledError (scheduler shutdown, client disconnect). The
        # child must never be left running with nobody to collect it.
        if proc is None:
            sem.release()
        else:
            _detach_reaper(proc, file_path, sem, kill_grace)
        raise
    else:
        sem.release()

    if proc.returncode != 0:
        logger.error(
            "ffprobe failed (exit %s) for %s: %s",
            proc.returncode, file_path, stderr.decode(errors="replace").strip(),
        )
        return None

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        logger.error("ffprobe returned invalid JSON for %s", file_path)
        return None

    tracks = []
    for stream in data.get("streams", []):
        tags = stream.get("tags", {})
        raw_lang = tags.get("language", "und")
        tracks.append(
            {
                "language": normalize_language(raw_lang),
                "codec": stream.get("codec_name", "unknown"),
            }
        )

    return tracks
