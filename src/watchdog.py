"""Event-loop stall watchdog.

Babel's event loop must keep turning for two separate reasons:

* it serves HTTP, including ``/api/health``; and
* under uvloop (which ``uvicorn[standard]`` selects by default) it is what
  reaps exited child processes — libuv handles SIGCHLD from a loop callback,
  so a stalled loop leaks zombies for as long as the stall lasts.

Docker's HEALTHCHECK correctly notices a stalled loop — a wedged process
cannot answer ``curl`` — but ``restart: unless-stopped`` only reacts to the
container *exiting*, never to it being *unhealthy*. So a stall is reported and
then ignored indefinitely. The watchdog closes that gap from inside: a plain
daemon thread (not a task — a task cannot run while the loop is stalled)
compares a loop-updated heartbeat against wall clock and hard-exits the
process once the lag is unambiguously pathological, letting the container's
restart policy recover it.
"""

import asyncio
import logging
import os
import sys
import threading
import time

logger = logging.getLogger(__name__)

# Written by the loop heartbeat, read by the watchdog thread. A bare float
# assignment/read needs no lock under CPython.
_last_beat: float = time.monotonic()
_started_at: float = time.monotonic()


def loop_lag_seconds() -> float:
    """How long since the event loop last made progress."""
    return max(0.0, time.monotonic() - _last_beat)


class LoopWatchdog:
    def __init__(self, interval: float, unhealthy_lag: float, abort_lag: float):
        self.interval = max(0.1, interval)
        self.unhealthy_lag = unhealthy_lag
        self.abort_lag = abort_lag
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._task: asyncio.Task | None = None
        self._warned = False

    async def start(self) -> None:
        global _last_beat, _started_at
        _last_beat = _started_at = time.monotonic()
        self._task = asyncio.get_running_loop().create_task(self._heartbeat())
        self._thread = threading.Thread(
            target=self._watch, name="loop-watchdog", daemon=True
        )
        self._thread.start()
        logger.info(
            "Loop watchdog started (unhealthy above %ss lag, abort above %ss)",
            self.unhealthy_lag,
            self.abort_lag if self.abort_lag > 0 else "disabled",
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _heartbeat(self) -> None:
        global _last_beat
        while True:
            _last_beat = time.monotonic()
            await asyncio.sleep(self.interval)

    def _watch(self) -> None:
        while not self._stop.wait(self.interval):
            lag = loop_lag_seconds()

            if lag > self.unhealthy_lag:
                if not self._warned:
                    logger.error(
                        "Event loop stalled for %.1fs — HTTP is not being served and "
                        "child processes are not being reaped. Most likely a blocking "
                        "filesystem call against a hung mount.", lag,
                    )
                    self._warned = True
            elif self._warned:
                logger.warning("Event loop recovered after a stall (lag now %.1fs)", lag)
                self._warned = False

            if self.abort_lag > 0 and lag > self.abort_lag:
                # The loop is wedged; nothing running on it can help, and signal
                # handlers won't run either because the main thread is stuck in a
                # syscall. Exit hard from this thread and let the container's
                # restart policy bring the process back.
                msg = (
                    f"FATAL: event loop stalled {lag:.0f}s (limit {self.abort_lag:.0f}s) "
                    f"— aborting so the container restart policy can recover.\n"
                )
                try:
                    logger.critical(msg.strip())
                    sys.stderr.write(msg)
                    sys.stderr.flush()
                except Exception:
                    pass
                os._exit(70)  # EX_SOFTWARE


_watchdog: LoopWatchdog | None = None


async def start_watchdog(interval: float, unhealthy_lag: float, abort_lag: float) -> None:
    global _watchdog
    if _watchdog is not None:
        return
    _watchdog = LoopWatchdog(interval, unhealthy_lag, abort_lag)
    await _watchdog.start()


async def stop_watchdog() -> None:
    global _watchdog
    if _watchdog is None:
        return
    await _watchdog.stop()
    _watchdog = None


def is_loop_healthy(unhealthy_lag: float | None = None) -> bool:
    limit = (
        unhealthy_lag
        if unhealthy_lag is not None
        else (_watchdog.unhealthy_lag if _watchdog else 15.0)
    )
    return loop_lag_seconds() <= limit
