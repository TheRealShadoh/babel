"""The pre-fix ffprobe implementation, kept only so the repro harness can
demonstrate the original failure mode side by side with the fix.

Not imported by the application.
"""
import asyncio
import json
import logging
from pathlib import Path

from src.config import normalize_language

logger = logging.getLogger(__name__)


async def get_audio_tracks(file_path, **_ignored):
    if not Path(file_path).exists():            # blocking stat on the event loop
        logger.warning("File not found: %s", file_path)
        return None
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "a", file_path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    except asyncio.TimeoutError:                # does NOT catch CancelledError
        logger.error("ffprobe timed out for %s", file_path)
        try:
            proc.kill()
            await proc.wait()                   # unbounded
        except ProcessLookupError:
            pass
        return None
    except FileNotFoundError:
        logger.error("ffprobe not found on PATH")
        return None
    if proc.returncode != 0:
        return None
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return [{"language": normalize_language(s.get("tags", {}).get("language", "und")),
             "codec": s.get("codec_name", "unknown")} for s in data.get("streams", [])]
