import asyncio
import json
import logging
from pathlib import Path

from src.config import normalize_language

logger = logging.getLogger(__name__)


async def get_audio_tracks(file_path: str) -> list[dict] | None:
    if not Path(file_path).exists():
        logger.warning("File not found: %s", file_path)
        return None

    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-select_streams", "a",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    except asyncio.TimeoutError:
        logger.error("ffprobe timed out for %s", file_path)
        try:
            proc.kill()
            await proc.wait()
        except ProcessLookupError:
            pass
        return None
    except FileNotFoundError:
        logger.error("ffprobe not found on PATH")
        return None

    if proc.returncode != 0:
        logger.error("ffprobe failed for %s: %s", file_path, stderr.decode().strip())
        return None

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        logger.error("ffprobe returned invalid JSON for %s", file_path)
        return None

    streams = data.get("streams", [])
    tracks = []
    for stream in streams:
        tags = stream.get("tags", {})
        raw_lang = tags.get("language", "und")
        tracks.append(
            {
                "language": normalize_language(raw_lang),
                "codec": stream.get("codec_name", "unknown"),
            }
        )

    return tracks
