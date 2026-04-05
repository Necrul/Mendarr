from __future__ import annotations

import asyncio
import json
import subprocess
from typing import Any

from app.config import get_settings
from app.domain.value_objects import ProbeResult
from app.logging import get_logger

log = get_logger(__name__)


def _parse_streams(data: dict[str, Any]) -> ProbeResult:
    streams = data.get("streams") or []
    video = None
    audio_codecs: list[str] = []
    duration = None
    width = height = None
    vcodec = None
    for s in streams:
        if s.get("codec_type") == "video" and not video:
            video = s
            width = s.get("width")
            height = s.get("height")
            vcodec = s.get("codec_name")
        elif s.get("codec_type") == "audio":
            c = s.get("codec_name")
            if c:
                audio_codecs.append(c)
    fmt = data.get("format") or {}
    dur_s = fmt.get("duration")
    if dur_s is not None:
        try:
            duration = float(dur_s)
        except (TypeError, ValueError):
            duration = None
    return ProbeResult(
        ok=True,
        duration_seconds=duration,
        width=int(width) if width else None,
        height=int(height) if height else None,
        video_codec=vcodec,
        audio_codecs=audio_codecs,
        raw=data,
        error=None,
    )


def probe_sync(path: str) -> ProbeResult:
    s = get_settings()
    cmd = [s.ffprobe_path, "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        return ProbeResult(False, None, None, None, None, [], None, "ffprobe timeout")
    except FileNotFoundError:
        return ProbeResult(False, None, None, None, None, [], None, f"ffprobe not found: {s.ffprobe_path}")
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip() or f"exit {proc.returncode}"
        return ProbeResult(False, None, None, None, None, [], None, err[:500])
    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as e:
        return ProbeResult(False, None, None, None, None, [], None, f"invalid json: {e}")
    return _parse_streams(data)


async def probe_file(path: str) -> ProbeResult:
    return await asyncio.to_thread(probe_sync, path)
