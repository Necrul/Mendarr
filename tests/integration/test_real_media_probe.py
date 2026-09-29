"""Exercise the installed media tools; CI's Linux job installs ffmpeg."""

import shutil
import subprocess

import pytest

from app.domain.enums import MediaKind
from app.domain.scoring import score_finding
from app.integrations.ffprobe_adapter import probe_file


@pytest.mark.asyncio
@pytest.mark.parametrize("audio", [True, False])
async def test_generated_media_stream_detection(tmp_path, audio):
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        pytest.skip("ffmpeg and ffprobe are required for real media checks")
    media = tmp_path / "movie.mp4"
    command = ["ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i", "color=c=blue:s=128x72:r=10"]
    if audio:
        command += ["-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100"]
    command += ["-t", "2", "-c:v", "mpeg4", "-c:a", "aac", str(media)]
    subprocess.run(command, check=True, capture_output=True, timeout=30)
    probe = await probe_file(str(media))
    result = score_finding(file_path=str(media), media_kind=MediaKind.MOVIE,
                           size_bytes=media.stat().st_size, probe=probe,
                           min_tv_size_bytes=0, min_movie_size_bytes=0,
                           min_duration_tv=1, min_duration_movie=1)
    assert probe.ok
    assert probe.duration_seconds >= 1.9
    assert ("MD_NO_AUDIO_EXPECTED" in {reason.code for reason in result.reasons}) == (not audio)
    if audio:
        assert result.score == 0


@pytest.mark.asyncio
async def test_real_probe_rejects_empty_media(tmp_path):
    if not shutil.which("ffprobe"):
        pytest.skip("ffprobe required")
    media = tmp_path / "empty.mkv"
    media.touch()
    assert not (await probe_file(str(media))).ok
