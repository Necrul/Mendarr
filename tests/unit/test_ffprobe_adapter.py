import subprocess

import pytest

from app.integrations.ffprobe_adapter import ProbeUnavailableError, probe_sync


@pytest.mark.parametrize("error", [FileNotFoundError, PermissionError])
def test_unavailable_probe_is_an_operational_failure(monkeypatch, error):
    def unavailable(*args, **kwargs):
        raise error("cannot execute")

    monkeypatch.setattr(subprocess, "run", unavailable)
    with pytest.raises(ProbeUnavailableError, match="Cannot run ffprobe"):
        probe_sync("movie.mkv")


def test_bad_container_still_returns_media_failure(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs:
                        subprocess.CompletedProcess(args[0], 1, stdout="", stderr="Invalid container"))
    result = probe_sync("movie.mkv")
    assert not result.ok
    assert result.error == "Invalid container"
