from types import SimpleNamespace

import httpx
import pytest

from app.persistence.models import Finding, RemediationJob
from app.services import remediation_service as remediation


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sonarr", "radarr"])
@pytest.mark.parametrize("change", ["path", "size", "unknown_size", "unknown_path", "none"])
async def test_delete_checks_current_manager_file(monkeypatch, tmp_path, kind, change):
    media = tmp_path / "original.mkv"
    media.write_bytes(b"bad")
    current = {"id": 9, "path": "/media/original.mkv", "size": 3}
    if change == "path":
        current["path"] = "/media/replacement.mkv"
    elif change == "size":
        current["size"] = 200_000
    elif change == "unknown_size":
        current.pop("size")
    elif change == "unknown_path":
        current.pop("path")
    calls, attempts = [], []

    async def pairs(session):
        return {kind: [("/media", str(tmp_path))]}

    class Manager:
        async def get_episode_by_id(self, eid):
            return {"episodeFileId": 9}

        async def get_episode_file(self, eid):
            return current

        async def get_movie(self, mid):
            return {"movieFile": current}

        async def get_movie_file(self, mid):
            return current

        async def delete_episode_file(self, fid):
            calls.append("delete")
            return {"status": 200}

        delete_movie_file = delete_episode_file

        async def episode_search(self, ids):
            calls.append("search")
            return {"id": 10}

        movies_search = episode_search

    monkeypatch.setattr(remediation, "load_root_pairs", pairs)
    finding = Finding(file_path=str(media), file_size_bytes=3,
                      manager_entity_id="episode:1" if kind == "sonarr" else "1")
    session = SimpleNamespace(add=attempts.append)
    operation = getattr(remediation, f"_execute_{kind}_delete_search")
    if change == "none":
        await operation(session, Manager(), finding, RemediationJob(id=1))
        assert calls == ["delete", "search"]
        assert attempts[0].status == "succeeded"
    else:
        with pytest.raises(RuntimeError, match="verify scan"):
            await operation(session, Manager(), finding, RemediationJob(id=1))
        assert calls == []
        assert attempts == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sonarr", "radarr"])
async def test_search_timeout_keeps_successful_delete_attempt(monkeypatch, tmp_path, kind):
    path = tmp_path / "bad.mkv"
    attempts = []
    current = {"id": 9, "path": str(path), "size": 0}

    async def pairs(session):
        return {kind: []}

    class Manager:
        async def get_episode_by_id(self, eid):
            return {"episodeFileId": 9}

        async def get_episode_file(self, fid):
            return current

        async def get_movie(self, mid):
            return {"movieFile": current}

        async def delete_episode_file(self, fid):
            return {"status": 200}

        delete_movie_file = delete_episode_file

        async def episode_search(self, ids):
            raise httpx.ReadTimeout("search timed out")

        movies_search = episode_search

    monkeypatch.setattr(remediation, "load_root_pairs", pairs)
    finding = Finding(file_path=str(path), file_size_bytes=0,
                      manager_entity_id="episode:1" if kind == "sonarr" else "1")
    operation = getattr(remediation, f"_execute_{kind}_delete_search")
    with pytest.raises(httpx.ReadTimeout):
        await operation(SimpleNamespace(add=attempts.append), Manager(), finding, RemediationJob(id=1))
    assert len(attempts) == 1
    assert attempts[0].status == "succeeded"
    assert attempts[0].step_name.startswith("Delete")
