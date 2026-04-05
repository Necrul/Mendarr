import pytest

from app.domain.enums import ManagerKind
from app.services.match_service import match_tv_path


class _StubSonarrClient:
    async def all_series(self):
        return [{"id": 77, "title": "Broken Show", "alternateTitles": []}]

    async def parse_path(self, title: str):
        return {"episodes": []}

    async def episodes_for_series(self, series_id: int):
        return [{"id": 501, "seasonNumber": 1, "episodeNumber": 1}]


class _TaggedPathSonarrClient:
    async def all_series(self):
        return [{"id": 77, "title": "Cops", "alternateTitles": []}]

    async def parse_path(self, title: str):
        return {"episodes": []}

    async def episodes_for_series(self, series_id: int):
        return [{"id": 509, "seasonNumber": 2, "episodeNumber": 9}]


@pytest.mark.asyncio
async def test_match_tv_path_falls_back_to_episode_lookup_for_series():
    outcome = await match_tv_path(
        None,
        "/mnt/RAYNAS/TV Shows/Broken Show/Season 01/Broken.Show.S01E01.mkv",
        "http://sonarr:8989",
        "secret",
        pairs=[],
        client=_StubSonarrClient(),
        series_list=[{"id": 77, "title": "Broken Show", "alternateTitles": []}],
    )

    assert outcome.manager_kind == ManagerKind.SONARR
    assert outcome.manager_entity_id == "episode:501"


@pytest.mark.asyncio
async def test_match_tv_path_matches_short_title_when_folder_has_tvdb_tag():
    outcome = await match_tv_path(
        None,
        "/mnt/RAYNAS/Z-Plex-Media/TV Shows/Cops {tvdb-74709}/Season 02/Cops - S02E09 - Portland, OR 9.mp4",
        "http://sonarr:8989",
        "secret",
        pairs=[],
        client=_TaggedPathSonarrClient(),
        series_list=[{"id": 77, "title": "Cops", "alternateTitles": []}],
    )

    assert outcome.manager_kind == ManagerKind.SONARR
    assert outcome.manager_entity_id == "episode:509"
