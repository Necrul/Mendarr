import pytest

from app.domain.enums import ManagerKind
from app.services.match_service import match_movie_path, match_tv_path


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


class _RelativePathSonarrClient:
    def __init__(self):
        self.seen_titles = []

    async def all_series(self):
        return [{"id": 88, "title": "Cadillacs and Dinosaurs", "alternateTitles": []}]

    async def parse_path(self, title: str):
        self.seen_titles.append(title)
        if title == "/tv/Cadillacs and Dinosaurs/Season 01/Cadillacs and Dinosaurs - S01E13 - Wildfire.mkv":
            return {"episodes": [{"id": 1313, "seasonNumber": 1, "episodeNumber": 13}]}
        return {"episodes": []}

    async def episodes_for_series(self, series_id: int):
        raise AssertionError("episodes_for_series fallback should not run when parse_path succeeds")


class _RelativePathRadarrClient:
    def __init__(self):
        self.seen_titles = []

    async def all_movies(self):
        return []

    async def parse_path(self, title: str):
        self.seen_titles.append(title)
        if title == "/movies/The Matrix (1999)/The Matrix (1999).mkv":
            return {"movie": {"id": 1999, "title": "The Matrix", "year": 1999}}
        return {"movie": {}}


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


@pytest.mark.asyncio
async def test_match_tv_path_uses_manager_relative_path_for_parse_lookup():
    client = _RelativePathSonarrClient()

    outcome = await match_tv_path(
        None,
        "/mnt/RAYNAS/Z-Plex-Media/Cartoons/Cadillacs and Dinosaurs/Season 01/Cadillacs and Dinosaurs - S01E13 - Wildfire.mkv",
        "http://sonarr:8989",
        "secret",
        pairs=[("/tv", "/mnt/RAYNAS/Z-Plex-Media/Cartoons")],
        client=client,
        series_list=[{"id": 88, "title": "Cadillacs and Dinosaurs", "alternateTitles": []}],
    )

    assert outcome.manager_kind == ManagerKind.SONARR
    assert outcome.manager_entity_id == "episode:1313"
    assert client.seen_titles[0] == "/tv/Cadillacs and Dinosaurs/Season 01/Cadillacs and Dinosaurs - S01E13 - Wildfire.mkv"


@pytest.mark.asyncio
async def test_match_movie_path_uses_manager_relative_path_for_parse_lookup():
    client = _RelativePathRadarrClient()

    outcome = await match_movie_path(
        None,
        "/mnt/RAYNAS/Movies/The Matrix (1999)/The Matrix (1999).mkv",
        "http://radarr:7878",
        "secret",
        pairs=[("/movies", "/mnt/RAYNAS/Movies")],
        client=client,
        movies=[],
    )

    assert outcome.manager_kind == ManagerKind.RADARR
    assert outcome.manager_entity_id == "1999"
    assert client.seen_titles[0] == "/movies/The Matrix (1999)/The Matrix (1999).mkv"
