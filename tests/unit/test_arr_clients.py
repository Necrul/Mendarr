import pytest

from app.integrations.radarr_client import RadarrClient
from app.integrations.sonarr_client import SonarrClient


class _StubResponse:
    def __init__(self, payload: dict, *, status_code: int = 200, text: str = "", reason_phrase: str = "OK"):
        self.status_code = status_code
        self.text = text
        self.reason_phrase = reason_phrase
        self._payload = payload
        self.content = b"{}"

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _StubAsyncClient:
    def __init__(self, calls: list, *args, **kwargs):
        self.calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, headers=None, json=None):
        self.calls.append(("POST", url, headers, json))
        return _StubResponse({"ok": True, "name": json["name"]})

    async def get(self, url, headers=None, params=None):
        self.calls.append(("GET", url, headers, params))
        return _StubResponse({"records": []})


@pytest.mark.asyncio
async def test_sonarr_episode_search_shapes_request(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "app.integrations.sonarr_client.httpx.AsyncClient",
        lambda *args, **kwargs: _StubAsyncClient(calls, *args, **kwargs),
    )
    client = SonarrClient("http://sonarr:8989", "secret")
    await client.episode_search([11, 12])

    method, url, headers, body = calls[0]
    assert method == "POST"
    assert url == "http://sonarr:8989/api/v3/command"
    assert headers["X-Api-Key"] == "secret"
    assert body == {"name": "EpisodeSearch", "episodeIds": [11, 12]}


@pytest.mark.asyncio
async def test_radarr_refresh_movie_shapes_request(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "app.integrations.radarr_client.httpx.AsyncClient",
        lambda *args, **kwargs: _StubAsyncClient(calls, *args, **kwargs),
    )
    client = RadarrClient("http://radarr:7878", "secret")
    await client.refresh_movie([55])

    method, url, headers, body = calls[0]
    assert method == "POST"
    assert url == "http://radarr:7878/api/v3/command"
    assert headers["X-Api-Key"] == "secret"
    assert body == {"name": "RefreshMovie", "movieIds": [55]}


@pytest.mark.asyncio
async def test_sonarr_series_search_shapes_request(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "app.integrations.sonarr_client.httpx.AsyncClient",
        lambda *args, **kwargs: _StubAsyncClient(calls, *args, **kwargs),
    )
    client = SonarrClient("http://sonarr:8989", "secret")
    await client.series_search(77)

    method, url, headers, body = calls[0]
    assert method == "POST"
    assert url == "http://sonarr:8989/api/v3/command"
    assert headers["X-Api-Key"] == "secret"
    assert body == {"name": "SeriesSearch", "seriesId": 77}


@pytest.mark.asyncio
async def test_sonarr_episodes_for_series_shapes_request(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "app.integrations.sonarr_client.httpx.AsyncClient",
        lambda *args, **kwargs: _StubAsyncClient(calls, *args, **kwargs),
    )
    client = SonarrClient("http://sonarr:8989", "secret")
    await client.episodes_for_series(55)

    method, url, headers, params = calls[0]
    assert method == "GET"
    assert url == "http://sonarr:8989/api/v3/episode"
    assert headers["X-Api-Key"] == "secret"
    assert params == {"seriesId": 55}


class _ErrorAsyncClient(_StubAsyncClient):
    async def post(self, url, headers=None, json=None):
        self.calls.append(("POST", url, headers, json))
        return _StubResponse({}, status_code=409, reason_phrase="Conflict")


@pytest.mark.asyncio
async def test_radarr_post_command_returns_status_when_error_body_is_empty(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "app.integrations.radarr_client.httpx.AsyncClient",
        lambda *args, **kwargs: _ErrorAsyncClient(calls, *args, **kwargs),
    )
    client = RadarrClient("http://radarr:7878", "secret")

    payload = await client.movies_search([55])

    assert payload["status"] == 409
    assert payload["error"] == "HTTP 409: Conflict"
