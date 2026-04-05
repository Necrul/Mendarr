from __future__ import annotations

from typing import Any

import httpx

from app.logging import get_logger

log = get_logger(__name__)


class SonarrClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {"X-Api-Key": self.api_key}

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self.timeout, connect=min(3.0, self.timeout))

    async def test(self) -> tuple[bool, str]:
        try:
            async with httpx.AsyncClient(timeout=self._timeout()) as c:
                r = await c.get(f"{self.base_url}/api/v3/system/status", headers=self._headers())
                if r.status_code == 200:
                    return True, "ok"
                return False, f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            return False, str(e)

    async def root_folders(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/rootfolder", headers=self._headers())
            r.raise_for_status()
            return r.json()

    async def all_series(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/series", headers=self._headers())
            r.raise_for_status()
            return r.json()

    async def get_series(self, series_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/series/{series_id}", headers=self._headers())
            r.raise_for_status()
            return r.json()

    async def parse_path(self, title: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(
                f"{self.base_url}/api/v3/parse",
                headers=self._headers(),
                params={"title": title},
            )
            r.raise_for_status()
            return r.json()

    async def get_episode_by_id(self, episode_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/episode/{episode_id}", headers=self._headers())
            r.raise_for_status()
            return r.json()

    async def get_episode_file(self, episode_file_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/episodefile/{episode_file_id}", headers=self._headers())
            r.raise_for_status()
            return r.json()

    async def delete_episode_file(self, episode_file_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.delete(f"{self.base_url}/api/v3/episodefile/{episode_file_id}", headers=self._headers())
            if r.status_code >= 400:
                return {"error": r.text, "status": r.status_code}
            if r.content:
                try:
                    return r.json()
                except ValueError:
                    return {"status": r.status_code}
            return {"status": r.status_code}

    async def get_cutoff_unmet_episode(self, episode_id: int) -> dict[str, Any] | None:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/wanted/cutoff/{episode_id}", headers=self._headers())
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()

    async def episodes_for_series(self, series_id: int) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(
                f"{self.base_url}/api/v3/episode",
                headers=self._headers(),
                params={"seriesId": series_id},
            )
            r.raise_for_status()
            return r.json()

    async def post_command(self, body: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.post(f"{self.base_url}/api/v3/command", headers=self._headers(), json=body)
            if r.status_code >= 400:
                return {"error": r.text, "status": r.status_code}
            return r.json()

    async def refresh_series(self, series_id: int) -> dict[str, Any]:
        return await self.post_command({"name": "RefreshSeries", "seriesId": series_id})

    async def rescan_series(self, series_id: int) -> dict[str, Any]:
        return await self.post_command({"name": "RescanSeries", "seriesId": series_id})

    async def episode_search(self, episode_ids: list[int]) -> dict[str, Any]:
        return await self.post_command({"name": "EpisodeSearch", "episodeIds": episode_ids})

    async def series_search(self, series_id: int) -> dict[str, Any]:
        return await self.post_command({"name": "SeriesSearch", "seriesId": series_id})

    async def queue(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/queue", headers=self._headers())
            r.raise_for_status()
            data = r.json()
            return data.get("records") or data if isinstance(data, list) else []

    async def history(self, page_size: int = 20) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(
                f"{self.base_url}/api/v3/history",
                headers=self._headers(),
                params={"pageSize": page_size},
            )
            r.raise_for_status()
            data = r.json()
            return data.get("records") or []
