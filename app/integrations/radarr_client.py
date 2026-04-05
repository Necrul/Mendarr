from __future__ import annotations

from typing import Any

import httpx

from app.logging import get_logger

log = get_logger(__name__)


class RadarrClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {"X-Api-Key": self.api_key}

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self.timeout, connect=min(3.0, self.timeout))

    def _error_payload(self, response: httpx.Response) -> dict[str, Any]:
        payload: Any = None
        try:
            payload = response.json()
        except ValueError:
            payload = None

        message = ""
        if isinstance(payload, dict):
            candidates = [
                payload.get("message"),
                payload.get("errorMessage"),
                payload.get("error"),
                payload.get("description"),
                payload.get("title"),
            ]
            errors = payload.get("errors")
            if isinstance(errors, list):
                for item in errors:
                    if isinstance(item, dict):
                        candidates.extend(
                            [
                                item.get("message"),
                                item.get("errorMessage"),
                                item.get("error"),
                                item.get("description"),
                            ]
                        )
                    elif item:
                        candidates.append(str(item))
            elif isinstance(errors, dict):
                for key, value in errors.items():
                    if value:
                        candidates.append(f"{key}: {value}")
            message = next(
                (str(value).strip() for value in candidates if value is not None and str(value).strip()),
                "",
            )
        if not message:
            message = str(response.text or "").strip()
        if not message:
            reason = (getattr(response, "reason_phrase", "") or "").strip() or "Request failed"
            message = f"HTTP {response.status_code}: {reason}"
        elif not message.lower().startswith("http "):
            message = f"HTTP {response.status_code}: {message}"
        return {"error": message[:2000], "status": response.status_code}

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

    async def all_movies(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/movie", headers=self._headers())
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

    async def get_movie(self, movie_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/movie/{movie_id}", headers=self._headers())
            r.raise_for_status()
            return r.json()

    async def get_movie_file(self, movie_file_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.get(f"{self.base_url}/api/v3/moviefile/{movie_file_id}", headers=self._headers())
            r.raise_for_status()
            return r.json()

    async def delete_movie_file(self, movie_file_id: int) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.delete(f"{self.base_url}/api/v3/moviefile/{movie_file_id}", headers=self._headers())
            if r.status_code >= 400:
                return self._error_payload(r)
            if r.content:
                try:
                    return r.json()
                except ValueError:
                    return {"status": r.status_code}
            return {"status": r.status_code}

    async def post_command(self, body: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout()) as c:
            r = await c.post(f"{self.base_url}/api/v3/command", headers=self._headers(), json=body)
            if r.status_code >= 400:
                return self._error_payload(r)
            if not r.content:
                return {"status": r.status_code, "body": body}
            try:
                return r.json()
            except ValueError:
                return {"status": r.status_code, "body": body}

    async def refresh_movie(self, movie_ids: list[int]) -> dict[str, Any]:
        return await self.post_command({"name": "RefreshMovie", "movieIds": movie_ids})

    async def movies_search(self, movie_ids: list[int]) -> dict[str, Any]:
        return await self.post_command({"name": "MoviesSearch", "movieIds": movie_ids})

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
