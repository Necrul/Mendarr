from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

import httpx

from app.config import get_settings
from app.version import get_app_version, get_version_label


_VERSION_PARTS = re.compile(r"\d+")


def _normalize_repo(value: str) -> str:
    repo = value.strip().strip("/")
    if repo.endswith(".git"):
        repo = repo[:-4]
    if repo.startswith("https://github.com/"):
        repo = repo.removeprefix("https://github.com/")
    return repo


def _repo_url(repo: str) -> str:
    if not repo or "/" not in repo:
        return ""
    return f"https://github.com/{repo}"


def _release_url(repo: str, tag: str | None) -> str:
    if not repo or not tag:
        return _repo_url(repo)
    return f"https://github.com/{repo}/releases/tag/{tag}"


def _version_key(value: str | None) -> tuple[int, ...]:
    if not value:
        return ()
    parts = [int(match.group(0)) for match in _VERSION_PARTS.finditer(str(value))]
    return tuple(parts)


def _update_cache_path() -> Path:
    return get_settings().data_dir / "update-check.json"


def _read_cached_status() -> dict[str, object] | None:
    path = _update_cache_path()
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_cached_status(payload: dict[str, object]) -> None:
    path = _update_cache_path()
    try:
        path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    except OSError:
        pass


async def _fetch_latest_release(repo: str) -> dict[str, str | None]:
    settings = get_settings()
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": f"Mendarr/{get_app_version()}",
    }
    timeout = httpx.Timeout(5.0, connect=3.0)
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        release = await client.get(f"https://api.github.com/repos/{repo}/releases/latest")
        if release.status_code == 200:
            data = release.json()
            tag = str(data.get("tag_name") or "").strip() or None
            return {
                "latest_version": tag,
                "release_url": str(data.get("html_url") or _release_url(repo, tag)),
                "error": None,
            }
        if release.status_code not in {403, 404}:
            release.raise_for_status()

        tags = await client.get(f"https://api.github.com/repos/{repo}/tags")
        if tags.status_code == 200:
            data = tags.json()
            if isinstance(data, list) and data:
                tag = str((data[0] or {}).get("name") or "").strip() or None
                return {
                    "latest_version": tag,
                    "release_url": _release_url(repo, tag),
                    "error": None,
                }
        if release.status_code == 404:
            return {
                "latest_version": None,
                "release_url": _repo_url(repo),
                "error": "Release feed not found yet",
            }
        if release.status_code == 403 or tags.status_code == 403:
            return {
                "latest_version": None,
                "release_url": _repo_url(repo),
                "error": "Update check rate-limited by GitHub",
            }
        return {
            "latest_version": None,
            "release_url": _repo_url(repo),
            "error": "Unable to read release feed",
        }


async def get_update_status() -> dict[str, object]:
    settings = get_settings()
    current_version = get_version_label()
    repo = _normalize_repo(settings.public_repo)
    base = {
        "current_version": current_version,
        "repo": repo,
        "repo_url": _repo_url(repo),
        "enabled": bool(settings.update_check_enabled and repo),
        "latest_version": None,
        "update_available": False,
        "release_url": "",
        "checked_at": None,
        "status": "disabled",
        "message": "Update check disabled",
    }
    if not base["enabled"]:
        return base

    cached = _read_cached_status()
    now = dt.datetime.now(dt.UTC)
    if cached:
        checked_at = cached.get("checked_at")
        try:
            checked = dt.datetime.fromisoformat(str(checked_at))
            if checked.tzinfo is None:
                checked = checked.replace(tzinfo=dt.UTC)
            age = now - checked.astimezone(dt.UTC)
            if age.total_seconds() < max(1, settings.update_check_interval_hours) * 3600:
                cached["current_version"] = current_version
                cached["repo"] = repo
                cached["repo_url"] = _repo_url(repo)
                cached["enabled"] = True
                return cached
        except Exception:
            pass

    try:
        fetched = await _fetch_latest_release(repo)
    except Exception as exc:
        if cached:
            cached["message"] = f"Using cached update data after check failure: {exc}"
            cached["status"] = "stale"
            cached["current_version"] = current_version
            cached["repo"] = repo
            cached["repo_url"] = _repo_url(repo)
            cached["enabled"] = True
            return cached
        base["status"] = "error"
        base["message"] = f"Update check failed: {exc}"
        return base

    latest_version = fetched.get("latest_version")
    latest_label = f"v{str(latest_version).lstrip('vV')}" if latest_version else None
    update_available = bool(_version_key(latest_version) > _version_key(current_version))
    if latest_label and not update_available:
        status = "current"
        message = f"Running the latest known release ({current_version})"
    elif latest_label:
        status = "update_available"
        message = f"Update available: {latest_label}"
    elif fetched.get("error"):
        status = "unavailable"
        message = str(fetched["error"])
    else:
        status = "unknown"
        message = "Release information is not available yet"

    payload = {
        "current_version": current_version,
        "repo": repo,
        "repo_url": _repo_url(repo),
        "enabled": True,
        "latest_version": latest_label,
        "update_available": update_available,
        "release_url": fetched.get("release_url") or _repo_url(repo),
        "checked_at": now.isoformat(),
        "status": status,
        "message": message,
    }
    _write_cached_status(payload)
    return payload
