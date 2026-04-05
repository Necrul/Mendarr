from pathlib import Path

import pytest

from app.config import get_settings
from app.crypto import encrypt_secret
from app.persistence.models import IntegrationConfig, LibraryRoot
from app.services.root_discovery_service import discover_root_candidates, resolve_local_scan_path


class _StubSonarrClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

    async def root_folders(self):
        return [{"path": self.base_url}]


@pytest.mark.asyncio
async def test_root_discovery_marks_existing_local_path(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("app.services.root_discovery_service.SonarrClient", _StubSonarrClient)
    row = IntegrationConfig(
        kind="sonarr",
        name="Sonarr",
        base_url=str(tmp_path),
        api_key=encrypt_secret("secret"),
        enabled=True,
    )

    candidates = await discover_root_candidates(row, [])

    assert len(candidates) == 1
    assert candidates[0].manager_root_path == str(tmp_path)
    assert candidates[0].exists_locally is True
    assert candidates[0].already_mapped is False


@pytest.mark.asyncio
async def test_root_discovery_marks_existing_mapping(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("app.services.root_discovery_service.SonarrClient", _StubSonarrClient)
    row = IntegrationConfig(
        kind="sonarr",
        name="Sonarr",
        base_url=str(tmp_path),
        api_key=encrypt_secret("secret"),
        enabled=True,
    )
    existing = [
        LibraryRoot(
            manager_kind="sonarr",
            manager_root_path=str(tmp_path),
            local_root_path=str(tmp_path),
            enabled=True,
        )
    ]

    candidates = await discover_root_candidates(row, existing)

    assert candidates[0].already_mapped is True


def test_resolve_local_scan_path_maps_windows_drive_to_libraries_mount(tmp_path: Path):
    libraries_root = tmp_path / "libraries"
    mounted_path = libraries_root / "l" / "TV"
    mounted_path.mkdir(parents=True)

    resolved, exists = resolve_local_scan_path(r"L:\TV", libraries_root=str(libraries_root))

    assert exists is True
    assert resolved == str(mounted_path.resolve())


def test_resolve_local_scan_path_uses_configured_mapping(monkeypatch, tmp_path: Path):
    mounted_root = tmp_path / "raynas"
    mounted_tv = mounted_root / "TV Shows"
    mounted_tv.mkdir(parents=True)
    monkeypatch.setenv("MENDARR_PATH_MAPPINGS", f"/sonarr=>{mounted_root.as_posix()}")
    get_settings.cache_clear()

    resolved, exists = resolve_local_scan_path("/sonarr/TV Shows")

    assert exists is True
    assert resolved == str(mounted_tv.resolve())
    get_settings.cache_clear()


def test_resolve_local_scan_path_keeps_original_path_when_no_match(monkeypatch):
    monkeypatch.delenv("MENDARR_PATH_MAPPINGS", raising=False)
    get_settings.cache_clear()

    resolved, exists = resolve_local_scan_path("/manager/TV Shows")

    assert exists is False
    assert resolved == "/manager/TV Shows"
    get_settings.cache_clear()
