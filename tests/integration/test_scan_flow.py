import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import delete, select

from app.domain.value_objects import ProbeResult
from app.domain.enums import ManagerKind
from app.persistence.db import SessionLocal, init_db
from app.persistence.models import Finding, FindingReason, LibraryRoot, RemediationAttempt, RemediationJob, ScanRun
from app.services.scan_service import (
    recover_abandoned_scans,
    request_scan_stop,
    run_scan,
    run_verify_scan,
    start_scan,
    start_verify_scan,
)


@pytest.mark.asyncio
async def test_scan_creates_and_updates_finding(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "movies"
    media_root.mkdir(parents=True)
    movie_dir = media_root / "Broken Movie 2024"
    movie_dir.mkdir()
    movie_file = movie_dir / "Broken.Movie.2024.mkv"
    movie_file.write_bytes(b"")

    probe_fail = ProbeResult(False, None, None, None, None, [], None, "ffprobe failed")
    probe_short = ProbeResult(
        True,
        30.0,
        1920,
        1080,
        "h264",
        ["aac"],
        {
            "streams": [
                {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080},
                {"codec_type": "audio", "codec_name": "aac"},
            ],
            "format": {"duration": "30.0"},
        },
        None,
    )
    probes = [probe_fail, probe_short]

    async def fake_probe_file(path: str):
        return probes.pop(0)

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            session.add(
                LibraryRoot(
                    manager_kind="radarr",
                    manager_root_path="/movies",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            await run_scan(session, actor="test")

    movie_file.write_bytes(b"x" * 200_000)

    async with SessionLocal() as session:
        async with session.begin():
            await run_scan(session, actor="test")

    async with SessionLocal() as session:
        finding = (
            await session.execute(select(Finding).where(Finding.file_name == "Broken.Movie.2024.mkv"))
        ).scalar_one()
        assert finding.file_size_bytes == 200_000
        assert finding.duration_seconds == 30.0
        assert finding.suspicion_score > 0


@pytest.mark.asyncio
async def test_scan_does_not_call_manager_matching_for_healthy_file(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "tv"
    media_root.mkdir(parents=True)
    episode_dir = media_root / "Healthy Show" / "Season 01"
    episode_dir.mkdir(parents=True)
    episode_file = episode_dir / "Healthy.Show.S01E01.mkv"
    episode_file.write_bytes(b"x" * 2_000_000)

    async def fake_probe_file(path: str):
        return ProbeResult(
            True,
            1800.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1800.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")

    def fake_reveal_api_key(row):
        return "secret"

    called = {"tv": 0}

    async def fake_match_tv_path(*args, **kwargs):
        called["tv"] += 1
        raise AssertionError("Manager matching should not run for healthy files")

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.match_tv_path", fake_match_tv_path)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="sonarr",
                    manager_root_path="/tv",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            scan = await run_scan(session, actor="test")

    assert scan.files_seen == 1
    assert scan.suspicious_found == 0
    assert called["tv"] == 0


@pytest.mark.asyncio
async def test_scan_collects_paths_via_worker_thread(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "movies"
    media_root.mkdir(parents=True)
    movie_dir = media_root / "Broken Movie 2024"
    movie_dir.mkdir()
    movie_file = movie_dir / "Broken.Movie.2024.mkv"
    movie_file.write_bytes(b"")

    async def fake_probe_file(path: str):
        return ProbeResult(False, None, None, None, None, [], None, "ffprobe failed")

    calls = {"to_thread": 0}

    async def fake_to_thread(func, *args, **kwargs):
        calls["to_thread"] += 1
        return func(*args, **kwargs)

    monkeypatch.setattr(
        "app.services.scan_service.get_settings",
        lambda: SimpleNamespace(scan_precount_enabled=True),
    )
    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.asyncio.to_thread", fake_to_thread)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="radarr",
                    manager_root_path="/movies",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            scan = await run_scan(session, actor="test")

    assert calls["to_thread"] == 1
    assert scan.files_seen == 1
    assert movie_file.exists()


@pytest.mark.asyncio
async def test_scan_skips_precount_when_disabled(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "movies"
    media_root.mkdir(parents=True)
    movie_dir = media_root / "Broken Movie 2024"
    movie_dir.mkdir()
    movie_file = movie_dir / "Broken.Movie.2024.mkv"
    movie_file.write_bytes(b"")

    async def fake_probe_file(path: str):
        return ProbeResult(False, None, None, None, None, [], None, "ffprobe failed")

    monkeypatch.setattr(
        "app.services.scan_service.get_settings",
        lambda: SimpleNamespace(scan_precount_enabled=False),
    )
    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr(
        "app.services.scan_service._count_scan_paths",
        lambda root_specs: (_ for _ in ()).throw(AssertionError("pre-count should stay disabled")),
    )

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="radarr",
                    manager_root_path="/movies",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            scan = await run_scan(session, actor="test")

    assert scan.files_seen == 1
    assert scan.suspicious_found == 1
    assert movie_file.exists()


@pytest.mark.asyncio
async def test_scan_processes_files_as_they_are_discovered(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "movies"
    media_root.mkdir(parents=True)
    first_dir = media_root / "Broken Movie 2024"
    first_dir.mkdir()
    first_file = first_dir / "Broken.Movie.2024.mkv"
    first_file.write_bytes(b"")
    second_dir = media_root / "Broken Movie 2025"
    second_dir.mkdir()
    second_file = second_dir / "Broken.Movie.2025.mkv"
    second_file.write_bytes(b"")

    observed = {"probes": 0}

    async def fake_probe_file(path: str):
        observed["probes"] += 1
        return ProbeResult(False, None, None, None, None, [], None, "ffprobe failed")

    def fake_iter_video_files(root: Path):
        if root == media_root:
            yield first_file
            assert observed["probes"] == 1
            yield second_file
            return
        yield from ()

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service._count_scan_paths", lambda root_specs: 2)
    monkeypatch.setattr("app.services.scan_service.iter_video_files", fake_iter_video_files)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="radarr",
                    manager_root_path="/movies",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            scan = await run_scan(session, actor="test")

    assert scan.files_seen == 2
    assert observed["probes"] == 2


@pytest.mark.asyncio
async def test_scan_follows_linked_directories(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "movies"
    media_root.mkdir(parents=True)
    linked_target = tmp_path / "actual-library" / "Broken Movie 2024"
    linked_target.mkdir(parents=True)
    movie_file = linked_target / "Broken.Movie.2024.mkv"
    movie_file.write_bytes(b"")

    linked_dir = media_root / "Broken Movie 2024"
    try:
        linked_dir.symlink_to(linked_target, target_is_directory=True)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"directory symlinks unavailable in this environment: {exc}")

    async def fake_probe_file(path: str):
        return ProbeResult(False, None, None, None, None, [], None, "ffprobe failed")

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="radarr",
                    manager_root_path="/movies",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            scan = await run_scan(session, actor="test")

    assert scan.files_seen == 1


@pytest.mark.asyncio
async def test_scan_skips_disappeared_file_without_warning(monkeypatch, tmp_path: Path, caplog):
    media_root = tmp_path / "movies"
    media_root.mkdir(parents=True)
    movie_dir = media_root / "Broken Movie 2024"
    movie_dir.mkdir()
    movie_file = movie_dir / "Broken.Movie.2024.mkv"
    movie_file.write_bytes(b"")

    async def fake_probe_file(path: str):
        raise AssertionError("probe_file should not run when the file disappears before stat")

    original_stat = Path.stat

    def fake_stat(self, *args, **kwargs):
        if str(self) == str(movie_file):
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr(Path, "stat", fake_stat)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="radarr",
                    manager_root_path="/movies",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    caplog.set_level("WARNING", logger="app.services.scan_service")

    async with SessionLocal() as session:
        async with session.begin():
            scan = await run_scan(session, actor="test")

    assert scan.files_seen == 0
    assert scan.suspicious_found == 0
    assert not [
        record
        for record in caplog.records
        if record.name == "app.services.scan_service" and "stat failed" in record.getMessage()
    ]


@pytest.mark.asyncio
async def test_scan_matches_custom_tv_library_when_sonarr_is_configured(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "tv-custom"
    season_dir = media_root / "Broken Show" / "Season 01"
    season_dir.mkdir(parents=True)
    episode_file = season_dir / "Broken.Show.S01E01.mkv"
    episode_file.write_bytes(b"x" * 20_000)

    async def fake_probe_file(path: str):
        return ProbeResult(
            True,
            45.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "45.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def all_series(self):
            return []

    async def fake_match_tv_path(*args, **kwargs):
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="321",
            title="Broken Show",
            season_number=1,
            episode_number=1,
            year=None,
            match_confidence="high",
        )

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.scan_service.match_tv_path", fake_match_tv_path)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="tv",
                    manager_root_path="",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            await run_scan(session, actor="test")

    async with SessionLocal() as session:
        finding = (
            await session.execute(select(Finding).where(Finding.file_path == str(episode_file)))
        ).scalar_one()
        assert finding.manager_kind == "sonarr"
        assert finding.manager_entity_id == "321"


@pytest.mark.asyncio
async def test_verify_scan_checks_only_selected_finding_and_resolves_renamed_replacement(monkeypatch, tmp_path: Path):
    old_file = tmp_path / "tv" / "Broken Show" / "Season 01" / "Broken.Show.S01E01.mkv"
    old_file.parent.mkdir(parents=True)
    replacement_file = old_file.parent / "Broken Show - S01E01 - Fixed Release.mkv"
    replacement_file.write_bytes(b"x" * 2_000_000)
    unrelated_file = tmp_path / "tv" / "Other Show" / "Season 01" / "Other.Show.S01E01.mkv"
    unrelated_file.parent.mkdir(parents=True)
    unrelated_file.write_bytes(b"x" * 10)

    async def fake_probe_file(path: str):
        assert path == str(replacement_file)
        return ProbeResult(
            True,
            1800.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1800.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "episodeFileId": 99}

        async def get_episode_file(self, episode_file_id: int):
            return {"id": episode_file_id, "path": str(replacement_file)}

        async def all_series(self):
            return []

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.SonarrClient", _StubSonarrClient)

    async def fake_match_tv_path(*args, **kwargs):
        assert args[1] == str(replacement_file)
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="episode:55",
            title="Broken Show",
            season_number=1,
            episode_number=1,
            year=None,
            match_confidence="high",
        )

    monkeypatch.setattr("app.services.scan_service.match_tv_path", fake_match_tv_path)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path=str(old_file),
                file_name=old_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:55",
                title="Broken Show",
                season_number=1,
                episode_number=1,
                file_size_bytes=20_000,
                duration_seconds=10.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id)
        findings = (await session.execute(select(Finding).order_by(Finding.id.asc()))).scalars().all()
        assert run.files_seen == 1


@pytest.mark.asyncio
async def test_verify_scan_resolves_when_same_path_has_been_repaired_in_place(monkeypatch, tmp_path: Path):
    target_file = tmp_path / "tv" / "Wolfblood" / "Season 04" / "Wolfblood - S04E01 - Captivity.mkv"
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"x" * 2_000_000)

    async def fake_probe_file(path: str):
        assert path == str(target_file)
        return ProbeResult(
            True,
            1700.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1700.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "episodeFileId": 321}

        async def get_episode_file(self, episode_file_id: int):
            return {"id": episode_file_id, "path": str(target_file)}

        async def all_series(self):
            return []

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.SonarrClient", _StubSonarrClient)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path=str(target_file),
                file_name=target_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:135475",
                title="Wolfblood",
                season_number=4,
                episode_number=1,
                file_size_bytes=20_000,
                duration_seconds=None,
                resolution=None,
                codec_video=None,
                codec_audio=None,
                suspicion_score=70,
                confidence="high",
                proposed_action="rescan_only",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id)
        assert run.files_seen == 1
        assert finding is not None
        assert finding.status == "resolved"
        assert run.suspicious_found == 0


@pytest.mark.asyncio
async def test_verify_scan_relinks_stale_sonarr_finding_before_resolving_target(monkeypatch, tmp_path: Path):
    old_file = tmp_path / "tv" / "Mister Rogers Neighborhood" / "Season 04" / "Bad.Release.S04E36.mkv"
    old_file.parent.mkdir(parents=True)
    replacement_file = old_file.parent / "Mister Rogers' Neighborhood - S04E36 - Fixed.mkv"
    replacement_file.write_bytes(b"x" * 2_000_000)

    async def fake_probe_file(path: str):
        assert path == str(replacement_file)
        return ProbeResult(
            True,
            1700.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1700.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            if episode_id == 842:
                return {"id": 842, "episodeFileId": 1}
            if episode_id == 9036:
                return {"id": 9036, "episodeFileId": 2}
            raise AssertionError(f"unexpected episode id {episode_id}")

        async def get_episode_file(self, episode_file_id: int):
            if episode_file_id == 1:
                return {"id": 1, "path": "/tv/ER/Season 01/ER.S01E01.mkv"}
            if episode_file_id == 2:
                return {"id": 2, "path": str(replacement_file)}
            raise AssertionError(f"unexpected episode file id {episode_file_id}")

        async def all_series(self):
            return []

    async def fake_relink_finding(session, finding):
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="episode:9036",
            title="Mister Rogers' Neighborhood",
            season_number=4,
            episode_number=36,
            year=None,
            match_confidence="high",
        )

    async def fake_match_tv_path(*args, **kwargs):
        assert args[1] == str(replacement_file)
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="episode:9036",
            title="Mister Rogers' Neighborhood",
            season_number=4,
            episode_number=36,
            year=None,
            match_confidence="high",
        )

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.scan_service.relink_finding", fake_relink_finding, raising=False)
    monkeypatch.setattr("app.services.match_service.relink_finding", fake_relink_finding)
    monkeypatch.setattr("app.services.scan_service.match_tv_path", fake_match_tv_path)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path=str(old_file),
                file_name=old_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:842",
                title="ER",
                season_number=4,
                episode_number=36,
                file_size_bytes=20_000,
                duration_seconds=10.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id)
        assert run.files_seen == 1
        assert finding is not None
        assert finding.status == "resolved"
        assert finding.manager_entity_id == "episode:9036"
        assert finding.title == "Mister Rogers' Neighborhood"
        assert run.suspicious_found == 0


@pytest.mark.asyncio
async def test_verify_scan_finds_renamed_episode_in_same_folder_when_original_is_gone(monkeypatch, tmp_path: Path):
    season_dir = tmp_path / "tv" / "Mister Rogers Neighborhood" / "Season 04"
    season_dir.mkdir(parents=True)
    old_file = season_dir / "Mister Rogers' Neighborhood - S04E57 - Babysitters and Caring for Children.mkv"
    replacement_file = season_dir / "Mister Rogers' Neighborhood (1968) - S04E57 - Babysitters and Caring for Children [HDTV-1080p][AAC 2.0][x264]-PoF.mkv"
    replacement_file.write_bytes(b"x" * 2_000_000)

    async def fake_probe_file(path: str):
        assert path == str(replacement_file)
        return ProbeResult(
            True,
            1650.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1650.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "episodeFileId": None}

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.SonarrClient", _StubSonarrClient)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path=str(old_file),
                file_name=old_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:863",
                title="Mister Rogers' Neighborhood",
                season_number=4,
                episode_number=57,
                file_size_bytes=20_000,
                duration_seconds=10.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id)
        assert run.files_seen == 1
        assert finding is not None
        assert finding.status == "resolved"
        assert run.suspicious_found == 0


@pytest.mark.asyncio
async def test_verify_scan_finds_same_folder_replacement_even_when_stored_title_is_stale(monkeypatch, tmp_path: Path):
    season_dir = tmp_path / "tv" / "Wolfblood" / "Season 04"
    season_dir.mkdir(parents=True)
    old_file = season_dir / "Wolfblood - S04E01 - Captivity.mp4"
    replacement_file = season_dir / "Wolfblood - S04E01 - Captivity.mkv"
    replacement_file.write_bytes(b"x" * 2_000_000)

    async def fake_probe_file(path: str):
        assert path == str(replacement_file)
        return ProbeResult(
            True,
            1650.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1650.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "episodeFileId": None}

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.SonarrClient", _StubSonarrClient)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path=str(old_file),
                file_name=old_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:135475",
                title="V",
                season_number=4,
                episode_number=1,
                file_size_bytes=20_000,
                duration_seconds=10.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id)
        assert run.files_seen == 1
        assert finding is not None
        assert finding.status == "resolved"
        assert run.suspicious_found == 0


@pytest.mark.asyncio
async def test_verify_scan_maps_arr_path_back_to_local_root(monkeypatch, tmp_path: Path):
    local_root = tmp_path / "mnt" / "RAYNAS" / "TV Shows"
    replacement_file = local_root / "Broken Show" / "Season 01" / "Broken Show - S01E01 - Fixed Release.mkv"
    replacement_file.parent.mkdir(parents=True)
    replacement_file.write_bytes(b"x" * 2_000_000)

    async def fake_probe_file(path: str):
        assert path == str(replacement_file)
        return ProbeResult(
            True,
            1800.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1800.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "episodeFileId": 321}

        async def get_episode_file(self, episode_file_id: int):
            return {"id": episode_file_id, "path": "/tv/Broken Show/Season 01/Broken Show - S01E01 - Fixed Release.mkv"}

        async def all_series(self):
            return []

    async def fake_match_tv_path(*args, **kwargs):
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="episode:55",
            title="Broken Show",
            season_number=1,
            episode_number=1,
            year=None,
            match_confidence="high",
        )

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.scan_service.match_tv_path", fake_match_tv_path)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="sonarr",
                    manager_root_path="/tv",
                    local_root_path=str(local_root),
                    enabled=True,
                )
            )
            finding = Finding(
                file_path=str(local_root / "Broken Show" / "Season 01" / "Broken.Show.S01E01.mkv"),
                file_name="Broken.Show.S01E01.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:55",
                title="Broken Show",
                season_number=1,
                episode_number=1,
                file_size_bytes=20_000,
                duration_seconds=10.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id)
        assert run.files_seen == 1
        assert run.suspicious_found == 0
        assert finding.status == "resolved"


@pytest.mark.asyncio
async def test_verify_scan_finds_same_folder_movie_replacement_even_when_stored_title_is_stale(monkeypatch, tmp_path: Path):
    movie_dir = tmp_path / "movies" / "The Matrix (1999)"
    movie_dir.mkdir(parents=True)
    old_file = movie_dir / "The.Matrix.1999.mp4"
    replacement_file = movie_dir / "The.Matrix.1999.mkv"
    replacement_file.write_bytes(b"x" * 3_000_000)

    async def fake_probe_file(path: str):
        assert path == str(replacement_file)
        return ProbeResult(
            True,
            7200.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "7200.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "radarr":
            return SimpleNamespace(enabled=True, base_url="http://radarr:7878", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubRadarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_movie(self, movie_id: int):
            return {"id": movie_id, "movieFileId": None, "movieFile": {}}

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.RadarrClient", _StubRadarrClient)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path=str(old_file),
                file_name=old_file.name,
                media_kind="movie",
                manager_kind="radarr",
                manager_entity_id="42",
                title="M",
                year=1999,
                file_size_bytes=20_000,
                duration_seconds=10.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id)
        assert run.files_seen == 1
        assert finding is not None
        assert finding.status == "resolved"
        assert run.suspicious_found == 0


@pytest.mark.asyncio
async def test_verify_scan_falls_back_to_same_local_folder_when_arr_path_is_not_mapped(monkeypatch, tmp_path: Path):
    source_file = tmp_path / "mnt" / "RAYNAS" / "TV Shows" / "Broken Show" / "Season 01" / "Broken.Show.S01E01.mkv"
    source_file.parent.mkdir(parents=True)
    replacement_file = source_file.parent / "Broken Show - S01E01 - Fixed Release.mkv"
    replacement_file.write_bytes(b"x" * 2_000_000)
    finding_id_holder = {}

    async def fake_probe_file(path: str):
        assert path == str(replacement_file)
        return ProbeResult(
            True,
            1800.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1800.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="encrypted")
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "episodeFileId": 321}

        async def get_episode_file(self, episode_file_id: int):
            return {"id": episode_file_id, "path": "/tv/Broken Show/Season 01/Broken Show - S01E01 - Fixed Release.mkv"}

        async def all_series(self):
            return []

    async def fake_match_tv_path(*args, **kwargs):
        assert args[1] == str(replacement_file)
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="episode:55",
            title="Broken Show",
            season_number=1,
            episode_number=1,
            year=None,
            match_confidence="high",
        )

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.scan_service.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.services.scan_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.scan_service.match_tv_path", fake_match_tv_path)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.execute(delete(LibraryRoot))
            session.add(
                Finding(
                    file_path=str(source_file),
                    file_name=source_file.name,
                    media_kind="tv",
                    manager_kind="sonarr",
                    manager_entity_id="episode:55",
                    title="Broken Show",
                    season_number=1,
                    episode_number=1,
                    file_size_bytes=20_000,
                    duration_seconds=10.0,
                    resolution="1920x1080",
                    codec_video="h264",
                    codec_audio="aac",
                    suspicion_score=90,
                    confidence="high",
                    proposed_action="search_replacement",
                    status="open",
                    ignored=False,
                )
            )
            await session.flush()
            row = (
                await session.execute(select(Finding).where(Finding.file_path == str(source_file)))
            ).scalar_one()
            finding_id_holder["id"] = row.id

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id_holder["id"]], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id_holder["id"])
        assert run.files_seen == 1
        assert run.suspicious_found == 0
        assert finding.status == "resolved"


@pytest.mark.asyncio
async def test_verify_scan_skips_disappeared_target_without_warning(monkeypatch, tmp_path: Path, caplog):
    target_file = tmp_path / "tv" / "Broken Show" / "Season 01" / "Broken.Show.S01E01.mkv"
    target_file.parent.mkdir(parents=True)

    async def fake_probe_file(path: str):
        raise AssertionError("probe_file should not run when the verify target disappears before stat")

    async def fake_verification_target_path(*args, **kwargs):
        return str(target_file)

    original_stat = Path.stat

    def fake_stat(self, *args, **kwargs):
        if str(self) == str(target_file):
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service._verification_target_path", fake_verification_target_path)
    monkeypatch.setattr(Path, "stat", fake_stat)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path=str(target_file),
                file_name=target_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:55",
                title="Broken Show",
                season_number=1,
                episode_number=1,
                file_size_bytes=20_000,
                duration_seconds=10.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    caplog.set_level("WARNING", logger="app.services.scan_service")

    async with SessionLocal() as session:
        async with session.begin():
            run = await run_verify_scan(session, [finding_id], actor="test")

    async with SessionLocal() as session:
        finding = await session.get(Finding, finding_id)
        assert run.files_seen == 0
        assert run.suspicious_found == 0
        assert finding is not None
        assert finding.status == "resolved"

    assert not [
        record
        for record in caplog.records
        if record.name == "app.services.scan_service" and "verify stat failed" in record.getMessage()
    ]


@pytest.mark.asyncio
async def test_recover_abandoned_scans_marks_running_rows_interrupted():
    await init_db()
    marker = "recover-abandoned-scan-test"
    async with SessionLocal() as session:
        async with session.begin():
            session.add(
                ScanRun(
                    status="running",
                    files_seen=12,
                    suspicious_found=3,
                    notes=f'{{"scope":"verify","marker":"{marker}","total_files":20,"current_file":"/media/Show/Episode 01.mkv"}}',
                )
            )

    recovered = await recover_abandoned_scans(actor="test")

    async with SessionLocal() as session:
        run = (
            await session.execute(select(ScanRun).where(ScanRun.notes.like(f"%{marker}%")).order_by(ScanRun.id.desc()).limit(1))
        ).scalar_one()
        assert recovered >= 1
        assert run.status == "interrupted"
        assert run.completed_at is not None
        assert '"phase":"interrupted"' in (run.notes or "")


@pytest.mark.asyncio
async def test_start_scan_can_be_gracefully_interrupted(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "movies"
    media_root.mkdir(parents=True)
    first_file = media_root / "Broken.Movie.2024.Part1.mkv"
    second_file = media_root / "Broken.Movie.2024.Part2.mkv"
    first_file.write_bytes(b"x" * 300_000)
    second_file.write_bytes(b"x" * 300_000)

    first_started = asyncio.Event()
    allow_first_to_finish = asyncio.Event()

    async def fake_probe_file(path: str):
        if path == str(first_file):
            first_started.set()
            await allow_first_to_finish.wait()
        return ProbeResult(
            True,
            1800.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1800.0"}},
            None,
        )

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.execute(delete(ScanRun))
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="radarr",
                    manager_root_path="/movies",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    scan = await start_scan(actor="test")
    assert scan is not None

    await asyncio.wait_for(first_started.wait(), timeout=5)
    run_id = await request_scan_stop(actor="test")
    assert run_id == scan.id
    allow_first_to_finish.set()

    for _ in range(60):
        async with SessionLocal() as session:
            refreshed = await session.get(ScanRun, scan.id)
            if refreshed and refreshed.status != "running":
                break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("scan did not stop in time")

    async with SessionLocal() as session:
        refreshed = await session.get(ScanRun, scan.id)
        assert refreshed is not None
        assert refreshed.status == "interrupted"
        assert refreshed.files_seen == 1
        assert "operator request" in (refreshed.notes or "")


@pytest.mark.asyncio
async def test_start_scan_can_resume_from_interrupted_checkpoint(monkeypatch, tmp_path: Path):
    media_root = tmp_path / "movies"
    media_root.mkdir(parents=True)
    first_file = media_root / "Broken.Movie.2024.Part1.mkv"
    second_file = media_root / "Broken.Movie.2024.Part2.mkv"
    first_file.write_bytes(b"x" * 300_000)
    second_file.write_bytes(b"x" * 300_000)

    first_started = asyncio.Event()
    allow_first_to_finish = asyncio.Event()
    probes = {str(first_file): 0, str(second_file): 0}

    async def fake_probe_file(path: str):
        probes[path] += 1
        if path == str(first_file) and probes[path] == 1:
            first_started.set()
            await allow_first_to_finish.wait()
        return ProbeResult(
            True,
            1800.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1800.0"}},
            None,
        )

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.execute(delete(ScanRun))
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="radarr",
                    manager_root_path="/movies",
                    local_root_path=str(media_root),
                    enabled=True,
                )
            )

    interrupted_scan = await start_scan(actor="test")
    assert interrupted_scan is not None

    await asyncio.wait_for(first_started.wait(), timeout=5)
    run_id = await request_scan_stop(actor="test")
    assert run_id == interrupted_scan.id
    allow_first_to_finish.set()

    for _ in range(60):
        async with SessionLocal() as session:
            refreshed = await session.get(ScanRun, interrupted_scan.id)
            if refreshed and refreshed.status == "interrupted":
                break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("scan did not interrupt in time")

    resumed_scan = await start_scan(actor="test", resume=True)
    assert resumed_scan is not None

    for _ in range(60):
        async with SessionLocal() as session:
            refreshed = await session.get(ScanRun, resumed_scan.id)
            if refreshed and refreshed.status == "completed":
                break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("resumed scan did not finish in time")

    async with SessionLocal() as session:
        interrupted = await session.get(ScanRun, interrupted_scan.id)
        resumed = await session.get(ScanRun, resumed_scan.id)
        assert interrupted is not None
        assert resumed is not None
        assert interrupted.status == "interrupted"
        assert resumed.status == "completed"
        assert resumed.files_seen == 2
        assert resumed.suspicious_found == 0
        assert '"resumed_from_scan_id":' in (resumed.notes or "")
        assert probes[str(first_file)] == 1
        assert probes[str(second_file)] == 1


@pytest.mark.asyncio
async def test_start_verify_scan_persists_current_target_while_running(monkeypatch, tmp_path: Path):
    target_file = tmp_path / "tv" / "Broken Show" / "Season 01" / "Broken.Show.S01E01.mkv"
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"x" * 300_000)

    probe_started = asyncio.Event()
    allow_probe_to_finish = asyncio.Event()

    async def fake_probe_file(path: str):
        assert path == str(target_file)
        probe_started.set()
        await allow_probe_to_finish.wait()
        return ProbeResult(
            True,
            1800.0,
            1920,
            1080,
            "h264",
            ["aac"],
            {"streams": [{"codec_type": "video"}], "format": {"duration": "1800.0"}},
            None,
        )

    async def fake_get_integration(session, kind):
        return None

    monkeypatch.setattr("app.services.scan_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.scan_service.get_integration", fake_get_integration)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.execute(delete(ScanRun))
            finding = Finding(
                file_path=str(target_file),
                file_name=target_file.name,
                media_kind="tv",
                manager_kind=None,
                manager_entity_id=None,
                title="Broken Show",
                season_number=1,
                episode_number=1,
                file_size_bytes=target_file.stat().st_size,
                duration_seconds=20.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            finding_id = finding.id

    scan = await start_verify_scan([finding_id], actor="test")
    assert scan is not None

    await asyncio.wait_for(probe_started.wait(), timeout=5)

    for _ in range(50):
        async with SessionLocal() as session:
            refreshed = await session.get(ScanRun, scan.id)
            if refreshed and refreshed.status == "running" and target_file.name in (refreshed.notes or ""):
                assert refreshed.files_seen == 0
                break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("verify scan did not persist current target while running")

    allow_probe_to_finish.set()

    for _ in range(50):
        async with SessionLocal() as session:
            refreshed = await session.get(ScanRun, scan.id)
            if refreshed and refreshed.status != "running":
                break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("verify scan did not complete in time")
