from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.domain.finding_state import derive_finding_state
from app.domain.enums import JobStatus, ManagerKind
from app.domain.value_objects import ProbeResult
from app.persistence.db import SessionLocal, init_db
from app.persistence.models import Finding, FindingReason, RemediationAttempt, RemediationJob
from app.services.remediation_service import execute_job


class _StubRadarrClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

    async def refresh_movie(self, movie_ids: list[int]):
        return {"name": "RefreshMovie", "movieIds": movie_ids}

    async def movies_search(self, movie_ids: list[int]):
        return {"name": "MoviesSearch", "movieIds": movie_ids}


@pytest.mark.asyncio
async def test_remediation_job_marks_finding_resolved(monkeypatch, tmp_path: Path):
    media_file = tmp_path / "Resolved.Movie.2024.mkv"
    media_file.write_bytes(b"x" * 300_000)

    async def fake_get_integration(session, kind):
        return SimpleNamespace(enabled=True, base_url="http://radarr:7878", api_key="secret")

    async def fake_probe_file(path: str):
        return ProbeResult(True, 7200.0, 1920, 1080, "h264", ["aac"], {"streams": [], "format": {}}, None)

    async def fake_relink_finding(session, finding):
        return SimpleNamespace(
            manager_kind=ManagerKind.RADARR,
            manager_entity_id="12",
            title="Resolved Movie",
            season_number=None,
            episode_number=None,
            year=2024,
            match_confidence="high",
        )

    monkeypatch.setattr("app.services.remediation_service.RadarrClient", _StubRadarrClient)
    monkeypatch.setattr("app.services.remediation_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.remediation_service.probe_file", fake_probe_file)
    monkeypatch.setattr("app.services.remediation_service.relink_finding", fake_relink_finding)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            for model in (RemediationAttempt, RemediationJob, Finding):
                await session.execute(model.__table__.delete())
            finding = Finding(
                file_path=str(media_file),
                file_name=media_file.name,
                media_kind="movie",
                manager_kind="radarr",
                manager_entity_id="12",
                title="Resolved Movie",
                year=2024,
                file_size_bytes=media_file.stat().st_size,
                duration_seconds=20.0,
                resolution="1920x1080",
                codec_video="h264",
                codec_audio="aac",
                suspicion_score=90,
                confidence="high",
                proposed_action="rescan_only",
                status="open",
                ignored=False,
            )
            session.add(finding)
            await session.flush()
            session.add(
                RemediationJob(
                    finding_id=finding.id,
                    action_type="rescan_only",
                    status="queued",
                    attempt_count=0,
                    requested_by="test",
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            job = (await session.execute(select(RemediationJob).order_by(RemediationJob.id.desc()))).scalar_one()
            await execute_job(session, job.id, actor="test")

    async with SessionLocal() as session:
        job = (await session.execute(select(RemediationJob).order_by(RemediationJob.id.desc()))).scalar_one()
        finding = (await session.execute(select(Finding).order_by(Finding.id.desc()))).scalar_one()
        attempts = (await session.execute(select(RemediationAttempt))).scalars().all()
        assert job.status == JobStatus.SUCCEEDED.value
        assert finding.status == "resolved"
        assert attempts


@pytest.mark.asyncio
async def test_remediation_relinks_unmanaged_finding_before_search(monkeypatch, tmp_path: Path):
    media_file = tmp_path / "Broken.Show.S01E01.mkv"
    media_file.write_bytes(b"x" * 300_000)

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_cutoff_unmet_episode(self, episode_id: int):
            return {"id": episode_id}

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "seriesId": 77}

        async def episode_search(self, episode_ids: list[int]):
            return {"name": "EpisodeSearch", "episodeIds": episode_ids}

        async def series_search(self, series_id: int):
            return {"name": "SeriesSearch", "seriesId": series_id}

        async def rescan_series(self, series_id: int):
            return {"name": "RescanSeries", "seriesId": series_id}

    async def fake_get_integration(session, kind):
        return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="secret")

    async def fake_relink_finding(session, finding):
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="55",
            title="Broken Show",
            season_number=1,
            episode_number=1,
            year=None,
            match_confidence="high",
        )

    async def fake_probe_file(path: str):
        return ProbeResult(True, 1800.0, 1920, 1080, "h264", ["aac"], {"streams": [], "format": {}}, None)

    monkeypatch.setattr("app.services.remediation_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.remediation_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.remediation_service.relink_finding", fake_relink_finding)
    monkeypatch.setattr("app.services.remediation_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            for model in (RemediationAttempt, RemediationJob, FindingReason, Finding):
                await session.execute(model.__table__.delete())
            finding = Finding(
                file_path=str(media_file),
                file_name=media_file.name,
                media_kind="tv",
                manager_kind=None,
                manager_entity_id=None,
                title="Broken Show",
                season_number=1,
                episode_number=1,
                file_size_bytes=media_file.stat().st_size,
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
            session.add(
                RemediationJob(
                    finding_id=finding.id,
                    action_type="search_replacement",
                    status="queued",
                    attempt_count=0,
                    requested_by="test-search",
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            job = (
                await session.execute(
                    select(RemediationJob)
                    .where(RemediationJob.requested_by == "test-search")
                    .order_by(RemediationJob.id.desc())
                )
            ).scalar_one()
            await execute_job(session, job.id, actor="test")

    async with SessionLocal() as session:
        job = (
            await session.execute(
                select(RemediationJob)
                .where(RemediationJob.requested_by == "test-search")
                .order_by(RemediationJob.id.desc())
            )
        ).scalar_one()
        finding = (
            await session.execute(select(Finding).where(Finding.file_path == str(media_file)))
        ).scalar_one()
        attempts = (
            await session.execute(select(RemediationAttempt).where(RemediationAttempt.job_id == job.id).order_by(RemediationAttempt.id.desc()))
        ).scalars().all()
        assert job.status == JobStatus.SUCCEEDED.value
        assert finding.manager_kind == "sonarr"
        assert finding.manager_entity_id == "55"
        assert attempts[0].response_summary
        assert "EpisodeSearch" in attempts[0].response_summary


@pytest.mark.asyncio
async def test_remediation_refreshes_stale_sonarr_link_before_search(monkeypatch, tmp_path: Path):
    media_file = tmp_path / "Mister Rogers' Neighborhood - S04E36 - Show 1166.mkv"
    media_file.write_bytes(b"x" * 300_000)

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_cutoff_unmet_episode(self, episode_id: int):
            return {"id": episode_id}

        async def get_episode_by_id(self, episode_id: int):
            if episode_id == 9036:
                return {"id": episode_id, "seriesId": 77}
            raise AssertionError(f"unexpected episode id {episode_id}")

        async def episode_search(self, episode_ids: list[int]):
            assert episode_ids == [9036]
            return {"name": "EpisodeSearch", "episodeIds": episode_ids}

        async def series_search(self, series_id: int):
            return {"name": "SeriesSearch", "seriesId": series_id}

        async def rescan_series(self, series_id: int):
            return {"name": "RescanSeries", "seriesId": series_id}

    async def fake_get_integration(session, kind):
        return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="secret")

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

    async def fake_probe_file(path: str):
        return ProbeResult(True, 1800.0, 1920, 1080, "h264", ["aac"], {"streams": [], "format": {}}, None)

    monkeypatch.setattr("app.services.remediation_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.remediation_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.remediation_service.relink_finding", fake_relink_finding)
    monkeypatch.setattr("app.services.remediation_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            for model in (RemediationAttempt, RemediationJob, FindingReason, Finding):
                await session.execute(model.__table__.delete())
            finding = Finding(
                file_path=str(media_file),
                file_name=media_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:842",
                title="ER",
                season_number=4,
                episode_number=36,
                file_size_bytes=media_file.stat().st_size,
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
            session.add(
                RemediationJob(
                    finding_id=finding.id,
                    action_type="search_replacement",
                    status="queued",
                    attempt_count=0,
                    requested_by="test-refresh",
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            job = (
                await session.execute(
                    select(RemediationJob)
                    .where(RemediationJob.requested_by == "test-refresh")
                    .order_by(RemediationJob.id.desc())
                )
            ).scalar_one()
            await execute_job(session, job.id, actor="test")

    async with SessionLocal() as session:
        job = (
            await session.execute(
                select(RemediationJob)
                .where(RemediationJob.requested_by == "test-refresh")
                .order_by(RemediationJob.id.desc())
            )
        ).scalar_one()
        finding = (
            await session.execute(select(Finding).where(Finding.file_path == str(media_file)))
        ).scalar_one()
        attempts = (
            await session.execute(select(RemediationAttempt).where(RemediationAttempt.job_id == job.id).order_by(RemediationAttempt.id.desc()))
        ).scalars().all()
        assert job.status == JobStatus.SUCCEEDED.value
        assert finding.manager_entity_id == "episode:9036"
        assert finding.title == "Mister Rogers' Neighborhood"
        assert "EpisodeSearch" in attempts[0].response_summary


@pytest.mark.asyncio
async def test_remediation_fails_fast_when_sonarr_cutoff_is_already_met(monkeypatch, tmp_path: Path):
    media_file = tmp_path / "Masters.of.Sex.S04E01.mkv"
    media_file.write_bytes(b"x" * 300_000)

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_cutoff_unmet_episode(self, episode_id: int):
            return None

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "seriesId": 77}

        async def episode_search(self, episode_ids: list[int]):
            raise AssertionError("EpisodeSearch should not run when cutoff is already met")

        async def series_search(self, series_id: int):
            raise AssertionError("SeriesSearch should not run when cutoff is already met")

        async def rescan_series(self, series_id: int):
            return {"name": "RescanSeries", "seriesId": series_id}

    async def fake_get_integration(session, kind):
        return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="secret")

    async def fake_relink_finding(session, finding):
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="episode:261557",
            title="Masters of Sex",
            season_number=4,
            episode_number=1,
            year=None,
            match_confidence="high",
        )

    async def fake_probe_file(path: str):
        return ProbeResult(True, 1800.0, 1920, 1080, "h264", ["aac"], {"streams": [], "format": {}}, None)

    monkeypatch.setattr("app.services.remediation_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.remediation_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.remediation_service.relink_finding", fake_relink_finding)
    monkeypatch.setattr("app.services.remediation_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            for model in (RemediationAttempt, RemediationJob, FindingReason, Finding):
                await session.execute(model.__table__.delete())
            finding = Finding(
                file_path=str(media_file),
                file_name=media_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:261557",
                title="Masters of Sex",
                season_number=4,
                episode_number=1,
                file_size_bytes=media_file.stat().st_size,
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
            session.add(
                RemediationJob(
                    finding_id=finding.id,
                    action_type="search_replacement",
                    status="queued",
                    attempt_count=0,
                    requested_by="test-cutoff-met",
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            job = (
                await session.execute(
                    select(RemediationJob)
                    .where(RemediationJob.requested_by == "test-cutoff-met")
                    .order_by(RemediationJob.id.desc())
                )
            ).scalar_one()
            await execute_job(session, job.id, actor="test")

    async with SessionLocal() as session:
        job = (
            await session.execute(
                select(RemediationJob)
                .where(RemediationJob.requested_by == "test-cutoff-met")
                .order_by(RemediationJob.id.desc())
            )
        ).scalar_one()
        attempts = (
            await session.execute(select(RemediationAttempt).where(RemediationAttempt.job_id == job.id).order_by(RemediationAttempt.id.desc()))
        ).scalars().all()
        assert job.status == JobStatus.FAILED.value
        assert "cutoff already met" in (job.last_error or "").lower()
        assert attempts
        assert "cutoff already met" in (attempts[0].response_summary or "").lower()


@pytest.mark.asyncio
async def test_remediation_delete_search_replacement_uses_manager_delete_then_search(monkeypatch, tmp_path: Path):
    media_file = tmp_path / "Wolfblood.S04E01.mkv"
    media_file.write_bytes(b"x" * 300_000)
    calls = []

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            calls.append(("get_episode_by_id", episode_id))
            return {"id": episode_id, "seriesId": 262554, "episodeFileId": 999}

        async def delete_episode_file(self, episode_file_id: int):
            calls.append(("delete_episode_file", episode_file_id))
            return {"status": 200}

        async def episode_search(self, episode_ids: list[int]):
            calls.append(("episode_search", episode_ids))
            return {"name": "EpisodeSearch", "episodeIds": episode_ids}

        async def series_search(self, series_id: int):
            raise AssertionError("SeriesSearch should not run")

        async def rescan_series(self, series_id: int):
            raise AssertionError("RescanSeries should not run")

    async def fake_get_integration(session, kind):
        return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="secret")

    async def fake_relink_finding(session, finding):
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="episode:135475",
            title="Wolfblood",
            season_number=4,
            episode_number=1,
            year=None,
            match_confidence="high",
        )

    async def fake_probe_file(path: str):
        return ProbeResult(True, 1800.0, 1920, 1080, "h264", ["aac"], {"streams": [], "format": {}}, None)

    monkeypatch.setattr("app.services.remediation_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.remediation_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.remediation_service.relink_finding", fake_relink_finding)
    monkeypatch.setattr("app.services.remediation_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            for model in (RemediationAttempt, RemediationJob, FindingReason, Finding):
                await session.execute(model.__table__.delete())
            finding = Finding(
                file_path=str(media_file),
                file_name=media_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:135475",
                title="Wolfblood",
                season_number=4,
                episode_number=1,
                file_size_bytes=media_file.stat().st_size,
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
            session.add(
                RemediationJob(
                    finding_id=finding.id,
                    action_type="delete_search_replacement",
                    status="queued",
                    attempt_count=0,
                    requested_by="test-delete-search",
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            job = (
                await session.execute(
                    select(RemediationJob)
                    .where(RemediationJob.requested_by == "test-delete-search")
                    .order_by(RemediationJob.id.desc())
                )
            ).scalar_one()
            await execute_job(session, job.id, actor="test")

    async with SessionLocal() as session:
        job = (
            await session.execute(
                select(RemediationJob)
                .where(RemediationJob.requested_by == "test-delete-search")
                .order_by(RemediationJob.id.desc())
            )
        ).scalar_one()
        attempts = (
            await session.execute(select(RemediationAttempt).where(RemediationAttempt.job_id == job.id).order_by(RemediationAttempt.id.asc()))
        ).scalars().all()
        assert job.status == JobStatus.SUCCEEDED.value
        assert [attempt.step_name for attempt in attempts] == ["DeleteEpisodeFile", "EpisodeSearch"]
        assert calls == [
            ("get_episode_by_id", 135475),
            ("delete_episode_file", 999),
            ("episode_search", [135475]),
        ]


@pytest.mark.asyncio
async def test_remediation_search_replacement_stays_pending_verification(monkeypatch, tmp_path: Path):
    media_file = tmp_path / "Wolfblood.S04E02.mkv"
    media_file.write_bytes(b"x" * 300_000)

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_cutoff_unmet_episode(self, episode_id: int):
            return {"id": episode_id}

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "seriesId": 262554}

        async def episode_search(self, episode_ids: list[int]):
            return {"name": "EpisodeSearch", "episodeIds": episode_ids}

        async def series_search(self, series_id: int):
            raise AssertionError("SeriesSearch should not run")

        async def rescan_series(self, series_id: int):
            raise AssertionError("RescanSeries should not run")

    async def fake_get_integration(session, kind):
        return SimpleNamespace(enabled=True, base_url="http://sonarr:8989", api_key="secret")

    async def fake_relink_finding(session, finding):
        return SimpleNamespace(
            manager_kind=ManagerKind.SONARR,
            manager_entity_id="episode:135476",
            title="Wolfblood",
            season_number=4,
            episode_number=2,
            year=None,
            match_confidence="high",
        )

    async def fake_probe_file(path: str):
        raise AssertionError("search replacement should not immediately probe the old file path")

    monkeypatch.setattr("app.services.remediation_service.SonarrClient", _StubSonarrClient)
    monkeypatch.setattr("app.services.remediation_service.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.remediation_service.relink_finding", fake_relink_finding)
    monkeypatch.setattr("app.services.remediation_service.probe_file", fake_probe_file)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            for model in (RemediationAttempt, RemediationJob, FindingReason, Finding):
                await session.execute(model.__table__.delete())
            finding = Finding(
                file_path=str(media_file),
                file_name=media_file.name,
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:135476",
                title="Wolfblood",
                season_number=4,
                episode_number=2,
                file_size_bytes=media_file.stat().st_size,
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
            session.add(
                RemediationJob(
                    finding_id=finding.id,
                    action_type="search_replacement",
                    status="queued",
                    attempt_count=0,
                    requested_by="test-pending-verify",
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            job = (
                await session.execute(
                    select(RemediationJob)
                    .where(RemediationJob.requested_by == "test-pending-verify")
                    .order_by(RemediationJob.id.desc())
                )
            ).scalar_one()
            await execute_job(session, job.id, actor="test")

    async with SessionLocal() as session:
        job = (
            await session.execute(
                select(RemediationJob)
                .where(RemediationJob.requested_by == "test-pending-verify")
                .order_by(RemediationJob.id.desc())
            )
        ).scalar_one()
        finding = (
            await session.execute(
                select(Finding)
                .options(selectinload(Finding.jobs))
                .where(Finding.file_path == str(media_file))
            )
        ).scalar_one()
        assert job.status == JobStatus.SUCCEEDED.value
        assert finding.status == "open"
        assert derive_finding_state(finding) == "pending_verify"


@pytest.mark.asyncio
async def test_remediation_marks_job_failed_when_relink_raises(monkeypatch, tmp_path: Path):
    media_file = tmp_path / "Unlinked.Show.S01E01.mkv"
    media_file.write_bytes(b"x" * 300_000)

    async def fake_relink_finding(session, finding):
        raise RuntimeError("Sonarr relink timed out")

    monkeypatch.setattr("app.services.remediation_service.relink_finding", fake_relink_finding)

    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            for model in (RemediationAttempt, RemediationJob, FindingReason, Finding):
                await session.execute(model.__table__.delete())
            finding = Finding(
                file_path=str(media_file),
                file_name=media_file.name,
                media_kind="tv",
                manager_kind=None,
                manager_entity_id=None,
                title="Unlinked Show",
                season_number=1,
                episode_number=1,
                file_size_bytes=media_file.stat().st_size,
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
            session.add(
                RemediationJob(
                    finding_id=finding.id,
                    action_type="search_replacement",
                    status="queued",
                    attempt_count=0,
                    requested_by="test-relink-timeout",
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            job = (
                await session.execute(
                    select(RemediationJob)
                    .where(RemediationJob.requested_by == "test-relink-timeout")
                    .order_by(RemediationJob.id.desc())
                )
            ).scalar_one()
            await execute_job(session, job.id, actor="test")

    async with SessionLocal() as session:
        job = (
            await session.execute(
                select(RemediationJob)
                .where(RemediationJob.requested_by == "test-relink-timeout")
                .order_by(RemediationJob.id.desc())
            )
        ).scalar_one()
        attempts = (
            await session.execute(
                select(RemediationAttempt)
                .where(RemediationAttempt.job_id == job.id)
                .order_by(RemediationAttempt.id.desc())
            )
        ).scalars().all()
        assert job.status == JobStatus.FAILED.value
        assert "relink timed out" in (job.last_error or "").lower()
        assert job.attempt_count == 1
        assert attempts
