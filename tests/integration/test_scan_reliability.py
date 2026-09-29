"""Regression coverage for persisted scan outcomes, including background commits."""

import asyncio
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.domain.scan_notes import parse_scan_notes
from app.domain.value_objects import ProbeResult
from app.persistence.models import Base, Finding, LibraryRoot, RemediationJob, ScanRun
from app.services import scan_service as scans
from app.services.rule_service import get_or_create_rule_settings


@pytest_asyncio.fixture
async def library(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path / 'audit.db').as_posix()}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(scans, "SessionLocal", sessions)

    async def no_integration(*args):
        return None

    monkeypatch.setattr(scans, "get_integration", no_integration)
    root = tmp_path / "movies"
    root.mkdir()
    media = root / "Movie.2024.mkv"
    media.write_bytes(b"x" * 200_000)
    async with sessions.begin() as session:
        session.add(LibraryRoot(manager_kind="radarr", manager_root_path="/movies", local_root_path=str(root)))
        finding = Finding(file_path=str(media.resolve()), file_name=media.name, media_kind="movie",
                          suspicion_score=90, status="open", manager_kind="radarr", manager_entity_id="42")
        session.add(finding)
        await session.flush()
        finding_id = finding.id
    yield sessions, media, finding_id
    await engine.dispose()


def healthy_probe():
    return ProbeResult(True, 7200.0, 1920, 1080, "h264", ["aac"],
                       {"streams": [{"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080},
                                    {"codec_type": "audio", "codec_name": "aac"}],
                        "format": {"duration": "7200"}}, None)


@pytest.mark.asyncio
async def test_library_scan_resolves_repaired_file_and_refreshes_evidence(library, monkeypatch):
    sessions, media, fid = library

    async def probe(path):
        return healthy_probe()

    monkeypatch.setattr(scans, "probe_file", probe)
    async with sessions.begin() as session:
        run = await scans.run_scan(session)
    async with sessions() as session:
        finding = await session.get(Finding, fid)
        assert finding.status == "resolved"
        assert finding.suspicion_score == 0
        assert finding.duration_seconds == 7200
        assert finding.last_scan_run_id == run.id
        assert finding.manager_entity_id == "42"


@pytest.mark.asyncio
async def test_scan_preserves_ignore_and_manager_link_when_manager_unavailable(library, monkeypatch):
    sessions, media, fid = library
    async with sessions.begin() as session:
        finding = await session.get(Finding, fid)
        finding.ignored = True
        finding.status = "ignored"

    async def probe(path):
        return ProbeResult(False, None, None, None, None, [], None, "broken container")

    monkeypatch.setattr(scans, "probe_file", probe)
    async with sessions.begin() as session:
        await scans.run_scan(session)
    async with sessions() as session:
        finding = await session.get(Finding, fid)
        assert finding.ignored is True
        assert finding.status == "ignored"
        assert finding.manager_entity_id == "42"
        assert not (await session.execute(select(RemediationJob))).scalars().all()


@pytest.mark.asyncio
@pytest.mark.parametrize("background", [False, True])
async def test_verification_persists_resolution_with_progress_commits(library, monkeypatch, background):
    sessions, media, fid = library

    async def target(*args, **kwargs):
        return str(media.resolve())

    async def probe(path):
        return healthy_probe()

    monkeypatch.setattr(scans, "_verification_target_path", target)
    monkeypatch.setattr(scans, "probe_file", probe)
    async with sessions() as session:
        run = ScanRun(status="running", files_seen=0, suspicious_found=0)
        session.add(run)
        await session.flush()
        await scans._perform_verify_scan(session, run, [fid], commit_progress=background)
        await session.commit()
    async with sessions() as session:
        finding = await session.get(Finding, fid)
        assert finding.status == "resolved"
        assert finding.last_scan_run_id == run.id


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [FileNotFoundError, PermissionError])
async def test_unreadable_verify_target_is_skipped_not_resolved(library, monkeypatch, error):
    sessions, media, fid = library
    original_stat = Path.stat

    def stat(path, *args, **kwargs):
        if path == media:
            raise error("file unavailable")
        return original_stat(path, *args, **kwargs)

    async def target(*args, **kwargs):
        return str(media)

    monkeypatch.setattr(scans, "_verification_target_path", target)
    monkeypatch.setattr(Path, "stat", stat)
    async with sessions.begin() as session:
        run = await scans.run_verify_scan(session, [fid])
    async with sessions() as session:
        finding = await session.get(Finding, fid)
        assert finding.status == "open"
        assert finding.last_scan_run_id is None
        assert fid in parse_scan_notes(run.notes)["skipped_findings"]


@pytest.mark.asyncio
async def test_existing_source_is_checked_before_guessing_local_replacement(library, monkeypatch):
    sessions, media, fid = library
    alternate = media.with_suffix(".mp4")
    alternate.write_bytes(b"healthy alternate")

    async def no_relink(*args, **kwargs):
        raise RuntimeError("manager offline")

    monkeypatch.setattr("app.services.match_service.relink_finding", no_relink)
    async with sessions() as session:
        finding = await session.get(Finding, fid)
        target = await scans._verification_target_path(session, finding, sonarr=None, radarr=None,
                    sonarr_api_key=None, radarr_api_key=None, pairs={"sonarr": [], "radarr": []})
        assert target == str(media.resolve())


@pytest.mark.asyncio
async def test_ignored_finding_is_not_automatically_queued(library, monkeypatch):
    sessions, media, fid = library
    async with sessions.begin() as session:
        finding = await session.get(Finding, fid)
        finding.ignored = True
        finding.status = "ignored"
        rules = await get_or_create_rule_settings(session)
        rules.auto_remediation_enabled = True

    from types import SimpleNamespace
    from app.domain.enums import ManagerKind

    async def integration(*args):
        return SimpleNamespace(enabled=True, base_url="http://unused", api_key="secret")

    async def movies(*args):
        return []

    async def match(*args, **kwargs):
        return SimpleNamespace(manager_kind=ManagerKind.RADARR, manager_entity_id="42", title="Movie",
                               season_number=None, episode_number=None, year=2024)

    async def probe(path):
        return ProbeResult(False, None, None, None, None, [], None, "broken")

    monkeypatch.setattr(scans, "get_integration", integration)
    monkeypatch.setattr(scans, "reveal_integration_api_key", lambda row: "secret")
    monkeypatch.setattr(scans.RadarrClient, "all_movies", movies)
    monkeypatch.setattr(scans, "match_movie_path", match)
    monkeypatch.setattr(scans, "probe_file", probe)
    async with sessions.begin() as session:
        await scans.run_scan(session)
    async with sessions() as session:
        assert not (await session.execute(select(RemediationJob))).scalars().all()


@pytest.mark.asyncio
async def test_completed_file_checkpoint_survives_restart(library, monkeypatch):
    sessions, media, fid = library
    async with sessions.begin() as session:
        run = ScanRun(status="running", files_seen=1, suspicious_found=1,
                      notes=scans.merge_scan_notes(None, scope="library", resume_after_file=str(media)))
        session.add(run)
    assert await scans.recover_abandoned_scans() == 1
    async with sessions() as session:
        resumable = await scans.latest_resumable_library_scan(session)
        assert resumable.id == run.id
        assert parse_scan_notes(resumable.notes)["resume_after_file"] == str(media)


@pytest.mark.asyncio
async def test_background_verify_resolves_renamed_source(library, monkeypatch):
    sessions, media, fid = library
    replacement = media.with_name("new-release.mkv")
    media.rename(replacement)

    async def target(*args, **kwargs):
        return str(replacement)

    async def probe(path):
        return healthy_probe()

    monkeypatch.setattr(scans, "_verification_target_path", target)
    monkeypatch.setattr(scans, "probe_file", probe)
    async with sessions() as session:
        run = ScanRun(status="running", files_seen=0, suspicious_found=0)
        session.add(run)
        await session.flush()
        await scans._perform_verify_scan(session, run, [fid], commit_progress=True)
        await session.commit()
    async with sessions() as session:
        assert (await session.get(Finding, fid)).status == "resolved"


@pytest.mark.asyncio
async def test_repair_claim_is_durable_and_not_replayed_after_restart(library):
    from app.services.job_service import claim_next_job, recover_abandoned_jobs

    sessions, media, fid = library
    async with sessions.begin() as session:
        job = RemediationJob(finding_id=fid, action_type="delete_search_replacement", status="queued",
                             requested_by="test", attempt_count=0)
        session.add(job)
    async with sessions.begin() as session:
        assert await claim_next_job(session) == job.id
    # A different session sees the committed claim before any manager request.
    async with sessions.begin() as session:
        claimed = await session.get(RemediationJob, job.id)
        assert claimed.status == "running"
        assert claimed.attempt_count == 1
        assert await claim_next_job(session) is None
    async with sessions.begin() as session:
        assert await recover_abandoned_jobs(session) == 1
    async with sessions() as session:
        failed = await session.get(RemediationJob, job.id)
        assert failed.status == "failed"
        assert "unknown" in failed.last_error
        assert await claim_next_job(session) is None


@pytest.mark.asyncio
async def test_checkpoint_does_not_advance_before_finding_is_saved(library, monkeypatch):
    from types import SimpleNamespace

    sessions, media, fid = library

    async def integration(*args):
        return SimpleNamespace(enabled=True, base_url="http://unused", api_key="secret")

    async def interrupted_lookup(*args):
        raise asyncio.CancelledError()

    async def probe(path):
        return ProbeResult(False, None, None, None, None, [], None, "broken")

    monkeypatch.setattr(scans, "get_integration", integration)
    monkeypatch.setattr(scans, "reveal_integration_api_key", lambda row: "secret")
    monkeypatch.setattr(scans.RadarrClient, "all_movies", interrupted_lookup)
    monkeypatch.setattr(scans, "probe_file", probe)
    monkeypatch.setattr(scans, "PROGRESS_COMMIT_INTERVAL", 1)
    async with sessions.begin() as session:
        run = ScanRun(status="running", files_seen=0, suspicious_found=0)
        session.add(run)
    async with sessions() as session:
        active = await session.get(ScanRun, run.id)
        with pytest.raises(asyncio.CancelledError):
            await scans._perform_scan(session, active, commit_progress=True)
    async with sessions() as session:
        interrupted = await session.get(ScanRun, run.id)
        assert interrupted.files_seen == 0
        assert not parse_scan_notes(interrupted.notes).get("resume_after_file")


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "resolved", "ignored"])
async def test_claimed_job_respects_latest_finding_state(library, monkeypatch, state):
    from types import SimpleNamespace
    from app.services import remediation_service as repairs
    from app.services.job_service import claim_next_job

    sessions, media, fid = library
    calls = []

    async def relink(*args, **kwargs):
        calls.append("relink")
        return repairs._outcome_from_finding(args[1])

    async def integration(*args):
        return SimpleNamespace(enabled=True, base_url="http://unused", api_key="secret")

    async def search(*args):
        calls.append("search")
        return {"id": 10}

    monkeypatch.setattr(repairs, "relink_finding", relink)
    monkeypatch.setattr(repairs, "get_integration", integration)
    monkeypatch.setattr(repairs, "reveal_integration_api_key", lambda row: "secret")
    monkeypatch.setattr(repairs.RadarrClient, "movies_search", search)
    async with sessions.begin() as session:
        finding = await session.get(Finding, fid)
        finding.status = state
        finding.ignored = state == "ignored"
        job = RemediationJob(finding_id=fid, action_type="search_replacement", status="queued",
                             requested_by="test", attempt_count=0)
        session.add(job)
    async with sessions.begin() as session:
        assert await claim_next_job(session) == job.id
    async with sessions.begin() as session:
        await repairs.execute_job(session, job.id, claimed=True)
    async with sessions() as session:
        finished = await session.get(RemediationJob, job.id)
        assert finished.attempt_count == 1
        assert finished.status == ("succeeded" if state == "open" else "cancelled")
        assert calls == (["relink", "search"] if state == "open" else [])
