import asyncio
import datetime as dt
import re

from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.main import app
from app.persistence.db import SessionLocal, init_db
from app.persistence.models import Finding, FindingReason, LibraryRoot, RemediationAttempt, RemediationJob, ScanRun
from tests.conftest import extract_csrf_token


def _login(client: TestClient) -> None:
    login_page = client.get("/login")
    csrf = extract_csrf_token(login_page.text)
    response = client.post(
        "/login",
        data={
            "csrf_token": csrf,
            "next_url": "/",
            "username": "admin",
            "password": "test-admin-password-secure-123",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302


def test_dashboard_ignores_verify_scan_for_zero_files_warning():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(ScanRun))
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="tv",
                    manager_root_path="",
                    local_root_path="/mnt/RAYNAS/TV Shows",
                    enabled=True,
                )
            )
            session.add_all(
                [
                    ScanRun(
                        started_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=10),
                        completed_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=9),
                        status="completed",
                        files_seen=12,
                        suspicious_found=3,
                        notes='{"scope":"library","total_files":12}',
                    ),
                    ScanRun(
                        started_at=dt.datetime.now(dt.UTC),
                        completed_at=dt.datetime.now(dt.UTC),
                        status="completed",
                        files_seen=0,
                        suspicious_found=0,
                        notes='{"scope":"verify","total_files":0,"target_count":4}',
                    ),
                ]
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/")

        assert response.status_code == 200
        assert "The last scan saw zero media files" not in response.text
        assert "12" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(ScanRun))
            await session.execute(delete(LibraryRoot))
            await session.commit()

    asyncio.run(_cleanup())


def test_dashboard_open_findings_uses_real_review_link():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            session.add(
                Finding(
                    file_path="/mnt/RAYNAS/TV Shows/Mister Rogers Neighborhood/Season 04/Mister Rogers' Neighborhood - S04E36 - Show 1166.mkv",
                    file_name="Mister Rogers' Neighborhood - S04E36 - Show 1166.mkv",
                    media_kind="tv",
                    manager_kind="sonarr",
                    suspicion_score=70,
                    confidence="high",
                    proposed_action="rescan_only",
                    status="open",
                    ignored=False,
                    first_seen_at=dt.datetime.now(dt.UTC),
                    last_seen_at=dt.datetime.now(dt.UTC),
                    last_scanned_at=dt.datetime.now(dt.UTC),
                )
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/")

        assert response.status_code == 200
        assert "Recommended" in response.text
        assert "href=\"/findings/" in response.text
        assert ">Review<" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_dashboard_uses_scan_timestamps_for_latest_non_verify_run():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(ScanRun))
            await session.execute(delete(LibraryRoot))
            session.add(
                LibraryRoot(
                    manager_kind="tv",
                    manager_root_path="",
                    local_root_path="/mnt/RAYNAS/TV Shows",
                    enabled=True,
                )
            )
            newer_completed = ScanRun(
                started_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5),
                completed_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=4),
                status="completed",
                files_seen=222,
                suspicious_found=4,
                notes='{"scope":"library","libraries":1}',
            )
            older_interrupted = ScanRun(
                started_at=dt.datetime.now(dt.UTC) - dt.timedelta(hours=2),
                completed_at=dt.datetime.now(dt.UTC) - dt.timedelta(hours=2, minutes=-5),
                status="interrupted",
                files_seen=15,
                suspicious_found=2,
                notes='{"scope":"library","current_file":"/mnt/older-file.mkv"}',
            )
            session.add(newer_completed)
            session.add(older_interrupted)
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/")

        assert response.status_code == 200
        assert re.search(r'<div class="stat-label">Files</div>\s*<div class="stat-value" data-live-scan-files>222</div>', response.text)
        assert "/mnt/older-file.mkv" not in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(ScanRun))
            await session.execute(delete(LibraryRoot))
            await session.commit()

    asyncio.run(_cleanup())


def test_dashboard_shows_resume_button_for_interrupted_library_scan():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(ScanRun))
            session.add(
                ScanRun(
                    started_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5),
                    completed_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=4),
                    status="interrupted",
                    files_seen=15,
                    suspicious_found=2,
                    notes='{"scope":"library","resume_after_file":"/mnt/library/Episode 01.mkv","error":"Library scan interrupted by operator request"}',
                )
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/")

        assert response.status_code == 200
        assert ">Resume scan<" in response.text
        assert 'name="resume" value="1"' in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(ScanRun))
            await session.commit()

    asyncio.run(_cleanup())


def test_dashboard_includes_hidden_resume_form_when_no_interrupted_scan():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(ScanRun))
            session.add(
                ScanRun(
                    started_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5),
                    completed_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=4),
                    status="completed",
                    files_seen=20,
                    suspicious_found=1,
                    notes='{"scope":"library"}',
                )
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/")

        assert response.status_code == 200
        assert 'style="display:inline" class="hidden" data-scan-resume-form' in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(ScanRun))
            await session.commit()

    asyncio.run(_cleanup())
