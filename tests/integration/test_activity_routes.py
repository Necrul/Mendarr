import asyncio
import datetime as dt
import re

from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.main import app
from app.persistence.db import SessionLocal, init_db
from app.persistence.models import AuditEvent, Finding, RemediationAttempt, RemediationJob
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


def test_activity_page_groups_failure_reasons():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            now = dt.datetime.now(dt.UTC)
            session.add_all(
                [
                    AuditEvent(
                        event_type="job_failed",
                        entity_type="remediation_job",
                        entity_id="1",
                        message="Finding is not linked to Sonarr or Radarr - manual review only",
                        created_at=now,
                        actor="worker",
                    ),
                    AuditEvent(
                        event_type="job_failed",
                        entity_type="remediation_job",
                        entity_id="2",
                        message="Finding is not linked to Sonarr or Radarr - manual review only",
                        created_at=now + dt.timedelta(seconds=1),
                        actor="worker",
                    ),
                    AuditEvent(
                        event_type="job_failed",
                        entity_type="remediation_job",
                        entity_id="3",
                        message="Radarr not configured",
                        created_at=now + dt.timedelta(seconds=2),
                        actor="worker",
                    ),
                ]
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/activity")

        assert response.status_code == 200
        assert "What is blocking repairs" in response.text
        assert "Manual review only" in response.text
        assert "2 event(s)" in response.text
        assert "Radarr is not configured" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(AuditEvent))
            await session.commit()

    asyncio.run(_cleanup())


def test_activity_page_respects_page_size_selector():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            now = dt.datetime.now(dt.UTC)
            session.add_all(
                [
                    AuditEvent(
                        event_type="scan_started",
                        entity_type="scan_run",
                        entity_id=str(index),
                        message=f"Scan started {index}",
                        created_at=now + dt.timedelta(seconds=index),
                        actor="admin",
                    )
                    for index in range(12)
                ]
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/activity?page_size=10")

        assert response.status_code == 200
        assert 'option value="10" selected' in response.text
        assert 'data-submit-on-change' in response.text
        assert 'onchange=' not in response.text
        assert "Showing 1-10 of 12 events" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(AuditEvent))
            await session.commit()

    asyncio.run(_cleanup())


def test_activity_page_paginates_past_previous_hard_cap():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            now = dt.datetime.now(dt.UTC)
            session.add_all(
                [
                    AuditEvent(
                        event_type="scan_started",
                        entity_type="scan_run",
                        entity_id=str(index),
                        message=f"Event {index:03d}",
                        created_at=now + dt.timedelta(seconds=index),
                        actor="admin",
                    )
                    for index in range(520)
                ]
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/activity?page=52&page_size=10")

        assert response.status_code == 200
        assert "Showing 511-520 of 520 events" in response.text
        assert "Event 009" in response.text
        assert "Event 010" not in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(AuditEvent))
            await session.commit()

    asyncio.run(_cleanup())


def test_activity_page_uses_event_label_for_failed_jobs_without_command_attempt():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            now = dt.datetime.now(dt.UTC)
            session.add(
                AuditEvent(
                    event_type="job_failed",
                    entity_type="remediation_job",
                    entity_id="77",
                    message='{"error":"Finding is not linked to Sonarr or Radarr - manual review only"}',
                    created_at=now,
                    actor="worker",
                )
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/activity")

        assert response.status_code == 200
        assert "Repair failed" in response.text
        assert "Failed request" not in response.text
        assert "Manual review only" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(AuditEvent))
            await session.commit()

    asyncio.run(_cleanup())


def test_activity_page_does_not_apply_job_command_labels_to_non_job_events():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(AuditEvent))
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            now = dt.datetime.now(dt.UTC)
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Test Show/Season 01/Test.Show.S01E01.mkv",
                file_name="Test.Show.S01E01.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:123",
                suspicion_score=90,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
                first_seen_at=now,
                last_seen_at=now,
                last_scanned_at=now,
            )
            session.add(finding)
            await session.flush()
            job = RemediationJob(
                finding_id=finding.id,
                action_type="search_replacement",
                status="succeeded",
                attempt_count=1,
                requested_by="admin",
            )
            session.add(job)
            await session.flush()
            session.add(
                RemediationAttempt(
                    job_id=job.id,
                    step_name="EpisodeSearch",
                    status="succeeded",
                    request_summary=None,
                    response_summary='{"name":"EpisodeSearch"}',
                    created_at=now,
                )
            )
            session.add(
                AuditEvent(
                    event_type="scan_started",
                    entity_type="remediation_job",
                    entity_id=str(job.id),
                    message="Library scan started",
                    created_at=now + dt.timedelta(seconds=1),
                    actor="admin",
                )
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/activity")

        assert response.status_code == 200
        assert "Scan started" in response.text
        assert "Library scan started" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(AuditEvent))
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_activity_page_uses_job_status_for_queue_count_and_sorts_events_by_time():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(AuditEvent))
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            now = dt.datetime.now(dt.UTC)
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Test Show/Season 01/Test.Show.S01E01.mkv",
                file_name="Test.Show.S01E01.mkv",
                media_kind="tv",
                manager_kind="none",
                manager_entity_id=None,
                suspicion_score=50,
                confidence="medium",
                proposed_action="review",
                status="open",
                ignored=False,
                first_seen_at=now,
                last_seen_at=now,
                last_scanned_at=now,
            )
            session.add(finding)
            await session.flush()
            session.add(
                RemediationJob(
                    finding_id=finding.id,
                    action_type="rescan_only",
                    status="queued",
                    attempt_count=0,
                    requested_by="admin",
                )
            )
            session.add_all(
                [
                    AuditEvent(
                        event_type="integration_saved",
                        entity_type="sonarr",
                        entity_id=None,
                        message="Newest event",
                        created_at=now + dt.timedelta(minutes=5),
                        actor="admin",
                    ),
                    AuditEvent(
                        event_type="library_root_added",
                        entity_type="library_root",
                        entity_id="3",
                        message="Older event",
                        created_at=now,
                        actor="admin",
                    ),
                ]
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/activity")

        assert response.status_code == 200
        assert re.search(r'<div class="stat-label">Queued</div>\s*<div class="stat-value">1</div>', response.text)
        assert response.text.find("Newest event") < response.text.find("Older event")

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(AuditEvent))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())
