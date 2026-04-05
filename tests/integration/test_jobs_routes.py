import asyncio
import datetime as dt

from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.main import app
from app.persistence.db import SessionLocal, init_db
from app.persistence.models import Finding, RemediationAttempt, RemediationJob
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


def test_jobs_page_respects_page_size_selector():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            base_time = dt.datetime.now(dt.UTC)
            for index in range(12):
                finding = Finding(
                    file_path=f"/mnt/RAYNAS/TV Shows/Show/Season 01/Episode {index:02d}.mkv",
                    file_name=f"Episode {index:02d}.mkv",
                    media_kind="tv",
                    manager_kind="sonarr",
                    manager_entity_id=f"episode:{index + 1}",
                    suspicion_score=80,
                    confidence="high",
                    proposed_action="search_replacement",
                    status="open",
                    ignored=False,
                    first_seen_at=base_time,
                    last_seen_at=base_time,
                    last_scanned_at=base_time,
                )
                session.add(finding)
                await session.flush()
                session.add(
                    RemediationJob(
                        finding_id=finding.id,
                        action_type="search_replacement",
                        status="queued",
                        attempt_count=0,
                        requested_by="test",
                        created_at=base_time + dt.timedelta(seconds=index),
                    )
                )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/jobs?page_size=10")

        assert response.status_code == 200
        assert 'option value="10" selected' in response.text
        assert 'data-submit-on-change' in response.text
        assert 'onchange=' not in response.text
        assert "Showing 1-10 of 12 jobs" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_jobs_page_paginates_past_previous_hard_cap():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            base_time = dt.datetime.now(dt.UTC)
            for index in range(320):
                finding = Finding(
                    file_path=f"/mnt/RAYNAS/TV Shows/Long Queue/Season 01/Episode {index:03d}.mkv",
                    file_name=f"Episode {index:03d}.mkv",
                    media_kind="tv",
                    manager_kind="sonarr",
                    manager_entity_id=f"episode:{index + 1}",
                    suspicion_score=80,
                    confidence="high",
                    proposed_action="search_replacement",
                    status="open",
                    ignored=False,
                    first_seen_at=base_time,
                    last_seen_at=base_time,
                    last_scanned_at=base_time,
                )
                session.add(finding)
                await session.flush()
                session.add(
                    RemediationJob(
                        finding_id=finding.id,
                        action_type="search_replacement",
                        status="queued",
                        attempt_count=0,
                        requested_by="test",
                        created_at=base_time + dt.timedelta(seconds=index),
                    )
                )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/jobs?page=16&page_size=20")

        assert response.status_code == 200
        assert "Showing 301-320 of 320 jobs" in response.text
        assert "Episode 019.mkv" in response.text
        assert "Episode 020.mkv" not in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(RemediationAttempt))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding).where(Finding.file_path.like("/mnt/RAYNAS/TV Shows/Long Queue/%")))
            await session.commit()

    asyncio.run(_cleanup())
