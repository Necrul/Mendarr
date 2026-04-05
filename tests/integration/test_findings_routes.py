import asyncio
import datetime as dt
import re
from urllib.parse import parse_qs, urlsplit

from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.main import app
from app.persistence.db import SessionLocal, init_db
from app.persistence.models import Finding, FindingReason, IntegrationConfig, RemediationJob
from tests.conftest import extract_csrf_token

asyncio.run(init_db())


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


def test_findings_list_paginates_and_shows_review_action():
    async def _seed():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            for index in range(30):
                timestamp = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=index)
                session.add(
                    Finding(
                        file_path=f"/mnt/RAYNAS/TV Shows/Show {index:02d}/Episode {index:02d}.mkv",
                        file_name=f"Episode {index:02d}.mkv",
                        media_kind="tv",
                        manager_kind="sonarr" if index % 2 == 0 else None,
                        suspicion_score=60 + index,
                        confidence="high",
                        proposed_action="review",
                        status="open",
                        ignored=False,
                        first_seen_at=timestamp,
                        last_seen_at=timestamp,
                        last_scanned_at=timestamp,
                    )
                )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/findings?page=2")

        assert response.status_code == 200
        assert "Episode 04.mkv" in response.text
        assert "Episode 29.mkv" not in response.text
        assert "Review" in response.text
        assert "Showing 26-30 of 30 findings" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(
                delete(Finding).where(Finding.file_path.like("/mnt/RAYNAS/TV Shows/Show %"))
            )
            await session.commit()

    asyncio.run(_cleanup())


def test_findings_list_paginates_past_previous_hard_cap():
    async def _seed():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            for index in range(520):
                timestamp = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=index)
                session.add(
                    Finding(
                        file_path=f"/mnt/RAYNAS/TV Shows/Long Run/Season 01/Episode {index:03d}.mkv",
                        file_name=f"Episode {index:03d}.mkv",
                        media_kind="tv",
                        manager_kind="sonarr",
                        suspicion_score=70,
                        confidence="high",
                        proposed_action="review",
                        status="open",
                        ignored=False,
                        first_seen_at=timestamp,
                        last_seen_at=timestamp,
                        last_scanned_at=timestamp,
                    )
                )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/findings?page=21&page_size=25")

        assert response.status_code == 200
        assert "Showing 501-520 of 520 findings" in response.text
        assert "Episode 019.mkv" in response.text
        assert "Episode 020.mkv" not in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding).where(Finding.file_path.like("/mnt/RAYNAS/TV Shows/Long Run/%")))
            await session.commit()

    asyncio.run(_cleanup())


def test_findings_list_filters_by_reason():
    async def _seed():
        async with SessionLocal() as session:
            first = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Broken Show/Episode 01.mkv",
                file_name="Episode 01.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                suspicion_score=92,
                confidence="high",
                proposed_action="review",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            second = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Short Show/Episode 02.mkv",
                file_name="Episode 02.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                suspicion_score=58,
                confidence="medium",
                proposed_action="review",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add_all([first, second])
            await session.flush()
            session.add_all(
                [
                    FindingReason(
                        finding_id=first.id,
                        code="MD_PROBE_FAILED",
                        message="ffprobe failed",
                        severity="critical",
                    ),
                    FindingReason(
                        finding_id=second.id,
                        code="MD_SHORT_DURATION",
                        message="Too short",
                        severity="warn",
                    ),
                ]
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/findings?reason=MD_PROBE_FAILED")

        assert response.status_code == 200
        assert "Episode 01.mkv" in response.text
        assert "Episode 02.mkv" not in response.text
        assert "Probe failed" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(
                delete(Finding).where(Finding.file_path.like("/mnt/RAYNAS/TV Shows/%"))
            )
            await session.commit()

    asyncio.run(_cleanup())


def test_findings_list_uses_csp_safe_bulk_selection_markup():
    async def _seed():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            session.add(
                Finding(
                    file_path="/mnt/RAYNAS/TV Shows/Broken Show/Episode 01.mkv",
                    file_name="Episode 01.mkv",
                    media_kind="tv",
                    manager_kind="sonarr",
                    suspicion_score=92,
                    confidence="high",
                    proposed_action="review",
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
        response = client.get("/findings")

        assert response.status_code == 200
        assert 'data-findings-select-all' in response.text
        assert 'data-finding-select' in response.text
        assert 'data-navigate-on-change' in response.text
        assert 'onclick=' not in response.text
        assert 'onchange=' not in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_findings_list_shows_bulk_delete_replace_action():
    async def _seed():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            session.add(
                Finding(
                    file_path="/mnt/RAYNAS/TV Shows/Broken Show/Episode 01.mkv",
                    file_name="Episode 01.mkv",
                    media_kind="tv",
                    manager_kind="sonarr",
                    manager_entity_id="episode:123",
                    suspicion_score=92,
                    confidence="high",
                    proposed_action="search_replacement",
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
        response = client.get("/findings")

        assert response.status_code == 200
        assert 'option value="delete_search">Delete and replace</option>' in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_findings_list_counts_manager_none_as_unmanaged():
    async def _seed():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            session.add(
                Finding(
                    file_path="/mnt/RAYNAS/TV Shows/Unmanaged Show/Season 01/Episode 01.mkv",
                    file_name="Episode 01.mkv",
                    media_kind="tv",
                    manager_kind="none",
                    suspicion_score=58,
                    confidence="medium",
                    proposed_action="review",
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
        response = client.get("/findings")

        assert response.status_code == 200
        assert re.search(r'<div class="stat-label">Unmanaged</div>\s*<div class="stat-value">1</div>', response.text)

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_finding_detail_allows_queue_for_unmanaged_tv_when_sonarr_is_configured(monkeypatch):
    finding_id_holder = {}

    async def _seed():
        async with SessionLocal() as session:
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Broken Show/Season 01/Broken.Show.S01E01.mkv",
                file_name="Broken.Show.S01E01.mkv",
                media_kind="tv",
                manager_kind=None,
                manager_entity_id=None,
                suspicion_score=88,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add(finding)
            await session.flush()
            finding_id_holder["id"] = finding.id
            await session.commit()

    asyncio.run(_seed())

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return IntegrationConfig(
                kind="sonarr",
                name="Sonarr",
                base_url="http://sonarr:8989",
                api_key="enc:v1:test",
                enabled=True,
            )
        return None

    monkeypatch.setattr("app.api.routes_findings.get_integration", fake_get_integration)

    with TestClient(app) as client:
        _login(client)
        response = client.get(f"/findings/{finding_id_holder['id']}")
        assert response.status_code == 200
        assert "Repair can be queued. Mendarr will try to match this TV file in Sonarr first" in response.text
        assert "Search replacement" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_finding_remediate_queues_for_unmanaged_tv_when_sonarr_is_configured(monkeypatch):
    finding_id_holder = {}

    async def _seed():
        async with SessionLocal() as session:
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Broken Show/Season 01/Broken.Show.S01E02.mkv",
                file_name="Broken.Show.S01E02.mkv",
                media_kind="tv",
                manager_kind=None,
                manager_entity_id=None,
                suspicion_score=88,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add(finding)
            await session.flush()
            finding_id_holder["id"] = finding.id
            await session.commit()

    asyncio.run(_seed())

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return IntegrationConfig(
                kind="sonarr",
                name="Sonarr",
                base_url="http://sonarr:8989",
                api_key="enc:v1:test",
                enabled=True,
            )
        return None

    queued = {"called": False}

    async def fake_create_job(session, *, finding_id, action, requested_by, actor=None):
        queued["called"] = True
        return None

    monkeypatch.setattr("app.api.routes_findings.get_integration", fake_get_integration)
    monkeypatch.setattr("app.api.routes_findings.create_job", fake_create_job)

    with TestClient(app) as client:
        _login(client)
        detail = client.get(f"/findings/{finding_id_holder['id']}")
        csrf = extract_csrf_token(detail.text)
        response = client.post(
            f"/findings/{finding_id_holder['id']}/remediate",
            data={"csrf_token": csrf, "mode": "search"},
            follow_redirects=False,
        )

        assert response.status_code == 302
        assert response.headers["location"].endswith("?job_msg=queued")
        assert queued["called"] is True

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_finding_detail_uses_targeted_verify_scan():
    finding_id_holder = {}

    async def _seed():
        async with SessionLocal() as session:
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Broken Show/Season 01/Broken.Show.S01E03.mkv",
                file_name="Broken.Show.S01E03.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:99",
                suspicion_score=88,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add(finding)
            await session.flush()
            finding_id_holder["id"] = finding.id
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get(f"/findings/{finding_id_holder['id']}")
        assert response.status_code == 200
        assert 'action="/scan/verify"' in response.text
        assert 'name="finding_ids" value="' in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_finding_detail_shows_sonarr_manager_links(monkeypatch):
    finding_id_holder = {}

    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Wolfblood/Season 04/Wolfblood - S04E01 - Captivity.mkv",
                file_name="Wolfblood - S04E01 - Captivity.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:135475",
                suspicion_score=88,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add(finding)
            await session.flush()
            finding_id_holder["id"] = finding.id
            await session.commit()

    asyncio.run(_seed())

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return IntegrationConfig(
                kind="sonarr",
                name="Sonarr",
                base_url="http://sonarr:8989",
                api_key="enc:v1:test",
                enabled=True,
            )
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "seriesId": 262554}

        async def get_series(self, series_id: int):
            return {"id": series_id, "titleSlug": "wolfblood"}

    monkeypatch.setattr("app.api.routes_findings.get_integration", fake_get_integration)
    monkeypatch.setattr("app.api.routes_findings.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.api.routes_findings.SonarrClient", _StubSonarrClient)

    with TestClient(app) as client:
        _login(client)
        response = client.get(f"/findings/{finding_id_holder['id']}")
        assert response.status_code == 200
        assert 'href="http://sonarr:8989/series/wolfblood"' in response.text
        assert 'href="http://sonarr:8989/wanted/cutoffunmet"' in response.text
        assert "Interactive Search on the episode row" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_finding_detail_shows_delete_replace_when_supported(monkeypatch):
    finding_id_holder = {}

    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Wolfblood/Season 04/Wolfblood - S04E01 - Captivity.mkv",
                file_name="Wolfblood - S04E01 - Captivity.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:135475",
                suspicion_score=88,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add(finding)
            await session.flush()
            finding_id_holder["id"] = finding.id
            await session.commit()

    asyncio.run(_seed())

    async def fake_get_integration(session, kind):
        if kind.value == "sonarr":
            return IntegrationConfig(
                kind="sonarr",
                name="Sonarr",
                base_url="http://sonarr:8989",
                api_key="enc:v1:test",
                enabled=True,
            )
        return None

    def fake_reveal_api_key(row):
        return "secret"

    class _StubSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def get_episode_by_id(self, episode_id: int):
            return {"id": episode_id, "seriesId": 262554}

        async def get_series(self, series_id: int):
            return {"id": series_id, "titleSlug": "wolfblood"}

    monkeypatch.setattr("app.api.routes_findings.get_integration", fake_get_integration)
    monkeypatch.setattr("app.api.routes_findings.reveal_integration_api_key", fake_reveal_api_key)
    monkeypatch.setattr("app.api.routes_findings.SonarrClient", _StubSonarrClient)
    with TestClient(app) as client:
        _login(client)
        response = client.get(f"/findings/{finding_id_holder['id']}")
        assert response.status_code == 200
        assert "Delete and replace" in response.text
        assert "mounted read-only" in response.text
        assert 'data-confirm=' in response.text
        assert 'onsubmit=' not in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_findings_list_hides_resolved_items_by_default():
    async def _seed():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            session.add_all(
                [
                    Finding(
                        file_path="/mnt/RAYNAS/TV Shows/Active Show/Episode 01.mkv",
                        file_name="Episode 01.mkv",
                        media_kind="tv",
                        manager_kind="sonarr",
                        suspicion_score=88,
                        confidence="high",
                        proposed_action="review",
                        status="open",
                        ignored=False,
                        first_seen_at=dt.datetime.now(dt.UTC),
                        last_seen_at=dt.datetime.now(dt.UTC),
                        last_scanned_at=dt.datetime.now(dt.UTC),
                    ),
                    Finding(
                        file_path="/mnt/RAYNAS/TV Shows/Resolved Show/Episode 02.mkv",
                        file_name="Episode 02.mkv",
                        media_kind="tv",
                        manager_kind="sonarr",
                        suspicion_score=5,
                        confidence="low",
                        proposed_action="review",
                        status="resolved",
                        ignored=False,
                        first_seen_at=dt.datetime.now(dt.UTC),
                        last_seen_at=dt.datetime.now(dt.UTC),
                        last_scanned_at=dt.datetime.now(dt.UTC),
                    ),
                ]
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/findings")

        assert response.status_code == 200
        assert "Active Show" in response.text
        assert "Resolved Show" not in response.text

        resolved = client.get("/findings?state=resolved")
        assert resolved.status_code == 200
        assert "Resolved Show" in resolved.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_findings_bulk_redirect_preserves_filters():
    finding_id_holder = {}

    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Filter Show/Episode 01.mkv",
                file_name="Episode 01.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                suspicion_score=88,
                confidence="high",
                proposed_action="review",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add(finding)
            await session.flush()
            session.add(
                FindingReason(
                    finding_id=finding.id,
                    code="MD_PROBE_FAILED",
                    message="ffprobe failed",
                    severity="critical",
                )
            )
            finding_id_holder["id"] = finding.id
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        page = client.get("/findings?manager=sonarr&reason=MD_PROBE_FAILED&state=review&page=2&page_size=10&high_only=1")
        csrf = extract_csrf_token(page.text)
        response = client.post(
            "/findings/bulk",
            data={
                "csrf_token": csrf,
                "action": "review",
                "finding_ids": str(finding_id_holder["id"]),
                "manager": "sonarr",
                "filter_action": "",
                "reason": "MD_PROBE_FAILED",
                "state": "review",
                "page": "2",
                "page_size": "10",
                "high_only": "1",
            },
            follow_redirects=False,
        )

        assert response.status_code == 302
        location = response.headers["location"]
        assert urlsplit(location).path == "/findings"
        assert parse_qs(urlsplit(location).query) == {
            "manager": ["sonarr"],
            "reason": ["MD_PROBE_FAILED"],
            "state": ["review"],
            "page_size": ["10"],
            "page": ["2"],
            "high_only": ["1"],
            "reviewed": ["1"],
        }

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_findings_bulk_requires_selection_with_user_facing_message():
    with TestClient(app) as client:
        _login(client)
        page = client.get("/findings?manager=sonarr&page=2&page_size=10")
        csrf = extract_csrf_token(page.text)
        response = client.post(
            "/findings/bulk",
            data={
                "csrf_token": csrf,
                "action": "ignore",
                "manager": "sonarr",
                "page": "2",
                "page_size": "10",
            },
            follow_redirects=False,
        )

        assert response.status_code == 302
        location = response.headers["location"]
        assert urlsplit(location).path == "/findings"
        assert parse_qs(urlsplit(location).query) == {
            "manager": ["sonarr"],
            "page_size": ["10"],
            "page": ["2"],
            "selection_msg": ["missing"],
        }


def test_findings_detail_actions_show_status_feedback():
    finding_id_holder = {}

    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Status Show/Episode 01.mkv",
                file_name="Episode 01.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                suspicion_score=88,
                confidence="high",
                proposed_action="review",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add(finding)
            await session.flush()
            finding_id_holder["id"] = finding.id
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        detail = client.get(f"/findings/{finding_id_holder['id']}")
        csrf = extract_csrf_token(detail.text)

        ignored = client.post(
            f"/findings/{finding_id_holder['id']}/ignore",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        assert ignored.status_code == 302
        assert ignored.headers["location"].endswith("?status_msg=ignored")

        response = client.get(ignored.headers["location"])
        assert "Finding ignored." in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_finding_detail_missing_redirects_with_feedback():
    with TestClient(app) as client:
        _login(client)
        response = client.get("/findings/999999", follow_redirects=False)

        assert response.status_code == 302
        assert response.headers["location"] == "/findings?finding_msg=missing"


def test_findings_awaiting_filter_clears_after_verify_scan_updates_timestamp():
    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            verified = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Verified Show/Episode 01.mkv",
                file_name="Episode 01.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                suspicion_score=88,
                confidence="high",
                proposed_action="review",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            pending = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Pending Show/Episode 02.mkv",
                file_name="Episode 02.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                suspicion_score=88,
                confidence="high",
                proposed_action="review",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC) - dt.timedelta(minutes=10),
            )
            session.add_all([verified, pending])
            await session.flush()
            completed_at = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5)
            session.add_all(
                [
                    RemediationJob(
                        finding_id=verified.id,
                        action_type="rescan_only",
                        status="succeeded",
                        requested_by="test",
                        started_at=completed_at - dt.timedelta(seconds=30),
                        completed_at=completed_at,
                    ),
                    RemediationJob(
                        finding_id=pending.id,
                        action_type="rescan_only",
                        status="succeeded",
                        requested_by="test",
                        started_at=completed_at - dt.timedelta(seconds=30),
                        completed_at=completed_at,
                    ),
                ]
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/findings?state=awaiting")

        assert response.status_code == 200
        assert "Pending Show" in response.text
        assert "Verified Show" not in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())


def test_finding_remediate_accepts_delete_replace_when_supported():
    finding_id_holder = {}

    async def _seed():
        await init_db()
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            finding = Finding(
                file_path="/mnt/RAYNAS/TV Shows/Wolfblood/Season 04/Wolfblood - S04E01 - Captivity.mkv",
                file_name="Wolfblood - S04E01 - Captivity.mkv",
                media_kind="tv",
                manager_kind="sonarr",
                manager_entity_id="episode:135475",
                suspicion_score=88,
                confidence="high",
                proposed_action="search_replacement",
                status="open",
                ignored=False,
                first_seen_at=dt.datetime.now(dt.UTC),
                last_seen_at=dt.datetime.now(dt.UTC),
                last_scanned_at=dt.datetime.now(dt.UTC),
            )
            session.add(finding)
            await session.flush()
            finding_id_holder["id"] = finding.id
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        detail = client.get(f"/findings/{finding_id_holder['id']}")
        csrf = extract_csrf_token(detail.text)
        response = client.post(
            f"/findings/{finding_id_holder['id']}/remediate",
            data={"csrf_token": csrf, "mode": "delete_search"},
            follow_redirects=False,
        )

        assert response.status_code == 302
        assert response.headers["location"].endswith("?job_msg=queued")

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(FindingReason))
            await session.execute(delete(RemediationJob))
            await session.execute(delete(Finding))
            await session.commit()

    asyncio.run(_cleanup())
