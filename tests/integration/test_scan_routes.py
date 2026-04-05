import asyncio
import datetime as dt
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
from app.persistence.db import SessionLocal
from app.persistence.models import ScanRun
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


def test_scan_start_redirects_immediately(monkeypatch):
    async def fake_start_background_scan(actor=None, resume=False):
        assert resume is False
        return SimpleNamespace(id=42)

    monkeypatch.setattr("app.api.routes_scan.start_background_scan", fake_start_background_scan)

    with TestClient(app) as client:
        _login(client)
        dashboard = client.get("/")
        csrf = extract_csrf_token(dashboard.text)

        response = client.post("/scan/start", data={"csrf_token": csrf}, follow_redirects=False)

        assert response.status_code == 302
        assert response.headers["location"] == "/?scan=started&scan_id=42"


def test_scan_start_accepts_json_without_redirect(monkeypatch):
    async def fake_start_background_scan(actor=None, resume=False):
        assert resume is False
        return SimpleNamespace(id=77)

    monkeypatch.setattr("app.api.routes_scan.start_background_scan", fake_start_background_scan)

    with TestClient(app) as client:
        _login(client)
        dashboard = client.get("/")
        csrf = extract_csrf_token(dashboard.text)

        response = client.post(
            "/scan/start",
            data={"csrf_token": csrf},
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )

        assert response.status_code == 202
        assert response.json() == {"started": True, "resumed": False, "scan": {"id": 77}}


def test_scan_resume_redirects_immediately(monkeypatch):
    resume_target = Path(__file__).resolve()

    async def fake_latest_resumable_library_scan(session):
        del session
        return SimpleNamespace(id=12, notes=f'{{"resume_after_file":"{resume_target.as_posix()}"}}')

    async def fake_start_background_scan(actor=None, resume=False):
        assert resume is True
        return SimpleNamespace(id=42)

    monkeypatch.setattr("app.api.routes_scan.latest_resumable_library_scan", fake_latest_resumable_library_scan)
    monkeypatch.setattr("app.api.routes_scan.start_background_scan", fake_start_background_scan)

    with TestClient(app) as client:
        _login(client)
        dashboard = client.get("/")
        csrf = extract_csrf_token(dashboard.text)

        response = client.post("/scan/start", data={"csrf_token": csrf, "resume": "1"}, follow_redirects=False)

        assert response.status_code == 302
        assert response.headers["location"] == "/?scan=resumed&scan_id=42"


def test_scan_resume_returns_not_resumable(monkeypatch):
    async def fake_latest_resumable_library_scan(session):
        del session
        return None

    monkeypatch.setattr("app.api.routes_scan.latest_resumable_library_scan", fake_latest_resumable_library_scan)

    with TestClient(app) as client:
        _login(client)
        dashboard = client.get("/")
        csrf = extract_csrf_token(dashboard.text)

        response = client.post(
            "/scan/start",
            data={"csrf_token": csrf, "resume": "1"},
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )

        assert response.status_code == 409
        assert response.json() == {"started": False, "reason": "not_resumable"}


def test_scan_stop_redirects_immediately(monkeypatch):
    async def fake_request_scan_stop(actor=None):
        return 42

    monkeypatch.setattr("app.api.routes_scan.request_scan_stop", fake_request_scan_stop)

    with TestClient(app) as client:
        _login(client)
        dashboard = client.get("/")
        csrf = extract_csrf_token(dashboard.text)

        response = client.post("/scan/stop", data={"csrf_token": csrf}, follow_redirects=False)

        assert response.status_code == 302
        assert response.headers["location"] == "/?scan=stop_requested&scan_id=42"


def test_scan_stop_accepts_json_without_redirect(monkeypatch):
    async def fake_request_scan_stop(actor=None):
        return 77

    monkeypatch.setattr("app.api.routes_scan.request_scan_stop", fake_request_scan_stop)

    with TestClient(app) as client:
        _login(client)
        dashboard = client.get("/")
        csrf = extract_csrf_token(dashboard.text)

        response = client.post(
            "/scan/stop",
            data={"csrf_token": csrf},
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )

        assert response.status_code == 202
        assert response.json() == {"stopped": True, "scan": {"id": 77}}


def test_latest_scan_status_endpoint_returns_progress(monkeypatch):
    async def fake_recover_abandoned_scans(actor="system"):
        return 0

    monkeypatch.setattr("app.main.recover_abandoned_scans", fake_recover_abandoned_scans)

    async def _seed():
        async with SessionLocal() as session:
            session.add(
                ScanRun(
                    started_at=dt.datetime.now(dt.UTC),
                    status="running",
                    files_seen=12,
                    suspicious_found=3,
                    notes='{"total_files":40,"current_library":"/mnt/RAYNAS/TV Shows","current_file":"/mnt/RAYNAS/TV Shows/Season 01/Episode 01.mkv"}',
                )
            )
            await session.commit()

    asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/api/scans/latest")

        assert response.status_code == 200
        payload = response.json()["scan"]
        assert payload["status"] == "running"
        assert payload["progress_percent"] == 30
        assert payload["notes"]["current_file"].endswith("Episode 01.mkv")
