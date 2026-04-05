import tempfile
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from app.main import app
from app.persistence.db import SessionLocal
from app.persistence.models import IntegrationConfig, LibraryRoot
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


def test_save_sonarr_returns_fast_redirect_without_discovery():
    with TestClient(app) as client:
        _login(client)
        setup_page = client.get("/integrations")
        csrf = extract_csrf_token(setup_page.text)

        response = client.post(
            "/integrations/sonarr",
            data={
                "csrf_token": csrf,
                "base_url": "http://sonarr:8989",
                "api_key": "secret",
                "enabled": "on",
            },
            follow_redirects=False,
        )

        assert response.status_code == 302
        assert response.headers["location"] == "/integrations?sonarr_msg=saved"


def test_save_sonarr_invalid_url_redirects_with_feedback():
    with TestClient(app) as client:
        _login(client)
        setup_page = client.get("/integrations")
        csrf = extract_csrf_token(setup_page.text)

        response = client.post(
            "/integrations/sonarr",
            data={
                "csrf_token": csrf,
                "base_url": "sonarr.local:8989",
                "api_key": "secret",
                "enabled": "on",
            },
            follow_redirects=False,
        )

        assert response.status_code == 302
        assert response.headers["location"] == "/integrations?sonarr_msg=url_invalid"


def test_save_radarr_invalid_url_redirects_with_feedback():
    with TestClient(app) as client:
        _login(client)
        setup_page = client.get("/integrations")
        csrf = extract_csrf_token(setup_page.text)

        response = client.post(
            "/integrations/radarr",
            data={
                "csrf_token": csrf,
                "base_url": "radarr.local:7878",
                "api_key": "secret",
                "enabled": "on",
            },
            follow_redirects=False,
        )

        assert response.status_code == 302
        assert response.headers["location"] == "/integrations?radarr_msg=url_invalid"


def test_add_custom_library_does_not_require_manager_path():
    custom_root = Path(tempfile.mkdtemp(prefix="mendarr-custom-root-"))
    with TestClient(app) as client:
        _login(client)
        setup_page = client.get("/integrations")
        csrf = extract_csrf_token(setup_page.text)

        response = client.post(
            "/integrations/roots/add",
            data={
                "csrf_token": csrf,
                "manager_kind": "tv",
                "manager_root_path": "",
                "local_root_path": str(custom_root),
            },
            follow_redirects=False,
        )

        assert response.status_code == 302
        assert response.headers["location"] == "/integrations?root_msg=added"

    async def _assert_and_cleanup():
        async with SessionLocal() as session:
            row = (
                await session.execute(select(LibraryRoot).where(LibraryRoot.local_root_path == str(custom_root)))
            ).scalar_one()
            assert row.manager_root_path == ""
            await session.execute(delete(LibraryRoot).where(LibraryRoot.id == row.id))
            await session.commit()

    import asyncio

    asyncio.run(_assert_and_cleanup())


def test_remove_library_only_removes_it_from_mendarr():
    custom_root = Path(tempfile.mkdtemp(prefix="mendarr-remove-root-"))
    library_id = None

    async def _seed():
        async with SessionLocal() as session:
            row = LibraryRoot(
                manager_kind="tv",
                manager_root_path="",
                local_root_path=str(custom_root),
                enabled=True,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row.id

    import asyncio

    library_id = asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        setup_page = client.get("/integrations")
        csrf = extract_csrf_token(setup_page.text)

        response = client.post(
            f"/integrations/roots/{library_id}/delete",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )

        assert response.status_code == 302
        assert response.headers["location"] == "/integrations?root_msg=removed"

    async def _assert_removed():
        async with SessionLocal() as session:
            row = await session.get(LibraryRoot, library_id)
            assert row is None
            assert custom_root.exists()

    asyncio.run(_assert_removed())


def test_add_duplicate_library_returns_exists_redirect():
    custom_root = Path(tempfile.mkdtemp(prefix="mendarr-duplicate-root-"))

    with TestClient(app) as client:
        _login(client)
        setup_page = client.get("/integrations")
        csrf = extract_csrf_token(setup_page.text)

        first = client.post(
            "/integrations/roots/add",
            data={
                "csrf_token": csrf,
                "manager_kind": "tv",
                "manager_root_path": "",
                "local_root_path": str(custom_root),
            },
            follow_redirects=False,
        )
        assert first.status_code == 302
        assert first.headers["location"] == "/integrations?root_msg=added"

        setup_page = client.get("/integrations")
        csrf = extract_csrf_token(setup_page.text)
        duplicate = client.post(
            "/integrations/roots/add",
            data={
                "csrf_token": csrf,
                "manager_kind": "tv",
                "manager_root_path": "",
                "local_root_path": str(custom_root),
            },
            follow_redirects=False,
        )

        assert duplicate.status_code == 302
        assert duplicate.headers["location"] == "/integrations?root_msg=exists"


def test_integrations_page_uses_generic_custom_library_placeholder():
    with TestClient(app) as client:
        _login(client)
        response = client.get("/integrations")

        assert response.status_code == 200
        assert 'placeholder="/path/to/your/library"' in response.text
        assert 'value="/mnt/RAYNAS/TV Shows"' not in response.text


def test_integrations_page_never_renders_stored_api_key_plaintext():
    with TestClient(app) as client:
        _login(client)
        setup_page = client.get("/integrations")
        csrf = extract_csrf_token(setup_page.text)

        save = client.post(
            "/integrations/sonarr",
            data={
                "csrf_token": csrf,
                "base_url": "http://sonarr:8989",
                "api_key": "super-secret-sonarr-key",
                "enabled": "on",
            },
            follow_redirects=False,
        )
        assert save.status_code == 302

        response = client.get("/integrations")

        assert response.status_code == 200
        assert "super-secret-sonarr-key" not in response.text
        assert "********" in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(IntegrationConfig).where(IntegrationConfig.kind == "sonarr"))
            await session.commit()

    import asyncio

    asyncio.run(_cleanup())


def test_integrations_page_does_not_render_browse_section():
    with TestClient(app) as client:
        _login(client)
        response = client.get("/integrations")

        assert response.status_code == 200
        assert "Visible folders" not in response.text
        assert "Use current folder" not in response.text
        assert "Use this folder" not in response.text


def test_integrations_discovery_shows_message_when_connection_missing():
    with TestClient(app) as client:
        _login(client)
        response = client.get("/integrations?discover=sonarr")

        assert response.status_code == 200
        assert "Sonarr: Connection is not configured yet." in response.text
        assert "Nothing loaded from the selected app." in response.text


def test_integrations_discovery_surfaces_lookup_failure(monkeypatch):
    async def fake_get_integration(session, kind):
        del session
        if kind.value == "sonarr":
            return IntegrationConfig(
                kind="sonarr",
                name="Sonarr",
                base_url="http://sonarr:8989",
                api_key="enc:v1:test",
                enabled=True,
            )
        return None

    class _BrokenSonarrClient:
        def __init__(self, base_url: str, api_key: str):
            self.base_url = base_url
            self.api_key = api_key

        async def root_folders(self):
            raise RuntimeError("boom")

    monkeypatch.setattr("app.api.routes_integrations.get_integration", fake_get_integration)
    monkeypatch.setattr("app.services.root_discovery_service.reveal_integration_api_key", lambda row: "secret")
    monkeypatch.setattr("app.services.root_discovery_service.SonarrClient", _BrokenSonarrClient)

    with TestClient(app) as client:
        _login(client)
        response = client.get("/integrations?discover=sonarr")

        assert response.status_code == 200
        assert "Sonarr: Root folder lookup failed. Test the connection and check the app logs." in response.text


def test_integrations_discovery_shows_decryption_message_for_bad_stored_key(monkeypatch):
    async def fake_get_integration(session, kind):
        del session
        if kind.value == "sonarr":
            return IntegrationConfig(
                kind="sonarr",
                name="Sonarr",
                base_url="http://sonarr:8989",
                api_key="enc:v1:not-a-real-token",
                enabled=True,
            )
        return None

    monkeypatch.setattr("app.api.routes_integrations.get_integration", fake_get_integration)

    with TestClient(app) as client:
        _login(client)
        response = client.get("/integrations?discover=sonarr")

        assert response.status_code == 200
        assert "Sonarr: Stored API key could not be decrypted. Re-save the connection." in response.text


def test_integrations_page_uses_csp_safe_library_remove_confirmation():
    custom_root = Path(tempfile.mkdtemp(prefix="mendarr-confirm-root-"))

    async def _seed():
        async with SessionLocal() as session:
            row = LibraryRoot(
                manager_kind="tv",
                manager_root_path="",
                local_root_path=str(custom_root),
                enabled=True,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row.id

    import asyncio

    library_id = asyncio.run(_seed())

    with TestClient(app) as client:
        _login(client)
        response = client.get("/integrations")

        assert response.status_code == 200
        assert f'/integrations/roots/{library_id}/delete' in response.text
        assert 'data-confirm="Remove this library from Mendarr only?"' in response.text
        assert 'onclick=' not in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            row = await session.get(LibraryRoot, library_id)
            if row is not None:
                await session.delete(row)
                await session.commit()

    asyncio.run(_cleanup())
