from fastapi.testclient import TestClient

from app.main import app
from tests.conftest import extract_csrf_token


def test_login_flow_and_dashboard_access():
    with TestClient(app) as client:
        redirect = client.get("/", follow_redirects=False)
        assert redirect.status_code == 302
        assert redirect.headers["location"].startswith("/login")

        login_page = client.get("/login")
        assert login_page.status_code == 200
        csrf = extract_csrf_token(login_page.text)

        login = client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "next_url": "/",
                "username": "admin",
                "password": "test-admin-password-secure-123",
            },
            follow_redirects=False,
        )
        assert login.status_code == 302
        assert login.headers["location"] == "/"

        dashboard = client.get("/")
        assert dashboard.status_code == 200
        assert "Dashboard" in dashboard.text
        assert 'href="https://buymeacoffee.com/necrul"' in dashboard.text


def test_login_rejects_external_next_url():
    with TestClient(app) as client:
        login_page = client.get("/login?next=https://evil.example")
        assert 'value="/"' in login_page.text

        csrf = extract_csrf_token(login_page.text)
        login = client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "next_url": "https://evil.example",
                "username": "admin",
                "password": "test-admin-password-secure-123",
            },
            follow_redirects=False,
        )
        assert login.status_code == 302
        assert login.headers["location"] == "/"


def test_https_login_sets_secure_cookie():
    with TestClient(app, base_url="https://testserver") as client:
        login_page = client.get("/login")
        csrf = extract_csrf_token(login_page.text)

        login = client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "next_url": "/",
                "username": "admin",
                "password": "test-admin-password-secure-123",
            },
            follow_redirects=False,
        )
        assert login.status_code == 302
        assert "Secure" in login.headers.get("set-cookie", "")


def test_login_page_sets_security_headers():
    with TestClient(app) as client:
        response = client.get("/login")

        assert response.status_code == 200
        assert "script-src 'self'" in response.headers.get("content-security-policy", "")
        assert response.headers.get("x-content-type-options") == "nosniff"
        assert response.headers.get("x-frame-options") == "DENY"


def test_http_login_does_not_trust_forwarded_proto_by_default():
    with TestClient(app) as client:
        login_page = client.get("/login")
        csrf = extract_csrf_token(login_page.text)

        login = client.post(
            "/login",
            data={
                "csrf_token": csrf,
                "next_url": "/",
                "username": "admin",
                "password": "test-admin-password-secure-123",
            },
            headers={"X-Forwarded-Proto": "https"},
            follow_redirects=False,
        )

        assert login.status_code == 302
        assert "Secure" not in login.headers.get("set-cookie", "")


def test_sensitive_post_route_requires_authentication():
    with TestClient(app) as client:
        response = client.post(
            "/integrations/sonarr",
            data={
                "csrf_token": "missing",
                "base_url": "http://sonarr:8989",
                "api_key": "secret",
                "enabled": "on",
            },
            follow_redirects=False,
        )

        assert response.status_code == 302
        assert response.headers["location"].startswith("/login")


def test_sensitive_api_route_requires_authentication():
    with TestClient(app) as client:
        response = client.get("/api/scans/latest", follow_redirects=False)

        assert response.status_code == 401
        assert response.json() == {"detail": "Unauthorized"}
