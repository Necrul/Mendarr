import asyncio

from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from app.main import app
from app.persistence.db import SessionLocal, init_db
from app.persistence.models import RuleException, RuleSettingsRow
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


def test_rules_save_shows_confirmation_message():
    original_values = {}

    async def _capture():
        await init_db()
        async with SessionLocal() as session:
            row = (await session.execute(select(RuleSettingsRow).limit(1))).scalar_one_or_none()
            if row is not None:
                original_values.update(
                    {
                        "min_tv_size_bytes": row.min_tv_size_bytes,
                        "min_movie_size_bytes": row.min_movie_size_bytes,
                        "min_duration_tv_seconds": row.min_duration_tv_seconds,
                        "min_duration_movie_seconds": row.min_duration_movie_seconds,
                        "excluded_keywords": row.excluded_keywords,
                        "extras_keywords": row.extras_keywords,
                        "excluded_paths": row.excluded_paths,
                        "ignored_patterns": row.ignored_patterns,
                        "auto_remediation_enabled": row.auto_remediation_enabled,
                    }
                )

    asyncio.run(_capture())

    with TestClient(app) as client:
        _login(client)
        page = client.get("/rules")
        csrf = extract_csrf_token(page.text)
        response = client.post(
            "/rules/save",
            data={
                "csrf_token": csrf,
                "min_tv_size_bytes": "12345",
                "min_movie_size_bytes": "67890",
                "min_duration_tv_seconds": "90",
                "min_duration_movie_seconds": "600",
                "excluded_keywords": "sample",
                "extras_keywords": "trailer",
                "excluded_paths": "",
                "ignored_patterns": "",
                "auto_remediation_enabled": "on",
            },
            follow_redirects=True,
        )

        assert response.status_code == 200
        assert "Rule settings saved." in response.text

    async def _restore():
        async with SessionLocal() as session:
            row = (await session.execute(select(RuleSettingsRow).limit(1))).scalar_one()
            for key, value in original_values.items():
                setattr(row, key, value)
            await session.execute(delete(RuleException).where(RuleException.note == "feedback-test"))
            await session.commit()

    asyncio.run(_restore())


def test_rule_exception_add_shows_confirmation_message():
    with TestClient(app) as client:
        _login(client)
        page = client.get("/rules")
        csrf = extract_csrf_token(page.text)
        response = client.post(
            "/rules/exceptions/add",
            data={
                "csrf_token": csrf,
                "path_pattern": "**/Extras/**",
                "title_pattern": "",
                "note": "feedback-test",
                "ignore_flag": "on",
            },
            follow_redirects=True,
        )

        assert response.status_code == 200
        assert "Exception added." in response.text

    async def _cleanup():
        async with SessionLocal() as session:
            await session.execute(delete(RuleException).where(RuleException.note == "feedback-test"))
            await session.commit()

    asyncio.run(_cleanup())
