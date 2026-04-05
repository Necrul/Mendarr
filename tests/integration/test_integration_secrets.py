import pytest
from sqlalchemy import select

from app.domain.enums import IntegrationKind
from app.persistence.db import SessionLocal, init_db
from app.persistence.models import IntegrationConfig
from app.services.integration_service import (
    get_integration,
    migrate_legacy_integration_secrets,
    reveal_integration_api_key,
    upsert_integration,
)


@pytest.mark.asyncio
async def test_integration_api_key_is_encrypted_at_rest():
    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            await upsert_integration(
                session,
                kind=IntegrationKind.SONARR.value,
                name="Sonarr",
                base_url="http://sonarr:8989",
                api_key="plain-api-key",
                enabled=True,
            )

    async with SessionLocal() as session:
        row = (await session.execute(select(IntegrationConfig).where(IntegrationConfig.kind == "sonarr"))).scalar_one()
        assert row.api_key != "plain-api-key"
        assert row.api_key.startswith("enc:v1:")
        assert reveal_integration_api_key(row) == "plain-api-key"


@pytest.mark.asyncio
async def test_plaintext_integration_api_key_is_migrated():
    await init_db()
    async with SessionLocal() as session:
        async with session.begin():
            session.add(
                IntegrationConfig(
                    kind="radarr",
                    name="Radarr",
                    base_url="http://radarr:7878",
                    api_key="legacy-plain-key",
                    enabled=True,
                )
            )

    async with SessionLocal() as session:
        async with session.begin():
            migrated = await migrate_legacy_integration_secrets(session)
            assert migrated >= 1

    async with SessionLocal() as session:
        row = await get_integration(session, IntegrationKind.RADARR)
        assert row is not None
        assert row.api_key != "legacy-plain-key"
        assert row.api_key.startswith("enc:v1:")
        assert reveal_integration_api_key(row) == "legacy-plain-key"
