from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crypto import decrypt_secret, encrypt_secret, is_encrypted_secret
from app.domain.enums import IntegrationKind
from app.persistence.models import IntegrationConfig


async def get_integration(session: AsyncSession, kind: IntegrationKind) -> IntegrationConfig | None:
    r = await session.execute(select(IntegrationConfig).where(IntegrationConfig.kind == kind.value).limit(1))
    return r.scalar_one_or_none()


def reveal_integration_api_key(row: IntegrationConfig | None) -> str:
    if not row or not row.api_key:
        return ""
    return decrypt_secret(row.api_key)


async def upsert_integration(
    session: AsyncSession,
    *,
    kind: str,
    name: str,
    base_url: str,
    api_key: str,
    enabled: bool = True,
) -> IntegrationConfig:
    r = await session.execute(select(IntegrationConfig).where(IntegrationConfig.kind == kind).limit(1))
    row = r.scalar_one_or_none()
    if row:
        row.name = name
        row.base_url = base_url
        if api_key.strip():
            row.api_key = encrypt_secret(api_key)
        row.enabled = enabled
        await session.flush()
        return row
    row = IntegrationConfig(
        kind=kind,
        name=name,
        base_url=base_url,
        api_key=encrypt_secret(api_key),
        enabled=enabled,
    )
    session.add(row)
    await session.flush()
    return row


async def migrate_legacy_integration_secrets(session: AsyncSession) -> int:
    rows = (await session.execute(select(IntegrationConfig).where(IntegrationConfig.api_key != ""))).scalars().all()
    migrated = 0
    for row in rows:
        if row.api_key and not is_encrypted_secret(row.api_key):
            row.api_key = encrypt_secret(row.api_key)
            migrated += 1
    if migrated:
        await session.flush()
    return migrated
