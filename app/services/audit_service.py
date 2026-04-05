from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models import AuditEvent


async def log_event(
    session: AsyncSession,
    *,
    event_type: str,
    entity_type: str,
    message: str,
    entity_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    actor: str | None = None,
) -> AuditEvent:
    ev = AuditEvent(
        event_type=event_type,
        entity_type=entity_type,
        entity_id=entity_id,
        message=message,
        metadata_json=metadata,
        created_at=dt.datetime.now(dt.UTC),
        actor=actor,
    )
    session.add(ev)
    return ev


async def recent_events(session: AsyncSession, limit: int = 100) -> list[AuditEvent]:
    r = await session.execute(
        select(AuditEvent)
        .order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc())
        .limit(limit)
    )
    return list(r.scalars().all())
