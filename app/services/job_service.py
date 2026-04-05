from __future__ import annotations

import datetime as dt

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import JobStatus, RemediationAction
from app.persistence.models import RemediationJob
from app.services.audit_service import log_event


async def create_job(
    session: AsyncSession,
    *,
    finding_id: int,
    action: RemediationAction,
    requested_by: str,
    actor: str | None = None,
) -> RemediationJob:
    existing = await session.execute(
        select(RemediationJob)
        .where(
            RemediationJob.finding_id == finding_id,
            RemediationJob.action_type == action.value,
            RemediationJob.status.in_((JobStatus.QUEUED.value, JobStatus.RUNNING.value)),
        )
        .order_by(RemediationJob.id.desc())
        .limit(1)
    )
    row = existing.scalar_one_or_none()
    if row:
        return row

    job = RemediationJob(
        finding_id=finding_id,
        action_type=action.value,
        status=JobStatus.QUEUED.value,
        requested_by=requested_by,
        created_at=dt.datetime.now(dt.UTC),
    )
    try:
        async with session.begin_nested():
            session.add(job)
            await session.flush()
    except IntegrityError:
        existing = await session.execute(
            select(RemediationJob)
            .where(
                RemediationJob.finding_id == finding_id,
                RemediationJob.action_type == action.value,
                RemediationJob.status.in_((JobStatus.QUEUED.value, JobStatus.RUNNING.value)),
            )
            .order_by(RemediationJob.id.desc())
            .limit(1)
        )
        row = existing.scalar_one_or_none()
        if row:
            return row
        raise
    await log_event(
        session,
        event_type="job_queued",
        entity_type="remediation_job",
        message=f"Queued {action.value} for finding {finding_id}",
        entity_id=str(job.id),
        metadata={"finding_id": finding_id},
        actor=actor,
    )
    return job


async def list_jobs(session: AsyncSession, limit: int = 200) -> list[RemediationJob]:
    r = await session.execute(select(RemediationJob).order_by(RemediationJob.id.desc()).limit(limit))
    return list(r.scalars().all())


async def get_job(session: AsyncSession, job_id: int) -> RemediationJob | None:
    r = await session.execute(select(RemediationJob).where(RemediationJob.id == job_id))
    return r.scalar_one_or_none()
