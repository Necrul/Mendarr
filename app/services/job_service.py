from __future__ import annotations

import datetime as dt

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import JobStatus, RemediationAction
from app.persistence.models import RemediationJob
from app.services.audit_service import log_event


async def claim_next_job(session: AsyncSession) -> int | None:
    """Caller must commit the claim before performing any external side effects."""
    next_id = (select(RemediationJob.id)
               .where(RemediationJob.status == JobStatus.QUEUED.value)
               .order_by(RemediationJob.id).limit(1).scalar_subquery())
    result = await session.execute(
        update(RemediationJob)
        .where(RemediationJob.id == next_id, RemediationJob.status == JobStatus.QUEUED.value)
        .values(status=JobStatus.RUNNING.value, started_at=dt.datetime.now(dt.UTC),
                attempt_count=RemediationJob.attempt_count + 1)
        .returning(RemediationJob.id)
    )
    return result.scalar_one_or_none()


async def recover_abandoned_jobs(session: AsyncSession, *, job_id: int | None = None) -> int:
    """An interrupted manager request has an unknown outcome and must not replay."""
    query = select(RemediationJob).where(RemediationJob.status == JobStatus.RUNNING.value)
    if job_id is not None:
        query = query.where(RemediationJob.id == job_id)
    jobs = (await session.execute(query)).scalars().all()
    for job in jobs:
        job.status = JobStatus.FAILED.value
        job.completed_at = dt.datetime.now(dt.UTC)
        job.last_error = "Mendarr was interrupted during this job. Manager outcome is unknown; verify the finding before retrying."
        await log_event(session, event_type="job_failed", entity_type="remediation_job",
                        entity_id=str(job.id), message=job.last_error, actor="system")
    return len(jobs)


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
