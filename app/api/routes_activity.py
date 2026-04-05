from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.dependencies import require_user
from app.persistence.db import get_db
from app.persistence.models import AuditEvent, RemediationJob, ScanRun
from app.security import generate_csrf_token
from app.services.audit_service import recent_events
from app.web.pagination import build_pagination
from app.web.templates import (
    get_templates,
    humanize_action_label,
    humanize_attempt_label,
    humanize_failure_reason,
)
from app.web.job_presenter import remediation_result_label, remediation_result_message

router = APIRouter(tags=["activity"])
templates = get_templates()
DEFAULT_PAGE_SIZE = 10
ALLOWED_PAGE_SIZES = (10, 20, 50)


@router.get("/activity", response_class=HTMLResponse)
async def activity_page(
    request: Request,
    _user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
):
    page_size = page_size if page_size in ALLOWED_PAGE_SIZES else DEFAULT_PAGE_SIZE
    s = get_settings()
    csrf = generate_csrf_token(s.secret_key)
    recent_summary_events = await recent_events(session, 500)
    scan_count = await session.scalar(select(func.count()).select_from(ScanRun))
    job_count = await session.scalar(select(func.count()).select_from(RemediationJob))
    event_count = await session.scalar(select(func.count()).select_from(AuditEvent))
    queued_job_count = await session.scalar(
        select(func.count()).select_from(RemediationJob).where(RemediationJob.status == "queued")
    )
    accepted_job_count = await session.scalar(
        select(func.count()).select_from(RemediationJob).where(RemediationJob.status == "succeeded")
    )
    failure_events = [
        event
        for event in recent_summary_events
        if event.event_type in {"job_failed", "scan_failed"}
    ]
    error_count = await session.scalar(
        select(func.count()).select_from(AuditEvent).where(AuditEvent.event_type.in_(("job_failed", "scan_failed")))
    )
    failure_groups_map: dict[str, dict[str, object]] = {}
    for event in failure_events:
        label = humanize_failure_reason(event.message)
        current = failure_groups_map.get(label)
        if current is None:
            failure_groups_map[label] = {
                "label": label,
                "count": 1,
                "last_seen": event.created_at,
                "sample": event.message,
            }
        else:
            current["count"] = int(current["count"]) + 1
    failure_groups = sorted(
        failure_groups_map.values(),
        key=lambda item: (-int(item["count"]), str(item["label"])),
    )[:6]
    latest_scans = (
        (await session.execute(select(ScanRun).order_by(ScanRun.started_at.desc(), ScanRun.id.desc()).limit(8)))
        .scalars()
        .all()
    )
    current_scan = next((scan for scan in latest_scans if scan.status == "running"), None)
    job_map: dict[str, RemediationJob] = {}

    def describe_event(event):
        job = job_map.get(event.entity_id or "")
        command_label = None
        action_label = None
        finding_label = None
        is_job_event = event.event_type in {"job_queued", "job_succeeded", "job_failed"}
        if job and is_job_event:
            action_label = humanize_action_label(job.action_type)
            if job.attempts:
                latest_attempt = max(job.attempts, key=lambda attempt: attempt.id)
                if latest_attempt.step_name != "error":
                    command_label = humanize_attempt_label(latest_attempt.step_name)
            else:
                command_label = action_label
            if job.finding:
                finding_label = job.finding.file_name or job.finding.title or f"Finding #{job.finding_id}"
            else:
                finding_label = f"Finding #{job.finding_id}"

        command_or_action = command_label or action_label
        if event.event_type == "job_queued" and command_or_action:
            message = f"{command_or_action} queued for {finding_label}"
        elif event.event_type == "job_succeeded" and command_or_action:
            message = f"{remediation_result_label(job)} for {finding_label}"
            detail = remediation_result_message(job)
            if detail:
                message = f"{message}. {detail}"
        elif event.event_type == "job_failed" and command_or_action:
            message = f"{remediation_result_label(job)} for {finding_label}: {humanize_failure_reason(event.message)}"
        elif event.event_type in {"job_failed", "scan_failed"}:
            message = humanize_failure_reason(event.message)
        else:
            message = event.message

        return {
            "created_at": event.created_at,
            "event_type": event.event_type,
            "display_type": command_or_action or event.event_type,
            "entity_type": event.entity_type,
            "entity_id": event.entity_id,
            "actor": event.actor,
            "message": message,
            "command_label": command_label,
        }

    summary_cards = [
        {"label": "Scans", "value": scan_count or 0, "tone": ""},
        {"label": "Jobs", "value": job_count or 0, "tone": ""},
        {"label": "Failures", "value": error_count, "tone": "danger" if error_count else ""},
        {"label": "Queued", "value": queued_job_count or 0, "tone": ""},
        {"label": "Accepted", "value": accepted_job_count or 0, "tone": ""},
        {"label": "Needs mapping", "value": next((int(group["count"]) for group in failure_groups if group["label"] == "Manual review only"), 0), "tone": "warn"},
    ]
    pagination = build_pagination(
        base_path="/activity",
        page=page,
        page_size=page_size,
        total_items=event_count or 0,
        params={"page_size": str(page_size)},
    )
    page_number = int(pagination["page"])
    timeline_rows = (
        await session.execute(
            select(AuditEvent)
            .order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc())
            .offset((page_number - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    remediation_job_ids = sorted(
        {
            int(event.entity_id)
            for event in [*recent_summary_events, *timeline_rows]
            if event.entity_type == "remediation_job" and (event.entity_id or "").isdigit()
        }
    )
    if remediation_job_ids:
        job_rows = (
            await session.execute(
                select(RemediationJob)
                .options(selectinload(RemediationJob.finding), selectinload(RemediationJob.attempts))
                .where(RemediationJob.id.in_(remediation_job_ids))
            )
        ).scalars().unique().all()
        job_map = {str(job.id): job for job in job_rows}
    recent_changes = [
        describe_event(event)
        for event in recent_summary_events
        if event.event_type
        in {
            "library_root_added",
            "library_root_removed",
            "integration_saved",
            "integration_test",
            "job_failed",
            "job_queued",
            "job_succeeded",
            "scan_failed",
            "scan_started",
            "scan_completed",
        }
    ][:12]
    timeline_events = [describe_event(event) for event in timeline_rows]
    return templates.TemplateResponse(
        request,
        "activity/list.html",
        {
            "request": request,
            "csrf_token": csrf,
            "events": timeline_events,
            "summary_cards": summary_cards,
            "scan_count": scan_count or 0,
            "job_count": job_count or 0,
            "error_count": error_count,
            "latest_scans": latest_scans,
            "current_scan": current_scan,
            "recent_changes": recent_changes,
            "failure_groups": failure_groups,
            "pagination": pagination,
            "page_size": page_size,
            "page_size_options": ALLOWED_PAGE_SIZES,
        },
    )
