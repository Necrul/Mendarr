from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.dependencies import require_user
from app.domain.scan_notes import parse_scan_notes
from app.persistence.db import get_db
from app.persistence.models import Finding, IntegrationConfig, LibraryRoot, RemediationJob, ScanRun
from app.security import generate_csrf_token
from app.services.audit_service import recent_events
from app.services.update_service import get_update_status
from app.web.templates import get_templates

router = APIRouter(tags=["dashboard"])
templates = get_templates()


@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    _user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
):
    s = get_settings()
    csrf = generate_csrf_token(s.secret_key)
    total = await session.scalar(select(func.count()).select_from(Finding))
    unresolved = await session.scalar(
        select(func.count()).select_from(Finding).where(Finding.status.in_(("open", "unresolved")), Finding.ignored.is_(False))
    )
    high_conf = await session.scalar(
        select(func.count())
        .select_from(Finding)
        .where(Finding.confidence == "high", Finding.ignored.is_(False))
    )
    root_count = await session.scalar(
        select(func.count()).select_from(LibraryRoot).where(LibraryRoot.enabled.is_(True))
    )
    sonarr_ready = await session.scalar(
        select(func.count())
        .select_from(IntegrationConfig)
        .where(
            IntegrationConfig.kind == "sonarr",
            IntegrationConfig.enabled.is_(True),
            IntegrationConfig.base_url != "",
        )
    )
    radarr_ready = await session.scalar(
        select(func.count())
        .select_from(IntegrationConfig)
        .where(
            IntegrationConfig.kind == "radarr",
            IntegrationConfig.enabled.is_(True),
            IntegrationConfig.base_url != "",
        )
    )
    sonarr_count = await session.scalar(
        select(func.count()).select_from(Finding).where(Finding.manager_kind == "sonarr")
    )
    radarr_count = await session.scalar(
        select(func.count()).select_from(Finding).where(Finding.manager_kind == "radarr")
    )
    jobs = (
        (
            await session.execute(
                select(RemediationJob).order_by(RemediationJob.created_at.desc(), RemediationJob.id.desc()).limit(12)
            )
        )
        .scalars()
        .all()
    )
    open_findings = (
        (
            await session.execute(
                select(Finding)
                .where(Finding.status.in_(("open", "unresolved")), Finding.ignored.is_(False))
                .order_by(Finding.suspicion_score.desc(), Finding.last_seen_at.desc())
                .limit(8)
            )
        )
        .scalars()
        .all()
    )
    roots = (
        (await session.execute(select(LibraryRoot).where(LibraryRoot.enabled.is_(True)).order_by(LibraryRoot.id.desc()).limit(8)))
        .scalars()
        .all()
    )
    recent_scans = (
        (await session.execute(select(ScanRun).order_by(ScanRun.started_at.desc(), ScanRun.id.desc()).limit(6)))
        .scalars()
        .all()
    )
    latest_scan = next(
        (
            scan
            for scan in recent_scans
            if parse_scan_notes(scan.notes).get("scope") != "verify"
        ),
        recent_scans[0] if recent_scans else None,
    )
    latest_scan_notes = parse_scan_notes(latest_scan.notes) if latest_scan else {}
    latest_scan_scope = latest_scan_notes.get("scope") if latest_scan else None
    resume_scan = (
        latest_scan
        if latest_scan
        and latest_scan.status == "interrupted"
        and latest_scan_notes.get("resume_after_file")
        else None
    )
    events = await recent_events(session, 15)
    update_status = await get_update_status()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "csrf_token": csrf,
            "total_findings": total or 0,
            "unresolved": unresolved or 0,
            "high_conf": high_conf or 0,
            "root_count": root_count or 0,
            "sonarr_ready": bool(sonarr_ready),
            "radarr_ready": bool(radarr_ready),
            "sonarr_count": sonarr_count or 0,
            "radarr_count": radarr_count or 0,
            "roots": roots,
            "recent_jobs": jobs,
            "recent_scans": recent_scans,
            "recent_events": events,
            "latest_scan": latest_scan,
            "latest_scan_scope": latest_scan_scope,
            "resume_scan": resume_scan,
            "open_findings": open_findings,
            "update_status": update_status,
        },
    )
