from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import selectinload
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import require_user
from app.config import get_settings
from app.persistence.db import get_db
from app.persistence.models import RemediationJob
from app.security import generate_csrf_token
from app.web.pagination import build_pagination
from app.web.templates import get_templates

router = APIRouter(tags=["jobs"])
templates = get_templates()
DEFAULT_PAGE_SIZE = 20
ALLOWED_PAGE_SIZES = (10, 20, 50)


@router.get("/jobs", response_class=HTMLResponse)
async def jobs_page(
    request: Request,
    _user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
):
    page_size = page_size if page_size in ALLOWED_PAGE_SIZES else DEFAULT_PAGE_SIZE
    s = get_settings()
    csrf = generate_csrf_token(s.secret_key)
    total_jobs = await session.scalar(select(func.count()).select_from(RemediationJob))
    status_counts = {
        "queued": await session.scalar(
            select(func.count()).select_from(RemediationJob).where(RemediationJob.status == "queued")
        ) or 0,
        "running": await session.scalar(
            select(func.count()).select_from(RemediationJob).where(RemediationJob.status == "running")
        ) or 0,
        "succeeded": await session.scalar(
            select(func.count()).select_from(RemediationJob).where(RemediationJob.status == "succeeded")
        ) or 0,
        "failed": await session.scalar(
            select(func.count()).select_from(RemediationJob).where(RemediationJob.status == "failed")
        ) or 0,
    }
    pagination = build_pagination(
        base_path="/jobs",
        page=page,
        page_size=page_size,
        total_items=total_jobs or 0,
        params={"page_size": str(page_size)},
    )
    page_number = int(pagination["page"])
    offset = (page_number - 1) * page_size
    r = await session.execute(
        select(RemediationJob)
        .options(selectinload(RemediationJob.finding), selectinload(RemediationJob.attempts))
        .order_by(RemediationJob.id.desc())
        .offset(offset)
        .limit(page_size)
    )
    jobs = list(r.scalars().unique().all())
    return templates.TemplateResponse(
        request,
        "jobs/list.html",
        {
            "request": request,
            "csrf_token": csrf,
            "jobs": jobs,
            "status_counts": status_counts,
            "pagination": pagination,
            "page_size": page_size,
            "page_size_options": ALLOWED_PAGE_SIZES,
        },
    )
