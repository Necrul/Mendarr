from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import datetime as dt
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.dependencies import require_csrf, require_user
from sqlalchemy import select

from app.persistence.db import get_db
from app.persistence.models import RuleException
from app.security import generate_csrf_token
from app.services.audit_service import log_event
from app.services.rule_service import get_or_create_rule_settings
from app.web.templates import get_templates

router = APIRouter(tags=["rules"])
templates = get_templates()


@router.get("/rules", response_class=HTMLResponse)
async def rules_page(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
):
    s = get_settings()
    csrf = generate_csrf_token(s.secret_key)
    row = await get_or_create_rule_settings(session)
    ex = (
        (await session.execute(select(RuleException).order_by(RuleException.id.desc()).limit(100)))
        .scalars()
        .all()
    )
    return templates.TemplateResponse(
        request,
        "rules/edit.html",
        {"request": request, "csrf_token": csrf, "rules": row, "exceptions": ex},
    )


@router.post("/rules/save")
async def rules_save(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    csrf_token: str = Form(...),
    min_tv_size_bytes: int = Form(...),
    min_movie_size_bytes: int = Form(...),
    min_duration_tv_seconds: float = Form(...),
    min_duration_movie_seconds: float = Form(...),
    excluded_keywords: str = Form(""),
    extras_keywords: str = Form(""),
    excluded_paths: str = Form(""),
    ignored_patterns: str = Form(""),
    auto_remediation_enabled: str = Form("off"),
):
    require_csrf(request, csrf_token)
    row = await get_or_create_rule_settings(session)
    row.min_tv_size_bytes = min_tv_size_bytes
    row.min_movie_size_bytes = min_movie_size_bytes
    row.min_duration_tv_seconds = min_duration_tv_seconds
    row.min_duration_movie_seconds = min_duration_movie_seconds
    row.excluded_keywords = excluded_keywords
    row.extras_keywords = extras_keywords
    row.excluded_paths = excluded_paths
    row.ignored_patterns = ignored_patterns
    row.auto_remediation_enabled = auto_remediation_enabled == "on"
    row.updated_at = dt.datetime.now(dt.UTC)
    await log_event(session, event_type="rules_updated", entity_type="rules", message="Rule settings saved", actor=user)
    return RedirectResponse("/rules?rules_msg=saved", status_code=302)


@router.post("/rules/exceptions/add")
async def rules_exception_add(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    csrf_token: str = Form(...),
    path_pattern: str = Form(""),
    title_pattern: str = Form(""),
    note: str = Form(""),
    ignore_flag: str = Form("on"),
):
    require_csrf(request, csrf_token)
    session.add(
        RuleException(
            path_pattern=path_pattern.strip() or None,
            title_pattern=title_pattern.strip() or None,
            ignore_flag=ignore_flag == "on",
            enabled=True,
            note=note.strip() or None,
        )
    )
    await log_event(session, event_type="rule_exception_added", entity_type="rule_exception", message="Exception added", actor=user)
    return RedirectResponse("/rules?exception_msg=added", status_code=302)
