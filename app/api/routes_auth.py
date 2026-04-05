from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.dependencies import require_csrf
from app.persistence.db import get_db
from app.rate_limit import limiter
from app.security import (
    clear_session_cookie,
    generate_csrf_token,
    is_request_secure,
    sanitize_next_url,
    set_session_cookie,
)
from app.services.user_service import verify_login
from app.web.templates import get_templates

router = APIRouter(tags=["auth"])
templates = get_templates()


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/"):
    s = get_settings()
    csrf = generate_csrf_token(s.secret_key)
    next_url = sanitize_next_url(next)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"request": request, "csrf_token": csrf, "next_url": next_url, "error": None},
    )


@router.post("/login", response_class=HTMLResponse)
@limiter.limit("120/minute")
async def login_submit(
    request: Request,
    session: AsyncSession = Depends(get_db),
    username: str = Form(...),
    password: str = Form(...),
    next_url: str = Form("/"),
    csrf_token: str = Form(...),
):
    require_csrf(request, csrf_token)
    s = get_settings()
    next_url = sanitize_next_url(next_url)
    ok = await verify_login(session, username, password)
    if not ok:
        csrf = generate_csrf_token(s.secret_key)
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "request": request,
                "csrf_token": csrf,
                "next_url": next_url,
                "error": "Invalid username or password",
            },
            status_code=401,
        )
    resp = RedirectResponse(url=next_url or "/", status_code=302)
    set_session_cookie(resp, username, secure=is_request_secure(request))
    return resp


@router.post("/logout")
@limiter.limit("30/minute")
async def logout(request: Request, csrf_token: str = Form(...)):
    require_csrf(request, csrf_token)
    resp = RedirectResponse(url="/login", status_code=302)
    clear_session_cookie(resp)
    return resp
