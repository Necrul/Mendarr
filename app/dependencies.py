from __future__ import annotations

from typing import Annotated

from fastapi import Cookie, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.persistence.db import get_db
from app.security import COOKIE_NAME, verify_csrf_token, verify_session_token


async def require_user(
    request: Request,
    _session: Annotated[AsyncSession, Depends(get_db)],
    m_session: str | None = Cookie(default=None, alias=COOKIE_NAME),
) -> str:
    if not m_session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    s = get_settings()
    payload = verify_session_token(m_session, s.secret_key)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired")
    return str(payload.get("u", "user"))


def require_csrf(request: Request, csrf_token: str | None) -> None:
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return
    s = get_settings()
    tok = csrf_token or request.headers.get("X-CSRF-Token")
    if not tok:
        raise HTTPException(status_code=400, detail="CSRF token missing")
    if not verify_csrf_token(tok, s.secret_key):
        raise HTTPException(status_code=403, detail="CSRF validation failed")
