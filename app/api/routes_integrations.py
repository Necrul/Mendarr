from __future__ import annotations

from typing import Annotated
from pathlib import Path
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.dependencies import require_csrf, require_user
from app.domain.enums import IntegrationKind
from app.integrations.radarr_client import RadarrClient
from app.integrations.sonarr_client import SonarrClient
from app.persistence.db import get_db
from app.persistence.models import LibraryRoot
from app.security import generate_csrf_token, mask_api_key
from app.services.audit_service import log_event
from app.services.integration_service import get_integration, reveal_integration_api_key, upsert_integration
from app.services.root_discovery_service import (
    discover_root_candidates,
    discover_root_candidates_with_status,
    resolve_local_scan_path,
)
from app.web.templates import get_templates

router = APIRouter(tags=["integrations"])
templates = get_templates()


def _masked_api_key_placeholder(integration) -> str:
    if not integration:
        return ""
    try:
        return mask_api_key(reveal_integration_api_key(integration))
    except Exception:
        return ""


def _normalize_base_url(value: str) -> str:
    raw = value.strip().rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Base URL must be a valid http(s) URL")
    return raw


def _normalize_local_root_path(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise HTTPException(status_code=400, detail="Local root path is required")
    resolved, exists_locally = resolve_local_scan_path(raw)
    if not exists_locally:
        raise HTTPException(status_code=400, detail="Local root path must exist and be a directory")
    return resolved


@router.get("/integrations", response_class=HTMLResponse)
async def integrations_page(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    discover: str | None = None,
):
    s = get_settings()
    csrf = generate_csrf_token(s.secret_key)
    sonarr = await get_integration(session, IntegrationKind.SONARR)
    radarr = await get_integration(session, IntegrationKind.RADARR)
    r = await session.execute(select(LibraryRoot).order_by(LibraryRoot.id.desc()))
    roots = list(r.scalars().all())
    sonarr_candidates: list = []
    radarr_candidates: list = []
    discover_messages: list[str] = []
    if discover in {"sonarr", "all"}:
        sonarr_candidates, sonarr_error = await discover_root_candidates_with_status(sonarr, roots)
        if sonarr_error:
            discover_messages.append(f"Sonarr: {sonarr_error}")
    if discover in {"radarr", "all"}:
        radarr_candidates, radarr_error = await discover_root_candidates_with_status(radarr, roots)
        if radarr_error:
            discover_messages.append(f"Radarr: {radarr_error}")
    custom_scan_path = request.query_params.get("scan_path") or ""
    return templates.TemplateResponse(
        request,
        "integrations/index.html",
        {
            "request": request,
            "csrf_token": csrf,
            "sonarr": sonarr,
            "radarr": radarr,
            "sonarr_key_masked": _masked_api_key_placeholder(sonarr),
            "radarr_key_masked": _masked_api_key_placeholder(radarr),
            "roots": roots,
            "sonarr_candidates": sonarr_candidates,
            "radarr_candidates": radarr_candidates,
            "discover_messages": discover_messages,
            "discover": discover or "",
            "custom_scan_path": custom_scan_path,
        },
    )


@router.post("/integrations/sonarr")
async def save_sonarr(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    csrf_token: str = Form(...),
    base_url: str = Form(""),
    api_key: str = Form(""),
    enabled: str = Form("on"),
):
    require_csrf(request, csrf_token)
    try:
        normalized_base_url = _normalize_base_url(base_url)
    except HTTPException:
        return RedirectResponse("/integrations?sonarr_msg=url_invalid", status_code=302)
    await upsert_integration(
        session,
        kind=IntegrationKind.SONARR.value,
        name="Sonarr",
        base_url=normalized_base_url,
        api_key=api_key.strip(),
        enabled=enabled == "on",
    )
    await log_event(session, event_type="integration_saved", entity_type="sonarr", message="Sonarr settings saved", actor=user)
    return RedirectResponse("/integrations?sonarr_msg=saved", status_code=302)


@router.post("/integrations/radarr")
async def save_radarr(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    csrf_token: str = Form(...),
    base_url: str = Form(""),
    api_key: str = Form(""),
    enabled: str = Form("on"),
):
    require_csrf(request, csrf_token)
    try:
        normalized_base_url = _normalize_base_url(base_url)
    except HTTPException:
        return RedirectResponse("/integrations?radarr_msg=url_invalid", status_code=302)
    await upsert_integration(
        session,
        kind=IntegrationKind.RADARR.value,
        name="Radarr",
        base_url=normalized_base_url,
        api_key=api_key.strip(),
        enabled=enabled == "on",
    )
    await log_event(session, event_type="integration_saved", entity_type="radarr", message="Radarr settings saved", actor=user)
    return RedirectResponse("/integrations?radarr_msg=saved", status_code=302)


@router.post("/integrations/roots/add")
async def add_root(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    csrf_token: str = Form(...),
    manager_kind: str = Form(...),
    manager_root_path: str = Form(""),
    local_root_path: str = Form(...),
):
    require_csrf(request, csrf_token)
    try:
        normalized_local_root = _normalize_local_root_path(local_root_path)
    except HTTPException as exc:
        detail = str(exc.detail).lower()
        if "must exist and be a directory" in detail:
            return RedirectResponse("/integrations?root_msg=path_missing", status_code=302)
        if "local root path is required" in detail:
            return RedirectResponse("/integrations?root_msg=path_required", status_code=302)
        return RedirectResponse("/integrations?root_msg=path_invalid", status_code=302)

    existing = (
        await session.execute(
            select(LibraryRoot).where(
                LibraryRoot.local_root_path == normalized_local_root,
                LibraryRoot.enabled.is_(True),
            )
        )
    ).scalar_one_or_none()
    if existing:
        return RedirectResponse("/integrations?root_msg=exists", status_code=302)

    if manager_root_path.strip():
        existing_manager_root = (
            await session.execute(
                select(LibraryRoot).where(
                    LibraryRoot.manager_kind == manager_kind.lower(),
                    LibraryRoot.manager_root_path == manager_root_path.strip(),
                    LibraryRoot.enabled.is_(True),
                )
            )
        ).scalar_one_or_none()
        if existing_manager_root:
            return RedirectResponse("/integrations?root_msg=exists", status_code=302)

    session.add(
        LibraryRoot(
            manager_kind=manager_kind.lower(),
            manager_root_path=manager_root_path.strip(),
            local_root_path=normalized_local_root,
            enabled=True,
        )
    )
    await log_event(session, event_type="library_root_added", entity_type="library_root", message="Library added", actor=user)
    return RedirectResponse("/integrations?root_msg=added", status_code=302)


@router.post("/integrations/roots/{root_id}/delete")
async def delete_root(
    root_id: int,
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    csrf_token: str = Form(...),
):
    require_csrf(request, csrf_token)
    root = await session.get(LibraryRoot, root_id)
    if not root:
        return RedirectResponse("/integrations?root_msg=missing", status_code=302)
    await session.delete(root)
    await log_event(
        session,
        event_type="library_root_removed",
        entity_type="library_root",
        entity_id=str(root_id),
        message=f"Removed library {root.local_root_path}",
        actor=user,
    )
    return RedirectResponse("/integrations?root_msg=removed", status_code=302)


@router.post("/integrations/test/sonarr")
async def test_sonarr(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    csrf_token: str = Form(...),
):
    require_csrf(request, csrf_token)
    row = await get_integration(session, IntegrationKind.SONARR)
    if not row or not row.base_url:
        return RedirectResponse("/integrations?sonarr_msg=not_configured", status_code=302)
    c = SonarrClient(row.base_url, reveal_integration_api_key(row))
    ok, msg = await c.test()
    await log_event(
        session,
        event_type="integration_test",
        entity_type="sonarr",
        message=f"test {'ok' if ok else 'fail'}: {msg[:200]}",
        actor=user,
    )
    return RedirectResponse(f"/integrations?sonarr_test={'1' if ok else '0'}&discover=sonarr", status_code=302)


@router.post("/integrations/test/radarr")
async def test_radarr(
    request: Request,
    user: Annotated[str, Depends(require_user)],
    session: Annotated[AsyncSession, Depends(get_db)],
    csrf_token: str = Form(...),
):
    require_csrf(request, csrf_token)
    row = await get_integration(session, IntegrationKind.RADARR)
    if not row or not row.base_url:
        return RedirectResponse("/integrations?radarr_msg=not_configured", status_code=302)
    c = RadarrClient(row.base_url, reveal_integration_api_key(row))
    ok, msg = await c.test()
    await log_event(
        session,
        event_type="integration_test",
        entity_type="radarr",
        message=f"test {'ok' if ok else 'fail'}: {msg[:200]}",
        actor=user,
    )
    return RedirectResponse(f"/integrations?radarr_test={'1' if ok else '0'}&discover=radarr", status_code=302)
