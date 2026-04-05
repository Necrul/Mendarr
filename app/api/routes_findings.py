from __future__ import annotations

from collections import Counter
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.dependencies import require_csrf, require_user
from app.domain.enums import IntegrationKind
from app.domain.enums import FindingStatus
from app.domain.enums import ManagerKind
from app.domain.enums import RemediationAction
from app.domain.finding_state import derive_finding_state
from app.domain.value_objects import CTX_TRAILER_IN_MAIN, KEYWORD_SAMPLE_TRAILER
from app.integrations.radarr_client import RadarrClient
from app.integrations.sonarr_client import SonarrClient
from app.persistence.db import get_db
from app.persistence.models import Finding, FindingReason, RemediationJob
from app.security import generate_csrf_token
from app.rate_limit import limiter
from app.services.audit_service import log_event
from app.services.integration_service import get_integration
from app.services.integration_service import reveal_integration_api_key
from app.services.job_service import create_job
from app.services.match_service import parse_sonarr_entity_id
from app.services.scan_service import start_verify_scan
from app.web.pagination import build_pagination
from app.web.templates import get_templates

router = APIRouter(tags=["findings"])
templates = get_templates()
DEFAULT_PAGE_SIZE = 25
ALLOWED_PAGE_SIZES = (10, 25, 50)
HIDDEN_REASON_CODES = {KEYWORD_SAMPLE_TRAILER, CTX_TRAILER_IN_MAIN}
_OPEN_FINDING_STATUSES = (FindingStatus.OPEN.value, FindingStatus.UNRESOLVED.value)


def _is_unmanaged(finding: Finding) -> bool:
    return (finding.manager_kind or ManagerKind.NONE.value) == ManagerKind.NONE.value


def _supports_manager_remediation(finding: Finding) -> bool:
    return bool(
        finding.manager_kind in {"sonarr", "radarr"}
        and finding.manager_entity_id
    )


async def _can_queue_remediation(session: AsyncSession, finding: Finding) -> bool:
    if _supports_manager_remediation(finding):
        return True
    if finding.media_kind == "tv":
        integration = await get_integration(session, IntegrationKind.SONARR)
        return bool(integration and integration.enabled and integration.base_url)
    if finding.media_kind == "movie":
        integration = await get_integration(session, IntegrationKind.RADARR)
        return bool(integration and integration.enabled and integration.base_url)
    return False


def _supports_delete_replace(finding: Finding) -> bool:
    if finding.manager_kind == ManagerKind.SONARR.value:
        entity_kind, entity_value = parse_sonarr_entity_id(finding.manager_entity_id)
        return entity_kind == "episode" and entity_value is not None
    if finding.manager_kind == ManagerKind.RADARR.value:
        return bool(finding.manager_entity_id and str(finding.manager_entity_id).isdigit())
    return False


def _derived_finding_state(finding: Finding) -> str:
    return derive_finding_state(finding)


def _latest_job_state_expressions():
    latest_job_id = (
        select(func.max(RemediationJob.id))
        .select_from(RemediationJob)
        .where(RemediationJob.finding_id == Finding.id)
        .correlate(Finding)
        .scalar_subquery()
    )
    latest_job_status = (
        select(RemediationJob.status)
        .select_from(RemediationJob)
        .where(RemediationJob.id == latest_job_id)
        .correlate(Finding)
        .scalar_subquery()
    )
    latest_job_started = (
        select(RemediationJob.started_at)
        .select_from(RemediationJob)
        .where(RemediationJob.id == latest_job_id)
        .correlate(Finding)
        .scalar_subquery()
    )
    latest_job_completed = (
        select(RemediationJob.completed_at)
        .select_from(RemediationJob)
        .where(RemediationJob.id == latest_job_id)
        .correlate(Finding)
        .scalar_subquery()
    )
    repaired_at = func.coalesce(latest_job_completed, latest_job_started)
    verified_after_repair = and_(
        Finding.last_scanned_at.is_not(None),
        repaired_at.is_not(None),
        Finding.last_scanned_at > repaired_at,
    )
    pending_verify = and_(
        Finding.status.in_(_OPEN_FINDING_STATUSES),
        latest_job_status == "succeeded",
        or_(
            Finding.last_scanned_at.is_(None),
            repaired_at.is_(None),
            Finding.last_scanned_at <= repaired_at,
        ),
    )
    needs_review = and_(
        Finding.status.in_(_OPEN_FINDING_STATUSES),
        or_(
            latest_job_status.is_(None),
            and_(
                latest_job_status != "queued",
                latest_job_status != "running",
                latest_job_status != "failed",
                latest_job_status != "succeeded",
            ),
            and_(latest_job_status == "succeeded", verified_after_repair),
        ),
    )
    return {
        "latest_job_status": latest_job_status,
        "pending_verify": pending_verify,
        "needs_review": needs_review,
        "failed": and_(
            Finding.status.in_(_OPEN_FINDING_STATUSES),
            latest_job_status == "failed",
        ),
        "queued_or_running": and_(
            Finding.status.in_(_OPEN_FINDING_STATUSES),
            latest_job_status.in_(("queued", "running")),
        ),
    }


def _append_query_params(target: str, params: dict[str, str]) -> str:
    parsed = urlsplit(target)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({key: value for key, value in params.items() if value not in ("", None)})
    filtered = {key: value for key, value in query.items() if value not in ("", None)}
    query_text = urlencode(filtered)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query_text, parsed.fragment))


def _findings_redirect_params(
    *,
    manager: str = "",
    media: str = "",
    filter_action: str = "",
    unresolved_only: str = "",
    ignored_only: str = "",
    high_only: str = "",
    reason: str = "",
    state: str = "",
    page_size: str = str(DEFAULT_PAGE_SIZE),
    page: str = "1",
) -> dict[str, str]:
    return {
        "manager": manager,
        "media": media,
        "action": filter_action,
        "unresolved_only": unresolved_only,
        "ignored_only": ignored_only,
        "high_only": high_only,
        "reason": reason,
        "state": state,
        "page_size": page_size,
        "page": page,
    }


async def _manager_links(session: AsyncSession, finding: Finding) -> dict[str, str]:
    if finding.manager_kind == ManagerKind.SONARR.value:
        integration = await get_integration(session, IntegrationKind.SONARR)
        api_key = reveal_integration_api_key(integration)
        if not integration or not integration.enabled or not integration.base_url or not api_key:
            return {}
        links: dict[str, str] = {
            "manager_label": "Sonarr",
            "manager_home_url": integration.base_url.rstrip("/"),
            "cutoff_url": f"{integration.base_url.rstrip('/')}/wanted/cutoffunmet",
        }
        entity_kind, entity_value = parse_sonarr_entity_id(finding.manager_entity_id)
        try:
            if entity_value is not None:
                client = SonarrClient(integration.base_url, api_key)
                series_id = entity_value if entity_kind == "series" else (await client.get_episode_by_id(entity_value)).get("seriesId")
                if series_id is not None:
                    series = await client.get_series(int(series_id))
                    title_slug = series.get("titleSlug")
                    if title_slug:
                        links["manager_open_url"] = f"{integration.base_url.rstrip('/')}/series/{title_slug}"
                        links["interactive_hint"] = "Open this series in Sonarr, then use Interactive Search on the episode row."
        except Exception:
            pass
        return links

    if finding.manager_kind == ManagerKind.RADARR.value:
        integration = await get_integration(session, IntegrationKind.RADARR)
        api_key = reveal_integration_api_key(integration)
        if not integration or not integration.enabled or not integration.base_url or not api_key:
            return {}
        links: dict[str, str] = {
            "manager_label": "Radarr",
            "manager_home_url": integration.base_url.rstrip("/"),
        }
        try:
            if finding.manager_entity_id and str(finding.manager_entity_id).isdigit():
                movie = await RadarrClient(integration.base_url, api_key).get_movie(int(finding.manager_entity_id))
                title_slug = movie.get("titleSlug")
                if title_slug:
                    links["manager_open_url"] = f"{integration.base_url.rstrip('/')}/movie/{title_slug}"
        except Exception:
            pass
        return links

    return {}


async def _queue_support_label(session: AsyncSession, finding: Finding) -> str:
    if _supports_manager_remediation(finding):
        return finding.manager_kind or "linked"
    if finding.media_kind == "tv":
        integration = await get_integration(session, IntegrationKind.SONARR)
        if integration and integration.enabled and integration.base_url:
            return "sonarr-relink"
    if finding.media_kind == "movie":
        integration = await get_integration(session, IntegrationKind.RADARR)
        if integration and integration.enabled and integration.base_url:
            return "radarr-relink"
    return "manual"


@router.get("/findings", response_class=HTMLResponse)
async def findings_list(
    request: Request,
    _user: str = Depends(require_user),
    session: AsyncSession = Depends(get_db),
    manager: str | None = None,
    media: str | None = None,
    unresolved_only: bool = False,
    ignored_only: bool = False,
    high_only: bool = False,
    action: str | None = None,
    reason: str | None = None,
    state: str | None = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
):
    page_size = page_size if page_size in ALLOWED_PAGE_SIZES else DEFAULT_PAGE_SIZE
    s = get_settings()
    csrf = generate_csrf_token(s.secret_key)
    job_state = _latest_job_state_expressions()

    base_conditions = []
    if manager:
        if manager == "none":
            base_conditions.append(or_(Finding.manager_kind.is_(None), Finding.manager_kind == "none"))
        else:
            base_conditions.append(Finding.manager_kind == manager)
    if media:
        base_conditions.append(Finding.media_kind == media)
    if ignored_only:
        base_conditions.append(Finding.ignored.is_(True))
    else:
        base_conditions.append(Finding.ignored.is_(False))
    if high_only:
        base_conditions.append(Finding.confidence == "high")
    if action:
        base_conditions.append(Finding.proposed_action == action)
    if unresolved_only:
        base_conditions.append(job_state["needs_review"])
    if not state:
        base_conditions.append(Finding.status != FindingStatus.RESOLVED.value)
    elif state == "awaiting":
        base_conditions.append(or_(job_state["queued_or_running"], job_state["pending_verify"]))
    elif state == "review":
        base_conditions.append(job_state["needs_review"])
    elif state == "failed":
        base_conditions.append(job_state["failed"])
    elif state == "resolved":
        base_conditions.append(Finding.status == FindingStatus.RESOLVED.value)

    pre_reason_conditions = list(base_conditions)
    filtered_conditions = list(base_conditions)
    if reason:
        filtered_conditions.append(
            select(FindingReason.id)
            .where(
                FindingReason.finding_id == Finding.id,
                FindingReason.code == reason,
            )
            .exists()
        )

    reason_rows = (
        await session.execute(
            select(FindingReason.code, func.count(FindingReason.id))
            .join(Finding, Finding.id == FindingReason.finding_id)
            .where(*pre_reason_conditions, FindingReason.code.not_in(HIDDEN_REASON_CODES))
            .group_by(FindingReason.code)
        )
    ).all()
    reason_counts = Counter({code: count for code, count in reason_rows})
    reason_options = [
        {"code": code, "count": count}
        for code, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    total_items = await session.scalar(
        select(func.count()).select_from(Finding).where(*filtered_conditions)
    ) or 0
    summary = {
        "total": total_items,
        "review": await session.scalar(
            select(func.count()).select_from(Finding).where(*filtered_conditions, job_state["needs_review"], Finding.ignored.is_(False))
        ) or 0,
        "high": await session.scalar(
            select(func.count()).select_from(Finding).where(*filtered_conditions, Finding.confidence == "high")
        ) or 0,
        "unmanaged": await session.scalar(
            select(func.count()).select_from(Finding).where(
                *filtered_conditions,
                or_(Finding.manager_kind.is_(None), Finding.manager_kind == "none"),
            )
        ) or 0,
        "pending_verify": await session.scalar(
            select(func.count()).select_from(Finding).where(
                *filtered_conditions,
                or_(job_state["queued_or_running"], job_state["pending_verify"]),
                Finding.status.in_(_OPEN_FINDING_STATUSES),
            )
        ) or 0,
    }
    pagination = build_pagination(
        base_path="/findings",
        page=page,
        page_size=page_size,
        total_items=total_items,
        params={
            "manager": manager or "",
            "media": media or "",
            "action": action or "",
            "reason": reason or "",
            "state": state or "",
            "page_size": str(page_size),
            "unresolved_only": "1" if unresolved_only else "",
            "ignored_only": "1" if ignored_only else "",
            "high_only": "1" if high_only else "",
        },
    )
    page_size_urls = {}
    for option in ALLOWED_PAGE_SIZES:
        query = {
            "manager": manager or "",
            "media": media or "",
            "action": action or "",
            "reason": reason or "",
            "state": state or "",
            "page_size": str(option),
            "unresolved_only": "1" if unresolved_only else "",
            "ignored_only": "1" if ignored_only else "",
            "high_only": "1" if high_only else "",
            "page": "1",
        }
        page_size_urls[option] = f"/findings?{urlencode({key: value for key, value in query.items() if value not in ('', None)})}"
    page_number = int(pagination["page"])
    findings = list(
        (
            await session.execute(
                select(Finding)
                .options(selectinload(Finding.reasons), selectinload(Finding.jobs))
                .where(*filtered_conditions)
                .order_by(Finding.last_seen_at.desc())
                .offset((page_number - 1) * page_size)
                .limit(page_size)
            )
        )
        .scalars()
        .unique()
        .all()
    )
    return templates.TemplateResponse(
        request,
        "findings/list.html",
        {
            "request": request,
            "csrf_token": csrf,
            "findings": findings,
            "summary": summary,
            "pagination": pagination,
            "page_size": page_size,
            "page_size_options": ALLOWED_PAGE_SIZES,
            "page_size_urls": page_size_urls,
            "reason_options": reason_options,
            "filters": {
                "manager": manager or "",
                "media": media or "",
                "unresolved_only": unresolved_only,
                "ignored_only": ignored_only,
                "high_only": high_only,
                "action": action or "",
                "reason": reason or "",
                "state": state or "",
            },
        },
    )


@router.get("/findings/{finding_id}", response_class=HTMLResponse)
async def finding_detail(
    request: Request,
    finding_id: int,
    _user: str = Depends(require_user),
    session: AsyncSession = Depends(get_db),
):
    s = get_settings()
    csrf = generate_csrf_token(s.secret_key)
    r = await session.execute(
        select(Finding)
        .options(
            selectinload(Finding.reasons),
            selectinload(Finding.jobs).selectinload(RemediationJob.attempts),
        )
        .where(Finding.id == finding_id)
    )
    finding = r.scalar_one_or_none()
    if not finding:
        return RedirectResponse("/findings?finding_msg=missing", status_code=302)
    jobs = sorted(finding.jobs, key=lambda j: j.id, reverse=True)[:20]
    manager_links = await _manager_links(session, finding)
    return templates.TemplateResponse(
        request,
        "findings/detail.html",
        {
            "request": request,
            "csrf_token": csrf,
            "finding": finding,
            "jobs": jobs,
            "can_remediate": await _can_queue_remediation(session, finding),
            "can_delete_replace": _supports_delete_replace(finding),
            "queue_mode": await _queue_support_label(session, finding),
            "derived_state": _derived_finding_state(finding),
            "manager_links": manager_links,
        },
    )


@router.post("/findings/{finding_id}/ignore")
async def finding_ignore(
    request: Request,
    finding_id: int,
    _user: str = Depends(require_user),
    session: AsyncSession = Depends(get_db),
    csrf_token: str = Form(...),
):
    require_csrf(request, csrf_token)
    f = await session.get(Finding, finding_id)
    if f:
        f.ignored = True
        f.status = "ignored"
        await log_event(session, event_type="finding_ignored", entity_type="finding", entity_id=str(f.id), message="Finding ignored", actor=_user)
    return RedirectResponse(f"/findings/{finding_id}?status_msg=ignored", status_code=302)


@router.post("/findings/{finding_id}/review")
async def finding_review(
    request: Request,
    finding_id: int,
    _user: str = Depends(require_user),
    session: AsyncSession = Depends(get_db),
    csrf_token: str = Form(...),
):
    require_csrf(request, csrf_token)
    f = await session.get(Finding, finding_id)
    if f:
        f.ignored = False
        f.status = FindingStatus.UNRESOLVED.value
        await log_event(
            session,
            event_type="finding_reviewed",
            entity_type="finding",
            entity_id=str(f.id),
            message="Finding reviewed",
            actor=_user,
        )
    return RedirectResponse(f"/findings/{finding_id}?status_msg=reviewed", status_code=302)


@router.post("/findings/{finding_id}/unignore")
async def finding_unignore(
    request: Request,
    finding_id: int,
    _user: str = Depends(require_user),
    session: AsyncSession = Depends(get_db),
    csrf_token: str = Form(...),
):
    require_csrf(request, csrf_token)
    f = await session.get(Finding, finding_id)
    if f:
        f.ignored = False
        if f.status == "ignored":
            f.status = "open"
        await log_event(session, event_type="finding_unignored", entity_type="finding", entity_id=str(f.id), message="Finding unignored", actor=_user)
    return RedirectResponse(f"/findings/{finding_id}?status_msg=unignored", status_code=302)


@router.post("/findings/{finding_id}/remediate")
@limiter.limit("30/minute")
async def finding_remediate(
    request: Request,
    finding_id: int,
    _user: str = Depends(require_user),
    session: AsyncSession = Depends(get_db),
    csrf_token: str = Form(...),
    mode: str = Form("rescan_only"),
):
    require_csrf(request, csrf_token)
    finding = await session.get(Finding, finding_id)
    if not finding:
        return RedirectResponse("/findings?finding_msg=missing", status_code=302)
    if not await _can_queue_remediation(session, finding):
        return RedirectResponse(f"/findings/{finding_id}?job_msg=manual_only", status_code=302)
    if mode == "delete_search":
        if not _supports_delete_replace(finding):
            return RedirectResponse(f"/findings/{finding_id}?job_msg=delete_replace_unavailable", status_code=302)
        action = RemediationAction.DELETE_SEARCH_REPLACEMENT
    else:
        action = (
            RemediationAction.SEARCH_REPLACEMENT if mode == "search" else RemediationAction.RESCAN_ONLY
        )
    await create_job(session, finding_id=finding_id, action=action, requested_by=_user, actor=_user)
    return RedirectResponse(f"/findings/{finding_id}?job_msg=queued", status_code=302)


@router.post("/findings/bulk")
@limiter.limit("60/minute")
async def findings_bulk(
    request: Request,
    _user: str = Depends(require_user),
    session: AsyncSession = Depends(get_db),
    csrf_token: str = Form(...),
    action: str = Form(...),
    finding_ids: list[int] = Form([]),
    manager: str = Form(""),
    media: str = Form(""),
    filter_action: str = Form(""),
    unresolved_only: str = Form(""),
    ignored_only: str = Form(""),
    high_only: str = Form(""),
    reason: str = Form(""),
    state: str = Form(""),
    page_size: str = Form(str(DEFAULT_PAGE_SIZE)),
    page: str = Form("1"),
):
    require_csrf(request, csrf_token)
    queued = 0
    reviewed = 0
    updated = 0
    skipped_manual = 0
    verify_scan = None
    redirect_params = _findings_redirect_params(
        manager=manager,
        media=media,
        filter_action=filter_action,
        unresolved_only=unresolved_only,
        ignored_only=ignored_only,
        high_only=high_only,
        reason=reason,
        state=state,
        page_size=page_size,
        page=page,
    )
    if not finding_ids:
        redirect_params["selection_msg"] = "missing"
        return RedirectResponse(_append_query_params("/findings", redirect_params), status_code=302)
    for fid in finding_ids:
        f = await session.get(Finding, fid)
        if not f:
            continue
        if action == "ignore":
            f.ignored = True
            f.status = "ignored"
            updated += 1
        elif action == "unignore":
            f.ignored = False
            if f.status == "ignored":
                f.status = "open"
            updated += 1
        elif action == "review":
            f.ignored = False
            f.status = FindingStatus.UNRESOLVED.value
            reviewed += 1
        elif action == "rescan":
            if await _can_queue_remediation(session, f):
                await create_job(session, finding_id=fid, action=RemediationAction.RESCAN_ONLY, requested_by=_user, actor=_user)
                queued += 1
            else:
                skipped_manual += 1
        elif action == "search":
            if await _can_queue_remediation(session, f):
                await create_job(session, finding_id=fid, action=RemediationAction.SEARCH_REPLACEMENT, requested_by=_user, actor=_user)
                queued += 1
            else:
                skipped_manual += 1
        elif action == "delete_search":
            if await _can_queue_remediation(session, f) and _supports_delete_replace(f):
                await create_job(
                    session,
                    finding_id=fid,
                    action=RemediationAction.DELETE_SEARCH_REPLACEMENT,
                    requested_by=_user,
                    actor=_user,
                )
                queued += 1
            else:
                skipped_manual += 1
        elif action == "verify":
            continue
    if action == "verify" and finding_ids:
        verify_scan = await start_verify_scan(finding_ids, actor=_user)
    await log_event(
        session,
        event_type="bulk_action",
        entity_type="finding",
        message=f"Bulk {action} on {len(finding_ids)} items",
        metadata={"ids": finding_ids, "queued": queued, "reviewed": reviewed, "updated": updated, "skipped_manual": skipped_manual},
        actor=_user,
    )
    if queued:
        redirect_params["queued"] = str(queued)
    if reviewed:
        redirect_params["reviewed"] = str(reviewed)
    if updated:
        redirect_params["updated"] = str(updated)
    if skipped_manual:
        redirect_params["manual_only"] = str(skipped_manual)
    if action == "verify" and finding_ids:
        redirect_params["verify_msg"] = "started" if verify_scan else "already_running"
    return RedirectResponse(_append_query_params("/findings", redirect_params), status_code=302)
