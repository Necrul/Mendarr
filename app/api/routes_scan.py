from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import require_csrf, require_user
from app.domain.scan_notes import parse_scan_notes, scan_progress_percent
from app.persistence.db import get_db
from app.persistence.models import ScanRun
from app.rate_limit import limiter
from app.services.scan_service import latest_resumable_library_scan
from app.services.scan_service import request_scan_stop
from app.services.scan_service import scan_stop_requested
from app.services.scan_service import start_scan as start_background_scan
from app.services.scan_service import start_verify_scan as start_background_verify_scan

router = APIRouter(tags=["scan"])


def _append_query_params(target: str, params: dict[str, str]) -> str:
    parsed = urlsplit(target)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({key: value for key, value in params.items() if value not in ("", None)})
    filtered = {key: value for key, value in query.items() if value not in ("", None)}
    query_text = urlencode(filtered)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query_text, parsed.fragment))


def _prefers_json(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "application/json" in accept.lower()


@router.post("/scan/start")
@limiter.limit("30/hour")
async def start_scan_route(
    request: Request,
    user: str = Depends(require_user),
    csrf_token: str = Form(...),
    resume: str | None = Form(None),
    session: AsyncSession = Depends(get_db),
):
    require_csrf(request, csrf_token)
    resume_requested = resume not in (None, "", "0", "false", "False")
    if resume_requested:
        resumable_scan = await latest_resumable_library_scan(session)
        resumable_notes = parse_scan_notes(resumable_scan.notes) if resumable_scan else {}
        resume_after_file = resumable_notes.get("resume_after_file")
        if resumable_scan is None or not resume_after_file or not Path(str(resume_after_file)).exists():
            if _prefers_json(request):
                return JSONResponse({"started": False, "reason": "not_resumable"}, status_code=409)
            return RedirectResponse("/?scan=resume_missing", status_code=302)
    scan = await start_background_scan(actor=user, resume=resume_requested)
    if _prefers_json(request):
        if not scan:
            return JSONResponse({"started": False, "reason": "already_running"}, status_code=409)
        return JSONResponse({"started": True, "resumed": resume_requested, "scan": {"id": scan.id}}, status_code=202)
    if not scan:
        return RedirectResponse("/?scan=already_running", status_code=302)
    return RedirectResponse(
        f"/?scan={'resumed' if resume_requested else 'started'}&scan_id={scan.id}",
        status_code=302,
    )


@router.post("/scan/stop")
@limiter.limit("60/hour")
async def stop_scan_route(
    request: Request,
    _user: str = Depends(require_user),
    csrf_token: str = Form(...),
):
    require_csrf(request, csrf_token)
    run_id = await request_scan_stop(actor=_user)
    if _prefers_json(request):
        if run_id is None:
            return JSONResponse({"stopped": False, "reason": "not_running"}, status_code=409)
        return JSONResponse({"stopped": True, "scan": {"id": run_id}}, status_code=202)
    if run_id is None:
        return RedirectResponse("/?scan=not_running", status_code=302)
    return RedirectResponse(f"/?scan=stop_requested&scan_id={run_id}", status_code=302)


@router.post("/scan/verify")
@limiter.limit("60/hour")
async def start_verify_scan_route(
    request: Request,
    user: str = Depends(require_user),
    csrf_token: str = Form(...),
    finding_ids: list[int] = Form([]),
    return_to: str = Form("/findings"),
):
    require_csrf(request, csrf_token)
    target = return_to if return_to.startswith("/") else "/findings"
    if not finding_ids:
        return RedirectResponse(_append_query_params(target, {"verify_msg": "missing"}), status_code=302)
    scan = await start_background_verify_scan(finding_ids, actor=user)
    if not scan:
        return RedirectResponse(_append_query_params(target, {"verify_msg": "missing"}), status_code=302)
    return RedirectResponse(
        _append_query_params(
            target,
            {
                "verify_msg": "queued" if scan.status == "queued" else "started",
                "scan_id": str(scan.id),
            },
        ),
        status_code=302,
    )


@router.get("/api/scans/latest")
async def latest_scan_status(
    _user: str = Depends(require_user),
    session: AsyncSession = Depends(get_db),
):
    latest = (
        (await session.execute(select(ScanRun).where(ScanRun.status == "running").order_by(ScanRun.id.desc()).limit(1)))
        .scalars()
        .first()
    )
    if not latest:
        latest = (
            (await session.execute(select(ScanRun).where(ScanRun.status == "queued").order_by(ScanRun.id.desc()).limit(1)))
            .scalars()
            .first()
        )
    if not latest:
        latest = (
            (await session.execute(select(ScanRun).order_by(ScanRun.id.desc()).limit(1)))
            .scalars()
            .first()
        )
    if not latest:
        return JSONResponse({"scan": None})

    notes = parse_scan_notes(latest.notes)
    notes.setdefault("files_seen", latest.files_seen)
    notes.setdefault("findings", latest.suspicious_found)
    scope = notes.get("scope") or "library"
    return JSONResponse(
        {
            "scan": {
                "id": latest.id,
                "status": latest.status,
                "files_seen": latest.files_seen,
                "suspicious_found": latest.suspicious_found,
                "started_at": latest.started_at.isoformat() if latest.started_at else None,
                "completed_at": latest.completed_at.isoformat() if latest.completed_at else None,
                "scope": scope,
                "stop_requested": scan_stop_requested() if latest.status == "running" and scope != "verify" else False,
                "notes": notes,
                "progress_percent": scan_progress_percent(notes, status=latest.status),
            }
        }
    )
