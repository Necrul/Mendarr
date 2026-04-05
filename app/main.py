from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.api import (
    routes_activity,
    routes_auth,
    routes_dashboard,
    routes_findings,
    routes_health,
    routes_integrations,
    routes_jobs,
    routes_rules,
    routes_scan,
)
from app.config import get_settings, validate_runtime_secret_key
from app.logging import get_logger, setup_logging
from app.persistence.db import SessionLocal, init_db
from app.security import COOKIE_NAME, verify_session_token
from app.version import get_version_label
from app.rate_limit import limiter
from app.services.remediation_service import execute_job
from app.services.integration_service import migrate_legacy_integration_secrets
from app.services.scan_service import recover_abandoned_scans, stop_background_scan
from app.services.scan_service import start_next_queued_verify_scan
from app.services.user_service import ensure_default_admin

setup_logging()
log = get_logger(__name__)

_SECURITY_HEADERS = {
    "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'",
    "Cross-Origin-Opener-Policy": "same-origin",
    "Permissions-Policy": "camera=(), geolocation=(), microphone=()",
    "Referrer-Policy": "same-origin",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}


async def _job_worker_loop(stop: asyncio.Event) -> None:
    from sqlalchemy import select

    from app.domain.enums import JobStatus
    from app.persistence.models import RemediationJob

    while not stop.is_set():
        jid = None
        try:
            async with SessionLocal() as session:
                r = await session.execute(
                    select(RemediationJob)
                    .where(RemediationJob.status == JobStatus.QUEUED.value)
                    .order_by(RemediationJob.id)
                    .limit(1)
                )
                job = r.scalar_one_or_none()
                if job:
                    jid = job.id
            if jid is not None:
                async with SessionLocal() as session:
                    async with session.begin():
                        await execute_job(session, jid, actor="worker")
        except Exception:
            log.exception("job worker tick failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=4.0)
        except TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop = asyncio.Event()
    settings = get_settings()
    validate_runtime_secret_key(settings.secret_key)
    await init_db()
    async with SessionLocal() as s:
        async with s.begin():
            await ensure_default_admin(s)
            migrated = await migrate_legacy_integration_secrets(s)
            if migrated:
                log.info("Migrated %s legacy integration secret(s) to encrypted storage", migrated)
    recovered_scans = await recover_abandoned_scans()
    if recovered_scans:
        log.warning("Recovered %s interrupted scan(s) from a previous app session", recovered_scans)
    await start_next_queued_verify_scan(actor="worker")
    task = asyncio.create_task(_job_worker_loop(stop))
    yield
    stop.set()
    await stop_background_scan()
    await task


app = FastAPI(title=f"Mendarr {get_version_label()}", lifespan=lifespan)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

_PUBLIC = {"/login", "/health", "/favicon.ico"}
_PUBLIC_PREFIXES = ("/static/",)


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in _PUBLIC or any(path.startswith(p) for p in _PUBLIC_PREFIXES):
            return await call_next(request)
        settings = get_settings()
        token = request.cookies.get(COOKIE_NAME, "")
        if token and verify_session_token(token, settings.secret_key):
            return await call_next(request)
        if path.startswith("/api/"):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        return RedirectResponse(url=f"/login?next={request.url.path}", status_code=302)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        for header, value in _SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        return response


app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(AuthMiddleware)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(routes_health.router)
app.include_router(routes_auth.router)
app.include_router(routes_dashboard.router)
app.include_router(routes_findings.router)
app.include_router(routes_jobs.router)
app.include_router(routes_rules.router)
app.include_router(routes_integrations.router)
app.include_router(routes_activity.router)
app.include_router(routes_scan.router)

app.state.version_label = get_version_label()


@app.get("/api/ping")
async def ping():
    return {"ok": True}


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return RedirectResponse(url="/static/img/mendarr-logo.png", status_code=307)
