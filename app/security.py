from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from urllib.parse import urlsplit
from typing import Any

from fastapi import Request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from starlette.responses import Response

from app.config import get_settings

ITERATIONS = 260_000
COOKIE_NAME = "mendarr_session"


def hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), ITERATIONS)
    return base64.b64encode(dk).decode(), salt


def verify_password(password: str, stored_hash: str, salt: str) -> bool:
    computed, _ = hash_password(password, salt)
    return secrets.compare_digest(computed, stored_hash)


def _serializer(secret: str) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(secret, salt="mendarr-session")


def create_session_token(username: str, secret: str, max_age_hours: int = 24) -> str:
    payload = {"u": username, "iat": int(time.time())}
    return _serializer(secret).dumps(payload)


def verify_session_token(token: str, secret: str, max_age_seconds: int = 86400) -> dict[str, Any] | None:
    try:
        return _serializer(secret).loads(token, max_age=max_age_seconds)
    except (BadSignature, SignatureExpired):
        return None


def generate_csrf_token(secret: str) -> str:
    raw = json.dumps({"r": secrets.token_hex(16), "t": int(time.time())})
    sig = hmac.new(secret.encode(), raw.encode(), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{raw}|{sig}".encode()).decode()


def verify_csrf_token(token: str, secret: str, max_age_seconds: int = 7200) -> bool:
    try:
        decoded = base64.urlsafe_b64decode(token.encode()).decode()
        raw, sig = decoded.rsplit("|", 1)
        exp = int(json.loads(raw).get("t", 0))
        if time.time() - exp > max_age_seconds:
            return False
        expect = hmac.new(secret.encode(), raw.encode(), hashlib.sha256).hexdigest()
        return secrets.compare_digest(sig, expect)
    except Exception:
        return False


def get_client_ip(request: Request) -> str:
    settings = get_settings()
    xff = request.headers.get("X-Forwarded-For", "")
    if settings.trust_proxy_headers and xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "127.0.0.1"


def is_request_secure(request: Request) -> bool:
    settings = get_settings()
    forwarded_proto = request.headers.get("X-Forwarded-Proto", "")
    if settings.trust_proxy_headers and forwarded_proto:
        return forwarded_proto.split(",")[0].strip().lower() == "https"
    return request.url.scheme == "https"


def sanitize_next_url(target: str | None) -> str:
    if not target:
        return "/"
    parsed = urlsplit(target)
    if parsed.scheme or parsed.netloc:
        return "/"
    if not target.startswith("/") or target.startswith("//") or "\\" in target:
        return "/"
    return target


def set_session_cookie(response: Response, username: str, *, secure: bool) -> None:
    s = get_settings()
    token = create_session_token(username, s.secret_key)
    response.set_cookie(
        COOKIE_NAME,
        token,
        httponly=True,
        secure=secure,
        samesite="lax",
        max_age=86400,
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(COOKIE_NAME, path="/")


def mask_api_key(key: str) -> str:
    if not key:
        return ""
    if len(key) <= 8:
        return "********"
    return f"********{key[-4:]}"


def safe_resolve_under_roots(candidate: str, allowed_roots: list[str]) -> str | None:
    """Return resolved path if it is under one of allowed_roots, else None."""
    try:
        import os

        real = os.path.realpath(candidate)
        for root in allowed_roots:
            rroot = os.path.realpath(root)
            if real == rroot or real.startswith(rroot + os.sep):
                return real
    except OSError:
        return None
    return None
