from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.logging import get_logger
from app.persistence.models import AppUser
from app.security import hash_password, verify_password

log = get_logger(__name__)
MIN_ADMIN_PASSWORD_LENGTH = 12


async def ensure_default_admin(session: AsyncSession) -> None:
    r = await session.execute(select(AppUser).limit(1))
    if r.scalar_one_or_none():
        return
    s = get_settings()
    pwd_hash = (s.admin_password_hash or "").strip()
    pwd_salt = (s.admin_password_salt or "").strip()
    if pwd_hash and pwd_salt:
        session.add(
            AppUser(
                username=s.admin_username,
                password_hash=pwd_hash,
                password_salt=pwd_salt,
            )
        )
        await session.flush()
        log.info("Created initial admin user %s from configured hash", s.admin_username)
        return

    pwd = (s.admin_password or "").strip()
    if not pwd:
        log.warning(
            "No MENDARR_ADMIN_PASSWORD or hash configured; set one and restart to seed the first admin"
        )
        return
    if len(pwd) < MIN_ADMIN_PASSWORD_LENGTH:
        log.warning(
            "MENDARR_ADMIN_PASSWORD is too short to seed the first admin; use at least %s characters",
            MIN_ADMIN_PASSWORD_LENGTH,
        )
        return
    h, salt = hash_password(pwd)
    session.add(AppUser(username=s.admin_username, password_hash=h, password_salt=salt))
    await session.flush()
    log.info("Created initial admin user %s", s.admin_username)


async def verify_login(session: AsyncSession, username: str, password: str) -> bool:
    r = await session.execute(select(AppUser).where(AppUser.username == username))
    u = r.scalar_one_or_none()
    if not u:
        return False
    return verify_password(password, u.password_hash, u.password_salt)
