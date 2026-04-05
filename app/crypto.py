from __future__ import annotations

import base64
import hashlib
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

from app.config import get_settings

SECRET_PREFIX = "enc:v1:"


def _key_seed() -> str:
    settings = get_settings()
    return settings.encryption_key or settings.secret_key


@lru_cache
def _fernet_for_seed(seed: str) -> Fernet:
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _fernet() -> Fernet:
    return _fernet_for_seed(_key_seed())


def is_encrypted_secret(value: str) -> bool:
    return bool(value) and value.startswith(SECRET_PREFIX)


def encrypt_secret(value: str) -> str:
    if not value:
        return ""
    if is_encrypted_secret(value):
        return value
    token = _fernet().encrypt(value.encode("utf-8")).decode("utf-8")
    return f"{SECRET_PREFIX}{token}"


def decrypt_secret(value: str) -> str:
    if not value:
        return ""
    if not is_encrypted_secret(value):
        return value
    token = value[len(SECRET_PREFIX) :].encode("utf-8")
    try:
        return _fernet().decrypt(token).decode("utf-8")
    except InvalidToken as exc:
        raise RuntimeError("Unable to decrypt stored integration secret with current key material") from exc
