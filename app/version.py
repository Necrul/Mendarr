from __future__ import annotations

from app.config import get_settings


def get_app_version() -> str:
    return get_settings().app_version.strip() or "1.0.0"


def get_version_label() -> str:
    version = get_app_version().lstrip("vV")
    return f"v{version}"
