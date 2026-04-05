"""Optional mediainfo CLI wrapper (stub for v1 — enable when binary configured)."""

from __future__ import annotations

from app.config import get_settings


def mediainfo_available() -> bool:
    s = get_settings()
    return bool(s.mediainfo_path and s.mediainfo_path.strip())
