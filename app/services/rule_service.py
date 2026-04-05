from __future__ import annotations

import datetime as dt

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.persistence.models import RuleSettingsRow


async def get_or_create_rule_settings(session: AsyncSession) -> RuleSettingsRow:
    r = await session.execute(select(RuleSettingsRow).limit(1))
    row = r.scalar_one_or_none()
    if row:
        return row
    s = get_settings()
    row = RuleSettingsRow(
        min_tv_size_bytes=s.min_tv_size_bytes,
        min_movie_size_bytes=s.min_movie_size_bytes,
        min_duration_tv_seconds=s.min_duration_tv_seconds,
        min_duration_movie_seconds=s.min_duration_movie_seconds,
        excluded_keywords="",
        extras_keywords="",
        excluded_paths="",
        ignored_patterns="",
        auto_remediation_enabled=s.auto_remediation_enabled,
        updated_at=dt.datetime.now(dt.UTC),
    )
    session.add(row)
    await session.flush()
    return row


def extras_tuple_from_settings(row: RuleSettingsRow) -> tuple[str, ...]:
    from app.domain.scoring import DEFAULT_EXTRAS_KEYWORDS

    extra = [x.strip() for x in (row.extras_keywords or "").split(",") if x.strip()]
    if extra:
        return tuple(extra)
    return DEFAULT_EXTRAS_KEYWORDS


def excluded_keywords_tuple_from_settings(row: RuleSettingsRow) -> tuple[str, ...]:
    return tuple(x.strip() for x in (row.excluded_keywords or "").split(",") if x.strip())
