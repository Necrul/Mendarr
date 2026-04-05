from __future__ import annotations

import datetime as dt


def _as_utc(value: object) -> dt.datetime | None:
    if not isinstance(value, dt.datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def derive_finding_state(finding: object | None) -> str:
    if not finding:
        return "open"

    status = getattr(finding, "status", None) or "open"
    jobs = list(getattr(finding, "jobs", []) or [])
    if not jobs or status not in {"open", "unresolved"}:
        return status

    latest_job = max(jobs, key=lambda job: getattr(job, "id", 0))
    latest_status = getattr(latest_job, "status", None)
    if latest_status in {"queued", "running", "failed"}:
        return latest_status
    if latest_status != "succeeded":
        return status

    verified_at = _as_utc(getattr(finding, "last_scanned_at", None))
    repaired_at = _as_utc(getattr(latest_job, "completed_at", None) or getattr(latest_job, "started_at", None))
    if verified_at and repaired_at and verified_at > repaired_at:
        return status
    return "pending_verify"
