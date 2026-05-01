from __future__ import annotations

import json
from functools import lru_cache
from pathlib import PurePath

from fastapi.templating import Jinja2Templates

from app.domain.finding_state import derive_finding_state
from app.domain.matching import normalize_title_token
from app.domain.scan_notes import scan_note_pairs
from app.web.job_presenter import remediation_result_label, remediation_result_message
from app.domain.value_objects import (
    BAD_EXTENSION,
    BITRATE_ANOMALY,
    CTX_TRAILER_IN_MAIN,
    DUPLICATE_VARIANT,
    KEYWORD_SAMPLE_TRAILER,
    MANAGER_METADATA_MISMATCH,
    MISSING_EXPECTED,
    NO_AUDIO,
    NO_DURATION,
    NO_RESOLUTION,
    NO_VIDEO_CODEC,
    NO_VIDEO_STREAM,
    PATH_EXCLUDED,
    PROBE_FAILED,
    RULE_IGNORE_PATTERN,
    SHORT_DURATION,
    VERY_SMALL,
    ZERO_BYTE,
)

HIDDEN_REASON_CODES = {KEYWORD_SAMPLE_TRAILER, CTX_TRAILER_IN_MAIN}


EVENT_TYPE_LABELS = {
    "bulk_action": "Bulk action",
    "finding_created": "Finding created",
    "finding_ignored": "Finding ignored",
    "finding_reviewed": "Finding reviewed",
    "finding_resolved": "Finding resolved",
    "finding_unignored": "Finding restored",
    "finding_updated": "Finding updated",
    "integration_saved": "Connection saved",
    "integration_test": "Connection test",
    "job_failed": "Repair failed",
    "job_queued": "Repair queued",
    "job_succeeded": "Repair accepted",
    "library_root_added": "Library added",
    "library_root_removed": "Library removed",
    "rule_exception_added": "Rule exception added",
    "rules_updated": "Rules updated",
    "scan_completed": "Scan finished",
    "scan_interrupted": "Scan interrupted",
    "scan_started": "Scan started",
}

ACTION_LABELS = {
    "blocked": "Blocked",
    "delete_search_replacement": "Delete and replace",
    "ignore": "Ignore",
    "rescan_only": "Rescan",
    "review": "Needs review",
    "search_replacement": "Search replacement",
}

ATTEMPT_LABELS = {
    "DeleteEpisodeFile": "Delete current file",
    "DeleteMovieFile": "Delete current file",
    "EpisodeSearch": "Episode search",
    "SeriesSearch": "Series search",
    "MoviesSearch": "Movie search",
    "RescanSeries": "Series rescan",
    "RefreshMovie": "Movie refresh",
    "error": "Failed request",
}

QUEUE_STATE_LABELS = {
    "queued": "Awaiting repair",
    "running": "Repair running",
    "failed": "Repair failed",
}

STATUS_LABELS = {
    "failed": "Failed",
    "ignored": "Ignored",
    "open": "Open",
    "pending_verify": "Pending verification",
    "queued": "Awaiting repair",
    "unresolved": "Reviewed",
    "resolved": "Resolved",
    "running": "Running",
    "completed": "Completed",
    "interrupted": "Interrupted",
    "succeeded": "Accepted",
}

REASON_LABELS = {
    ZERO_BYTE: "Zero-byte file",
    VERY_SMALL: "Very small file",
    BAD_EXTENSION: "Unexpected extension",
    DUPLICATE_VARIANT: "Duplicate variant nearby",
    KEYWORD_SAMPLE_TRAILER: "Looks like an extra",
    MISSING_EXPECTED: "Missing expected file",
    PROBE_FAILED: "Probe failed",
    NO_VIDEO_STREAM: "No video stream",
    NO_DURATION: "Missing duration",
    SHORT_DURATION: "Too short",
    NO_RESOLUTION: "Missing resolution",
    NO_VIDEO_CODEC: "Missing video codec",
    NO_AUDIO: "No audio stream",
    BITRATE_ANOMALY: "Low bitrate anomaly",
    CTX_TRAILER_IN_MAIN: "Trailer-style naming",
    MANAGER_METADATA_MISMATCH: "Manager metadata mismatch",
    PATH_EXCLUDED: "Excluded path",
    RULE_IGNORE_PATTERN: "Ignored by rule",
}


def _basename(path_value: str | None) -> str:
    if not path_value:
        return "-"
    cleaned = str(path_value).rstrip("/\\")
    if not cleaned:
        return str(path_value)
    name = PurePath(cleaned).name
    return name or cleaned


def _humanize_event_type(value: str | None) -> str:
    if not value:
        return "Activity"
    return EVENT_TYPE_LABELS.get(value, value.replace("_", " ").strip().title())


def _format_bytes(value: int | float | None) -> str:
    if value in (None, ""):
        return "-"
    try:
        size = float(value)
    except (TypeError, ValueError):
        return str(value)

    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return str(value)


def _parse_scan_notes(value: str | None, status: str | None = None) -> list[tuple[str, str]]:
    return scan_note_pairs(value, status=status)


def _humanize_action(value: str | None) -> str:
    if not value:
        return "Review"
    return ACTION_LABELS.get(value, value.replace("_", " ").strip().title())


def _humanize_status(value: str | None) -> str:
    if not value:
        return "Open"
    return STATUS_LABELS.get(value, value.replace("_", " ").strip().title())


def _humanize_attempt(value: str | None) -> str:
    if not value:
        return "Attempt"
    return ATTEMPT_LABELS.get(value, value.replace("_", " ").strip().title())


def humanize_attempt_label(value: str | None) -> str:
    return _humanize_attempt(value)


def humanize_action_label(value: str | None) -> str:
    return _humanize_action(value)


def _finding_state_code(finding: object | None) -> str:
    return derive_finding_state(finding)


def _finding_state_label(finding: object | None) -> str:
    code = _finding_state_code(finding)
    if code in QUEUE_STATE_LABELS:
        return QUEUE_STATE_LABELS[code]
    return _humanize_status(code)


def _finding_primary_name(finding: object | None) -> str:
    if not finding:
        return "Untitled"
    file_name = getattr(finding, "file_name", None)
    title = getattr(finding, "title", None)
    return file_name or title or "Untitled"


def _finding_secondary_name(finding: object | None) -> str:
    if not finding:
        return ""
    title = (getattr(finding, "title", None) or "").strip()
    file_name = (getattr(finding, "file_name", None) or "").strip()
    normalized_title = normalize_title_token(title)
    normalized_file_name = normalize_title_token(file_name)
    if (
        title
        and file_name
        and title.casefold() != file_name.casefold()
        and len(normalized_title) >= 4
        and normalized_title not in normalized_file_name
    ):
        return title
    return ""


def _humanize_reason(value: str | None) -> str:
    if not value:
        return "Unknown reason"
    if value in HIDDEN_REASON_CODES:
        return "Ignored signal"
    return REASON_LABELS.get(value, value.replace("_", " ").replace("FS ", "").replace("MD ", "").replace("CTX ", "").replace("RULE ", "").strip().title())


def _primary_reason(reasons: list[object] | None) -> str:
    if not reasons:
        return "Needs review"
    first = next(
        (reason for reason in reasons if getattr(reason, "code", None) not in HIDDEN_REASON_CODES),
        reasons[0],
    )
    code = getattr(first, "code", None)
    if code in HIDDEN_REASON_CODES:
        return "Needs review"
    return _humanize_reason(code)


def humanize_failure_reason(message: str | None) -> str:
    if not message:
        return "Unknown failure"
    text = str(message).strip()
    lowered = text.lower()
    if "manual review only" in lowered or "not linked to sonarr or radarr" in lowered:
        return "Manual review only"
    if "episode search requires a sonarr episode id" in lowered:
        return "Missing Sonarr episode link"
    if "no sonarr entity id" in lowered:
        return "Missing Sonarr mapping"
    if "no radarr movie id" in lowered:
        return "Missing Radarr mapping"
    if "sonarr not configured" in lowered:
        return "Sonarr is not configured"
    if "radarr not configured" in lowered:
        return "Radarr is not configured"
    if "finding missing" in lowered:
        return "Finding no longer exists"
    if "database is locked" in lowered:
        return "Database is busy"
    if "cutoff already met" in lowered or "will not force an upgrade" in lowered:
        return "Cutoff already met in Sonarr"
    if "manager request timed out" in lowered or lowered == "readtimeout":
        return "Manager request timed out"
    if "connection to the manager timed out" in lowered or lowered == "connecttimeout":
        return "Connection to the manager timed out"
    if "could not connect to the manager" in lowered or "connecterror" in lowered:
        return "Could not connect to the manager"
    return text


def _humanize_attempt_summary(attempt: object | None) -> str:
    if not attempt:
        return ""
    raw_summary = (
        getattr(attempt, "response_summary", None)
        or getattr(attempt, "request_summary", None)
        or ""
    )
    if not raw_summary:
        return ""
    text = str(raw_summary).strip()
    parsed: object = text
    if text.startswith("{") or text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = text
    if isinstance(parsed, dict):
        if parsed.get("error"):
            return humanize_failure_reason(str(parsed.get("error")))
        body = parsed.get("body")
        command_name = parsed.get("commandName") or parsed.get("name")
        if isinstance(body, dict):
            if body.get("episodeIds"):
                ids = ", ".join(str(value) for value in body["episodeIds"])
                return f"Episode search requested for episode id {ids}"
            if body.get("seriesId"):
                if str(command_name).lower().startswith("rescan"):
                    return f"Series rescan requested for series id {body['seriesId']}"
                return f"Series search requested for series id {body['seriesId']}"
            if body.get("movieIds"):
                ids = ", ".join(str(value) for value in body["movieIds"])
                return f"Movie search requested for movie id {ids}"
            if body.get("movieId"):
                if str(command_name).lower().startswith("refresh"):
                    return f"Movie refresh requested for movie id {body['movieId']}"
                return f"Movie search requested for movie id {body['movieId']}"
        if command_name:
            return _humanize_attempt(str(command_name))
    return humanize_failure_reason(text[:220])


@lru_cache(maxsize=1)
def get_templates() -> Jinja2Templates:
    templates = Jinja2Templates(directory="app/templates")
    templates.env.filters["basename"] = _basename
    templates.env.filters["event_label"] = _humanize_event_type
    templates.env.filters["filesize"] = _format_bytes
    templates.env.filters["scan_notes"] = _parse_scan_notes
    templates.env.filters["action_label"] = _humanize_action
    templates.env.filters["status_label"] = _humanize_status
    templates.env.filters["attempt_label"] = _humanize_attempt
    templates.env.filters["finding_state_code"] = _finding_state_code
    templates.env.filters["finding_state_label"] = _finding_state_label
    templates.env.filters["finding_primary_name"] = _finding_primary_name
    templates.env.filters["finding_secondary_name"] = _finding_secondary_name
    templates.env.filters["reason_label"] = _humanize_reason
    templates.env.filters["primary_reason"] = _primary_reason
    templates.env.filters["failure_reason"] = humanize_failure_reason
    templates.env.filters["attempt_summary"] = _humanize_attempt_summary
    templates.env.filters["job_result_label"] = remediation_result_label
    templates.env.filters["job_result_message"] = remediation_result_message
    return templates
