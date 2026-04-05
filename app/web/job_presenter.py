from __future__ import annotations


SEARCH_STEPS = {"EpisodeSearch", "SeriesSearch", "MoviesSearch"}
DELETE_STEPS = {"DeleteEpisodeFile", "DeleteMovieFile"}
REFRESH_STEPS = {"RescanSeries", "RefreshMovie"}


def latest_attempt(job: object | None) -> object | None:
    attempts = list(getattr(job, "attempts", []) or [])
    if not attempts:
        return None
    return max(attempts, key=lambda attempt: getattr(attempt, "id", 0))


def _manager_label(job: object | None) -> str:
    finding = getattr(job, "finding", None)
    manager_kind = (getattr(finding, "manager_kind", None) or "").strip().lower()
    if manager_kind == "sonarr":
        return "Sonarr"
    if manager_kind == "radarr":
        return "Radarr"
    return "the manager"


def remediation_result_code(job: object | None) -> str:
    if not job:
        return "unknown"
    status = getattr(job, "status", None) or ""
    if status in {"queued", "running", "failed"}:
        if status == "failed":
            lowered = str(getattr(job, "last_error", "") or "").strip().lower()
            if "cutoff already met" in lowered or "will not force an upgrade" in lowered:
                return "cutoff_met"
        return status
    if status != "succeeded":
        return status or "unknown"

    attempt = latest_attempt(job)
    step = getattr(attempt, "step_name", None)
    if step in DELETE_STEPS:
        action_type = getattr(job, "action_type", None)
        if action_type == "delete_search_replacement":
            return "delete_accepted"
        return "accepted"
    if step in SEARCH_STEPS:
        if getattr(job, "action_type", None) == "delete_search_replacement":
            return "delete_accepted"
        return "search_accepted"
    if step in REFRESH_STEPS:
        return "refresh_accepted"
    return "accepted"


def remediation_result_label(job: object | None) -> str:
    code = remediation_result_code(job)
    labels = {
        "accepted": "Accepted",
        "cutoff_met": "Cutoff already met",
        "delete_accepted": "Delete and search accepted",
        "failed": "Failed",
        "queued": "Awaiting repair",
        "refresh_accepted": "Rescan accepted",
        "running": "Repair running",
        "search_accepted": "Search accepted",
        "unknown": "Unknown",
    }
    return labels.get(code, code.replace("_", " ").strip().title())


def remediation_result_message(job: object | None) -> str:
    if not job:
        return ""
    code = remediation_result_code(job)
    manager = _manager_label(job)
    if code == "search_accepted":
        return f"{manager} accepted the search request. Verify to confirm it actually imported a better file."
    if code == "delete_accepted":
        return f"{manager} accepted the delete-and-search request. The current file was removed in the manager, but replacement is still not guaranteed."
    if code == "refresh_accepted":
        return f"{manager} accepted the rescan request."
    if code == "cutoff_met":
        return f"{manager} reports cutoff already met, so this search would not force an upgrade."
    if code == "failed":
        return str(getattr(job, "last_error", "") or "").strip()
    if code == "queued":
        return "Waiting for Mendarr to send the request."
    if code == "running":
        return "Mendarr is sending the request now."
    if code == "accepted":
        return f"{manager} accepted the request."
    return ""
