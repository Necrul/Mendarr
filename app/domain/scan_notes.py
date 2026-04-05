from __future__ import annotations

import json
from typing import Any


NOTE_LABELS = {
    "progress": "Progress",
    "scope": "Scope",
    "current_library": "Current library",
    "current_file": "Current file",
    "files_seen": "Files checked",
    "total_files": "Total files",
    "findings": "Findings",
    "target_count": "Targets",
    "resolved_findings": "Resolved",
    "moved_findings": "Moved",
    "libraries": "Libraries",
    "scanned_roots": "Scanned roots",
    "skipped_roots": "Skipped roots",
    "skipped_findings": "Skipped findings",
    "scanned_paths": "Scanned paths",
    "skipped_paths": "Skipped paths",
}


def parse_scan_notes(value: str | None) -> dict[str, Any]:
    if not value:
        return {}

    raw = value.strip()
    if not raw:
        return {}

    if raw.startswith("{"):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, dict):
            return decoded

    parsed: dict[str, Any] = {}
    for chunk in raw.split(";"):
        item = chunk.strip()
        if "=" not in item:
            continue
        key, raw_value = item.split("=", 1)
        key = key.strip()
        value_text = raw_value.strip()
        if not key or not value_text or value_text == "-":
            continue
        parsed[key] = value_text
    return parsed


def serialize_scan_notes(payload: dict[str, Any]) -> str | None:
    cleaned = {
        key: value
        for key, value in payload.items()
        if value not in (None, "", [], {})
    }
    if not cleaned:
        return None
    return json.dumps(cleaned, ensure_ascii=True, separators=(",", ":"))


def merge_scan_notes(existing: str | None, **updates: Any) -> str | None:
    merged = parse_scan_notes(existing)
    for key, value in updates.items():
        if value in (None, "", [], {}):
            merged.pop(key, None)
        else:
            merged[key] = value
    return serialize_scan_notes(merged)


def scan_progress_percent(payload: dict[str, Any], *, status: str | None = None) -> int | None:
    total_raw = payload.get("total_files")
    try:
        total = int(total_raw)
    except (TypeError, ValueError):
        total = 0

    if total <= 0:
        return 100 if status == "completed" else None

    files_seen_raw = payload.get("files_seen")
    try:
        files_seen = int(files_seen_raw)
    except (TypeError, ValueError):
        files_seen = 0

    if status == "completed":
        return 100

    ratio = max(files_seen, 0) / total
    percent = round(ratio * 100)
    if files_seen > 0 and percent == 0:
        percent = 1
    return max(0, min(percent, 100))


def scan_note_pairs(value: str | None, *, status: str | None = None) -> list[tuple[str, str]]:
    payload = parse_scan_notes(value)
    if not payload:
        return []

    pairs: list[tuple[str, str]] = []
    progress = scan_progress_percent(payload, status=status)
    files_seen = payload.get("files_seen")
    total_files = payload.get("total_files")
    if progress is not None and total_files not in (None, "", 0, "0"):
        pairs.append(("Progress", f"{progress}% ({files_seen or 0}/{total_files})"))

    for key in (
        "scope",
        "current_library",
        "current_file",
        "findings",
        "target_count",
        "resolved_findings",
        "moved_findings",
        "libraries",
        "scanned_roots",
        "skipped_roots",
        "skipped_findings",
        "scanned_paths",
        "skipped_paths",
    ):
        if key not in payload:
            continue
        value_obj = payload[key]
        if isinstance(value_obj, list):
            display = ", ".join(str(item) for item in value_obj if item not in (None, ""))
        else:
            display = str(value_obj)
        if not display:
            continue
        pairs.append((NOTE_LABELS.get(key, key.replace("_", " ").title()), display))
    return pairs
