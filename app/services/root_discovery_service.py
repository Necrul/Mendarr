from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from app.config import get_settings
from app.integrations.radarr_client import RadarrClient
from app.integrations.sonarr_client import SonarrClient
from app.logging import get_logger
from app.persistence.models import IntegrationConfig, LibraryRoot
from app.services.integration_service import reveal_integration_api_key

log = get_logger(__name__)


@dataclass(slots=True)
class RootCandidate:
    manager_kind: str
    manager_root_path: str
    local_root_path: str
    exists_locally: bool
    already_mapped: bool


WINDOWS_DRIVE_PATTERN = re.compile(r"^(?P<drive>[a-zA-Z]):[\\/]*(?P<rest>.*)$")
DEFAULT_SCAN_HINT_ROOTS: tuple[str, ...] = ()


def _split_config_list(raw: str) -> list[str]:
    values: list[str] = []
    for chunk in re.split(r"[;,\n]+", raw):
        value = chunk.strip()
        if value:
            values.append(value)
    return values


def _scan_hint_paths() -> list[Path]:
    hints: list[Path] = []
    seen: set[str] = set()
    configured = _split_config_list(get_settings().scan_path_hints)
    for raw in [*configured, *DEFAULT_SCAN_HINT_ROOTS]:
        candidate = Path(raw).expanduser()
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        key = str(resolved)
        if key in seen or not resolved.is_dir():
            continue
        seen.add(key)
        hints.append(resolved)
    return hints


def _path_mappings() -> list[tuple[str, str]]:
    mappings: list[tuple[str, str]] = []
    for entry in _split_config_list(get_settings().path_mappings):
        if "=>" in entry:
            source, target = entry.split("=>", 1)
        elif "=" in entry:
            source, target = entry.split("=", 1)
        else:
            continue
        source = source.strip()
        target = target.strip()
        if source and target:
            mappings.append((source, target))
    return mappings


def _normalize_compare_path(path_value: str) -> str:
    return path_value.strip().replace("\\", "/").rstrip("/").lower()


def _build_candidate_path(target_root: str, suffix: str) -> Path:
    candidate = Path(target_root).expanduser()
    if suffix:
        for part in suffix.split("/"):
            if part:
                candidate = candidate / part
    return candidate


def _candidate_from_path_mappings(path_value: str) -> str | None:
    normalized = _normalize_compare_path(path_value)
    if not normalized:
        return None

    for source, target in _path_mappings():
        normalized_source = _normalize_compare_path(source)
        if not normalized_source:
            continue
        if normalized == normalized_source:
            suffix = ""
        elif normalized.startswith(normalized_source + "/"):
            suffix = normalized[len(normalized_source) :].lstrip("/")
        else:
            continue
        return str(_build_candidate_path(target, suffix)).replace("\\", "/")
    return None


def resolve_local_scan_path(path_value: str, *, libraries_root: str = "/libraries") -> tuple[str, bool]:
    raw = path_value.strip()
    if not raw:
        return "", False

    try:
        direct_resolved = Path(raw).expanduser().resolve()
    except OSError:
        direct_resolved = None
    if direct_resolved and direct_resolved.exists() and direct_resolved.is_dir():
        return str(direct_resolved), True

    mapped_candidate = _candidate_from_path_mappings(raw)
    if mapped_candidate:
        try:
            mapped_resolved = Path(mapped_candidate).expanduser().resolve()
        except OSError:
            return mapped_candidate, False
        if mapped_resolved.exists() and mapped_resolved.is_dir():
            return str(mapped_resolved), True
        return mapped_candidate, False

    match = WINDOWS_DRIVE_PATTERN.match(raw)
    if match:
        drive = match.group("drive").lower()
        rest = match.group("rest").replace("\\", "/").strip("/")
        candidate = Path(libraries_root) / drive
        if rest:
            candidate = candidate / rest
        try:
            resolved = candidate.resolve()
        except OSError:
            return raw, False
        if resolved.exists() and resolved.is_dir():
            return str(resolved), True
        return raw, False

    try:
        resolved = Path(raw).expanduser().resolve()
    except OSError:
        return raw, False
    if resolved.exists() and resolved.is_dir():
        return str(resolved), True
    return raw, False


def _default_local_path(path_value: str) -> tuple[str, bool]:
    return resolve_local_scan_path(path_value)


async def discover_root_candidates(
    integration: IntegrationConfig | None,
    existing_roots: list[LibraryRoot],
) -> list[RootCandidate]:
    candidates, _error = await discover_root_candidates_with_status(integration, existing_roots)
    return candidates


async def discover_root_candidates_with_status(
    integration: IntegrationConfig | None,
    existing_roots: list[LibraryRoot],
) -> tuple[list[RootCandidate], str | None]:
    if not integration or not integration.enabled or not integration.base_url:
        return [], "Connection is not configured yet."

    try:
        api_key = reveal_integration_api_key(integration)
    except Exception:
        return [], "Stored API key could not be decrypted. Re-save the connection."
    if not api_key:
        return [], "API key is missing."

    mapped_paths = {
        (root.manager_kind.lower(), root.manager_root_path.strip().lower())
        for root in existing_roots
    }

    try:
        if integration.kind == "sonarr":
            rows = await SonarrClient(integration.base_url, api_key).root_folders()
        elif integration.kind == "radarr":
            rows = await RadarrClient(integration.base_url, api_key).root_folders()
        else:
            return [], "Unsupported integration type."
    except Exception as exc:
        log.info("root discovery failed for %s: %s", integration.kind, exc)
        return [], "Root folder lookup failed. Test the connection and check the app logs."

    candidates: list[RootCandidate] = []
    seen: set[str] = set()
    for row in rows:
        manager_root_path = str((row or {}).get("path") or "").strip()
        if not manager_root_path or manager_root_path.lower() in seen:
            continue
        seen.add(manager_root_path.lower())
        local_root_path, exists_locally = _default_local_path(manager_root_path)
        candidates.append(
            RootCandidate(
                manager_kind=integration.kind,
                manager_root_path=manager_root_path,
                local_root_path=local_root_path,
                exists_locally=exists_locally,
                already_mapped=(integration.kind, manager_root_path.lower()) in mapped_paths,
            )
        )
    return candidates, None
