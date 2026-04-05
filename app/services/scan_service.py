from __future__ import annotations

import asyncio
import datetime as dt
import fnmatch
import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.scan_notes import merge_scan_notes, parse_scan_notes
from app.domain.enums import (
    FindingStatus,
    IntegrationKind,
    ManagerKind,
    MediaKind,
    RemediationAction,
)
from app.domain.matching import (
    local_to_manager_relative,
    manager_path_to_local,
    normalize_title_token,
    parse_movie_from_path,
    parse_tv_from_path,
)
from app.domain.scoring import VIDEO_EXTENSIONS, score_finding
from app.integrations.ffprobe_adapter import _parse_streams, probe_file
from app.integrations.radarr_client import RadarrClient
from app.integrations.sonarr_client import SonarrClient
from app.logging import get_logger
from app.persistence.models import Finding, FindingReason, LibraryRoot, RuleException, ScanRun
from app.persistence.db import SessionLocal
from app.config import get_settings
from app.services.audit_service import log_event
from app.services.integration_service import get_integration, reveal_integration_api_key
from app.services.job_service import create_job
from app.services.match_service import load_root_pairs, match_movie_path, match_tv_path
from app.services.rule_service import (
    excluded_keywords_tuple_from_settings,
    extras_tuple_from_settings,
    get_or_create_rule_settings,
)

log = get_logger(__name__)

MIN_SCORE_TO_PERSIST = 8
PROGRESS_COMMIT_INTERVAL = 10
VERIFY_PROGRESS_COMMIT_INTERVAL = 1

_scan_task: asyncio.Task | None = None
_scan_task_lock: asyncio.Lock | None = None
_scan_stop_requested: asyncio.Event | None = None
_scan_runtime_loop: asyncio.AbstractEventLoop | None = None
_scan_run_id: int | None = None
_scan_scope: str | None = None


def _normalize_finding_ids(finding_ids: list[int] | list[str] | tuple[object, ...]) -> list[int]:
    return list(dict.fromkeys(int(fid) for fid in finding_ids if str(fid).isdigit()))


def _ensure_scan_runtime_primitives() -> tuple[asyncio.Lock, asyncio.Event]:
    global _scan_task, _scan_task_lock, _scan_stop_requested, _scan_runtime_loop, _scan_run_id, _scan_scope
    loop = asyncio.get_running_loop()
    if _scan_runtime_loop is not loop:
        _scan_runtime_loop = loop
        _scan_task = None
        _scan_task_lock = asyncio.Lock()
        _scan_stop_requested = asyncio.Event()
        _scan_run_id = None
        _scan_scope = None
    if _scan_task_lock is None:
        _scan_task_lock = asyncio.Lock()
    if _scan_stop_requested is None:
        _scan_stop_requested = asyncio.Event()
    return _scan_task_lock, _scan_stop_requested


def iter_video_files(root: Path):
    # Follow linked directories as some library layouts expose media through
    # junctions/symlinks instead of storing files directly under the root.
    seen_dirs: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=True):
        dir_path = Path(dirpath)
        try:
            real_dir = str(dir_path.resolve())
        except OSError:
            real_dir = os.path.realpath(dirpath)

        if real_dir in seen_dirs:
            dirnames[:] = []
            continue
        seen_dirs.add(real_dir)

        pruned_dirnames: list[str] = []
        for dirname in dirnames:
            child = dir_path / dirname
            try:
                real_child = str(child.resolve())
            except OSError:
                pruned_dirnames.append(dirname)
                continue
            if real_child in seen_dirs:
                continue
            pruned_dirnames.append(dirname)
        dirnames[:] = pruned_dirnames

        for fn in filenames:
            p = Path(dirpath) / fn
            if p.suffix.lower() in VIDEO_EXTENSIONS:
                yield p


def _library_root_media_kind(manager_kind: str) -> MediaKind:
    normalized = manager_kind.lower()
    if normalized in {"sonarr", "tv"}:
        return MediaKind.TV
    if normalized in {"radarr", "movie"}:
        return MediaKind.MOVIE
    return MediaKind.UNKNOWN

def _collect_scannable_roots(root_specs: list[tuple[str, str]]) -> tuple[list[str], list[str], list[tuple[str, MediaKind]]]:
    scanned_roots: list[str] = []
    skipped_roots: list[str] = []
    scannable_roots: list[tuple[str, MediaKind]] = []
    for local_root_path, manager_kind in root_specs:
        root = Path(local_root_path)
        if not root.is_dir():
            skipped_roots.append(local_root_path)
            continue
        scanned_roots.append(local_root_path)
        scannable_roots.append((local_root_path, _library_root_media_kind(manager_kind)))
    return scanned_roots, skipped_roots, scannable_roots


def _count_scan_paths(root_specs: list[tuple[str, MediaKind]]) -> int:
    total = 0
    for local_root_path, _media_kind in root_specs:
        for _ in iter_video_files(Path(local_root_path)):
            total += 1
    return total


def _resume_checkpoint_offset(root_specs: list[tuple[str, MediaKind]], resume_after_file: str) -> int | None:
    target = os.path.normcase(os.path.normpath(resume_after_file))
    skipped = 0
    for abs_path, _media_kind, _root_path in iter_scan_paths(root_specs):
        skipped += 1
        if os.path.normcase(os.path.normpath(abs_path)) == target:
            return skipped
    return None


def iter_scan_paths(root_specs: list[tuple[str, MediaKind]]):
    for local_root_path, media_kind in root_specs:
        root = Path(local_root_path)
        for file_path in iter_video_files(root):
            yield str(file_path.resolve()), media_kind, local_root_path


def local_under_roots(file_path: str, root_pairs: list[tuple[str, str]]) -> bool:
    return local_to_manager_relative(file_path, root_pairs) is not None


def _log_stat_failure(abs_path: str, error: OSError, *, verify: bool = False) -> None:
    if isinstance(error, FileNotFoundError):
        if verify:
            log.debug("verify skipped missing file %s", abs_path)
        else:
            log.debug("scan skipped missing file %s", abs_path)
        return
    if verify:
        log.warning("verify stat failed %s: %s", abs_path, error)
    else:
        log.warning("stat failed %s: %s", abs_path, error)


async def _active_exceptions(session: AsyncSession) -> list[RuleException]:
    r = await session.execute(select(RuleException).where(RuleException.enabled.is_(True)))
    return list(r.scalars().all())


@dataclass(frozen=True)
class RuleExceptionSnapshot:
    path_pattern: str | None
    title_pattern: str | None
    manager_kind: str | None
    media_kind: str | None
    action_override: str | None
    ignore_flag: bool


async def _active_exception_snapshots(session: AsyncSession) -> list[RuleExceptionSnapshot]:
    rows = await _active_exceptions(session)
    return [
        RuleExceptionSnapshot(
            path_pattern=row.path_pattern,
            title_pattern=row.title_pattern,
            manager_kind=row.manager_kind,
            media_kind=row.media_kind,
            action_override=row.action_override,
            ignore_flag=row.ignore_flag,
        )
        for row in rows
    ]


async def latest_resumable_library_scan(session: AsyncSession) -> ScanRun | None:
    runs = (
        await session.execute(
            select(ScanRun).where(ScanRun.status == "interrupted").order_by(ScanRun.started_at.desc(), ScanRun.id.desc())
        )
    ).scalars().all()
    for run in runs:
        notes = parse_scan_notes(run.notes)
        if notes.get("scope") == "verify":
            continue
        if notes.get("resume_after_file"):
            return run
    return None


def _matching_rule_exception(
    file_path: str,
    title: str | None,
    manager_kind: str | None,
    media_kind: str,
    exceptions: list[RuleExceptionSnapshot],
) -> RuleExceptionSnapshot | None:
    norm = file_path.replace("\\", "/")
    for ex in exceptions:
        if ex.manager_kind and ex.manager_kind.lower() != (manager_kind or "").lower():
            continue
        if ex.media_kind and ex.media_kind.lower() != media_kind.lower():
            continue

        matched = False
        if ex.path_pattern:
            pat = ex.path_pattern.strip()
            if pat and (pat in norm or fnmatch.fnmatch(norm, pat)):
                matched = True
        if ex.title_pattern and title:
            if ex.title_pattern.lower() in title.lower():
                matched = True
        if matched:
            return ex
    return None


def _probe_metadata(ffprobe_json: dict | None):
    duration = res = cv = ca = None
    if not ffprobe_json:
        return duration, res, cv, ca
    pr = _parse_streams(ffprobe_json)
    duration = pr.duration_seconds
    if pr.width and pr.height:
        res = f"{pr.width}x{pr.height}"
    cv = pr.video_codec
    ca = ",".join(pr.audio_codecs) if pr.audio_codecs else None
    return duration, res, cv, ca


def _compact_ffprobe_json(ffprobe_json: dict | None) -> dict | None:
    if not ffprobe_json:
        return None

    compact_streams: list[dict[str, object]] = []
    for stream in ffprobe_json.get("streams") or []:
        compact_stream: dict[str, object] = {}
        for key in ("codec_type", "codec_name", "width", "height"):
            value = stream.get(key)
            if value not in (None, ""):
                compact_stream[key] = value
        if compact_stream:
            compact_streams.append(compact_stream)

    compact_format: dict[str, object] = {}
    fmt = ffprobe_json.get("format") or {}
    for key in ("duration", "bit_rate"):
        value = fmt.get(key)
        if value not in (None, ""):
            compact_format[key] = value

    compact: dict[str, object] = {}
    if compact_streams:
        compact["streams"] = compact_streams
    if compact_format:
        compact["format"] = compact_format
    return compact or None


def _compact_sonarr_series_rows(rows: list[dict]) -> list[dict]:
    compact_rows: list[dict] = []
    for row in rows:
        alternate_titles = []
        for alt in row.get("alternateTitles") or []:
            title = alt.get("title") if isinstance(alt, dict) else None
            if title:
                alternate_titles.append({"title": title})
        compact_rows.append(
            {
                "id": row.get("id"),
                "title": row.get("title"),
                "alternateTitles": alternate_titles,
            }
        )
    return compact_rows


def _compact_radarr_movie_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            "id": row.get("id"),
            "title": row.get("title"),
            "year": row.get("year"),
        }
        for row in rows
    ]


def _sibling_video_files(path_obj: Path) -> list[Path] | None:
    siblings: list[Path] = []
    try:
        for child in path_obj.parent.iterdir():
            if child.is_file() and child.suffix.lower() in VIDEO_EXTENSIONS:
                siblings.append(child)
    except OSError:
        return None
    return siblings


async def upsert_finding(
    session: AsyncSession,
    run: ScanRun,
    abs_path: str,
    path_obj: Path,
    mkind: MediaKind,
    mo,
    scored,
    ffprobe_json: dict | None,
    *,
    actor: str | None = None,
    ignored: bool = False,
    action_override: str | None = None,
) -> None:
    r = await session.execute(select(Finding).where(Finding.file_path == abs_path))
    row = r.scalar_one_or_none()
    now = dt.datetime.now(dt.UTC)
    title = season = episode = year = None
    mgr_kind = mgr_entity = None
    if mo:
        title = mo.title
        season = mo.season_number
        episode = mo.episode_number
        year = mo.year
        mgr_kind = mo.manager_kind.value if mo.manager_kind != ManagerKind.NONE else None
        mgr_entity = mo.manager_entity_id

    duration, res, cv, ca = _probe_metadata(ffprobe_json)
    try:
        fsize = path_obj.stat().st_size if path_obj.exists() else 0
    except OSError:
        fsize = 0

    if row:
        row.file_name = path_obj.name
        row.media_kind = mkind.value
        row.manager_kind = mgr_kind
        row.manager_entity_id = mgr_entity
        row.title = title
        row.season_number = season
        row.episode_number = episode
        row.year = year
        row.file_size_bytes = fsize
        row.duration_seconds = duration
        row.resolution = res
        row.codec_video = cv
        row.codec_audio = ca
        row.suspicion_score = scored.score
        row.confidence = scored.confidence.value
        row.proposed_action = action_override or scored.proposed_action.value
        row.ignored = ignored
        if ignored:
            row.status = FindingStatus.IGNORED.value
        elif row.status == FindingStatus.UNRESOLVED.value:
            row.status = FindingStatus.UNRESOLVED.value
        else:
            row.status = FindingStatus.OPEN.value
        row.last_seen_at = now
        row.last_scanned_at = now
        row.last_scan_run_id = run.id
        row.ffprobe_json = ffprobe_json
        await session.execute(delete(FindingReason).where(FindingReason.finding_id == row.id))
        for reason in scored.reasons:
            session.add(
                FindingReason(
                    finding_id=row.id,
                    code=reason.code,
                    message=reason.message,
                    severity=reason.severity,
                )
            )
        await log_event(
            session,
            event_type="finding_updated",
            entity_type="finding",
            entity_id=str(row.id),
            message=f"Updated finding for {path_obj.name}",
            metadata={"score": scored.score, "ignored": ignored},
            actor=actor,
        )
        return

    row = Finding(
        file_path=abs_path,
        file_name=path_obj.name,
        media_kind=mkind.value,
        manager_kind=mgr_kind,
        manager_entity_id=mgr_entity,
        title=title,
        season_number=season,
        episode_number=episode,
        year=year,
        file_size_bytes=fsize,
        duration_seconds=duration,
        resolution=res,
        codec_video=cv,
        codec_audio=ca,
        suspicion_score=scored.score,
        confidence=scored.confidence.value,
        proposed_action=action_override or scored.proposed_action.value,
        status=FindingStatus.IGNORED.value if ignored else FindingStatus.OPEN.value,
        ignored=ignored,
        first_seen_at=now,
        last_seen_at=now,
        last_scanned_at=now,
        last_scan_run_id=run.id,
        ffprobe_json=ffprobe_json,
    )
    session.add(row)
    await session.flush()
    for reason in scored.reasons:
        session.add(
            FindingReason(
                finding_id=row.id,
                code=reason.code,
                message=reason.message,
                severity=reason.severity,
            )
        )
    await log_event(
        session,
        event_type="finding_created",
        entity_type="finding",
        entity_id=str(row.id),
        message=f"Created finding for {path_obj.name}",
        metadata={"score": scored.score, "ignored": ignored},
        actor=actor,
    )


async def _commit_scan_progress(session: AsyncSession, run_id: int) -> ScanRun:
    await session.commit()
    session.expunge_all()
    refreshed = await session.get(ScanRun, run_id)
    if refreshed is None:
        raise RuntimeError(f"scan run {run_id} disappeared during commit")
    return refreshed


async def _perform_scan(
    session: AsyncSession,
    run: ScanRun,
    *,
    actor: str | None = None,
    emit_started_event: bool = True,
    commit_progress: bool = False,
    resume_after_file: str | None = None,
    resume_from_scan_id: int | None = None,
) -> ScanRun:
    settings = get_settings()
    rules = await get_or_create_rule_settings(session)
    sonarr = await get_integration(session, IntegrationKind.SONARR)
    radarr = await get_integration(session, IntegrationKind.RADARR)
    sonarr_api_key = reveal_integration_api_key(sonarr)
    radarr_api_key = reveal_integration_api_key(radarr)
    excluded_keywords = excluded_keywords_tuple_from_settings(rules)
    progress_state = [0]
    progress_context = {"current_library": None, "total_files": None}

    def update_run_notes(**updates) -> None:
        run.notes = merge_scan_notes(run.notes, **updates)

    if emit_started_event:
        message = "Library scan started"
        metadata = None
        if resume_after_file:
            message = "Library scan resumed"
            metadata = {"scope": "library", "resumed_from_scan_id": resume_from_scan_id}
        await log_event(
            session,
            event_type="scan_started",
            entity_type="scan_run",
            message=message,
            entity_id=str(run.id),
            metadata=metadata,
            actor=actor,
        )
    log.info("scan %s started by %s", run.id, actor or "system")

    pairs = await load_root_pairs(session)
    exc_rows = await _active_exception_snapshots(session)
    sonarr_series_cache = None
    radarr_movies_cache = None
    run_id = run.id

    async def interrupt_scan(message: str) -> ScanRun:
        nonlocal run
        notes = parse_scan_notes(run.notes)
        run.status = "interrupted"
        run.completed_at = dt.datetime.now(dt.UTC)
        run.notes = merge_scan_notes(
            run.notes,
            phase="interrupted",
            resume_after_file=notes.get("current_file"),
            current_file=None,
            current_library=None,
            error=message,
        )
        await log_event(
            session,
            event_type="scan_interrupted",
            entity_type="scan_run",
            entity_id=str(run.id),
            message=message,
            metadata={"scope": "library"},
            actor=actor,
        )
        log.info("scan %s interrupted: %s", run.id, message)
        return run

    async def process_one(abs_path: str, media_guess: MediaKind) -> None:
        nonlocal run, sonarr_series_cache, radarr_movies_cache
        path_obj = Path(abs_path)
        try:
            st = path_obj.stat()
        except OSError as e:
            _log_stat_failure(abs_path, e)
            return
        size = st.st_size

        probe = await probe_file(abs_path)

        mkind = media_guess
        if mkind == MediaKind.UNKNOWN:
            if pairs["sonarr"] and local_under_roots(abs_path, pairs["sonarr"]):
                mkind = MediaKind.TV
            elif pairs["radarr"] and local_under_roots(abs_path, pairs["radarr"]):
                mkind = MediaKind.MOVIE

        extras = extras_tuple_from_settings(rules)
        siblings = _sibling_video_files(path_obj)

        mk_for_score = mkind if mkind != MediaKind.UNKNOWN else MediaKind.MOVIE
        scored = score_finding(
            file_path=abs_path,
            media_kind=mk_for_score,
            size_bytes=size,
            probe=probe,
            min_tv_size_bytes=rules.min_tv_size_bytes,
            min_movie_size_bytes=rules.min_movie_size_bytes,
            min_duration_tv=rules.min_duration_tv_seconds,
            min_duration_movie=rules.min_duration_movie_seconds,
            extras_keywords=extras,
            excluded_keywords=excluded_keywords,
            excluded_path_lines=rules.excluded_paths,
            ignored_pattern_lines=rules.ignored_patterns,
            siblings=siblings,
            has_manager_match=False,
            auto_remediation=rules.auto_remediation_enabled,
        )

        run.files_seen += 1
        progress_state[0] += 1
        update_run_notes(
            phase="running",
            current_library=progress_context["current_library"],
            current_file=abs_path,
            files_seen=run.files_seen,
            total_files=progress_context["total_files"],
            findings=run.suspicious_found,
        )
        if commit_progress and progress_state[0] >= PROGRESS_COMMIT_INTERVAL:
            run = await _commit_scan_progress(session, run_id)
            progress_state[0] = 0
        if scored.score < MIN_SCORE_TO_PERSIST:
            return

        mo = None
        has_mgr = False
        try:
            if (
                mkind == MediaKind.TV
                and sonarr
                and sonarr.enabled
                and sonarr.base_url
                and sonarr_api_key
            ):
                if sonarr_series_cache is None:
                    sonarr_series_cache = _compact_sonarr_series_rows(
                        await SonarrClient(sonarr.base_url, sonarr_api_key).all_series()
                    )
                mo = await match_tv_path(
                    session,
                    abs_path,
                    sonarr.base_url,
                    sonarr_api_key,
                    pairs=pairs["sonarr"],
                    series_list=sonarr_series_cache,
                )
                has_mgr = mo.manager_kind == ManagerKind.SONARR
            elif (
                mkind == MediaKind.MOVIE
                and radarr
                and radarr.enabled
                and radarr.base_url
                and radarr_api_key
            ):
                if radarr_movies_cache is None:
                    radarr_movies_cache = _compact_radarr_movie_rows(
                        await RadarrClient(radarr.base_url, radarr_api_key).all_movies()
                    )
                mo = await match_movie_path(
                    session,
                    abs_path,
                    radarr.base_url,
                    radarr_api_key,
                    pairs=pairs["radarr"],
                    movies=radarr_movies_cache,
                )
                has_mgr = mo.manager_kind == ManagerKind.RADARR
        except Exception as ex:
            log.debug("match failed: %s", ex)

        if has_mgr:
            scored = score_finding(
                file_path=abs_path,
                media_kind=mk_for_score,
                size_bytes=size,
                probe=probe,
                min_tv_size_bytes=rules.min_tv_size_bytes,
                min_movie_size_bytes=rules.min_movie_size_bytes,
                min_duration_tv=rules.min_duration_tv_seconds,
                min_duration_movie=rules.min_duration_movie_seconds,
                extras_keywords=extras,
                excluded_keywords=excluded_keywords,
                excluded_path_lines=rules.excluded_paths,
                ignored_pattern_lines=rules.ignored_patterns,
                siblings=siblings,
                has_manager_match=True,
                auto_remediation=rules.auto_remediation_enabled,
            )

        title_guess = mo.title if mo else None

        matching_exception = _matching_rule_exception(
            abs_path,
            title_guess,
            mo.manager_kind.value if mo else None,
            mk_for_score.value,
            exc_rows,
        )
        ignored = bool(matching_exception and matching_exception.ignore_flag)
        action_override = None
        if matching_exception and matching_exception.action_override:
            action_override = matching_exception.action_override

        run.suspicious_found += 1
        update_run_notes(
            phase="running",
            current_library=progress_context["current_library"],
            current_file=abs_path,
            files_seen=run.files_seen,
            total_files=progress_context["total_files"],
            findings=run.suspicious_found,
        )
        probe_raw = _compact_ffprobe_json(probe.raw if probe and probe.ok else None)
        await upsert_finding(
            session,
            run,
            abs_path,
            path_obj,
            mkind,
            mo,
            scored,
            probe_raw,
            actor=actor,
            ignored=ignored,
            action_override=action_override,
        )

        resolved_action = action_override or scored.proposed_action.value
        if (
            rules.auto_remediation_enabled
            and not ignored
            and scored.confidence.value == "high"
            and has_mgr
            and resolved_action in {RemediationAction.RESCAN_ONLY.value, RemediationAction.SEARCH_REPLACEMENT.value}
        ):
            finding_row = (
                await session.execute(select(Finding).where(Finding.file_path == abs_path).limit(1))
            ).scalar_one_or_none()
            if finding_row:
                await create_job(
                    session,
                    finding_id=finding_row.id,
                    action=RemediationAction(resolved_action),
                    requested_by="auto",
                    actor=actor,
                )
        if commit_progress:
            run = await _commit_scan_progress(session, run_id)
            progress_state[0] = 0

    r = await session.execute(select(LibraryRoot).where(LibraryRoot.enabled.is_(True)))
    lib_roots = list(r.scalars().all())
    root_specs = [(lr.local_root_path, lr.manager_kind) for lr in lib_roots]
    scanned_roots, skipped_roots, scannable_roots = _collect_scannable_roots(root_specs)
    total_files = None
    if settings.scan_precount_enabled:
        total_files = await asyncio.to_thread(_count_scan_paths, scannable_roots)
    resume_skip_count = 0
    if resume_after_file:
        resume_skip_count = await asyncio.to_thread(_resume_checkpoint_offset, scannable_roots, resume_after_file) or 0
        if resume_skip_count == 0:
            log.warning("scan %s resume checkpoint not found, restarting from beginning", run.id)
            run.files_seen = 0
            run.suspicious_found = 0
            run.notes = merge_scan_notes(
                run.notes,
                resumed_from_scan_id=resume_from_scan_id,
                resume_after_file=resume_after_file,
                resume_status="checkpoint_missing",
            )
        else:
            run.notes = merge_scan_notes(
                run.notes,
                resumed_from_scan_id=resume_from_scan_id,
                resume_after_file=resume_after_file,
                resume_status="applied",
            )

    progress_context["total_files"] = total_files
    update_run_notes(
        phase="running",
        current_library=scanned_roots[0] if scanned_roots else None,
        current_file=None,
        libraries=len(lib_roots),
        scanned_roots=len(scanned_roots),
        skipped_roots=len(skipped_roots),
        scanned_paths=scanned_roots[:6],
        skipped_paths=skipped_roots[:6],
        files_seen=run.files_seen,
        total_files=total_files,
        findings=run.suspicious_found,
    )
    if commit_progress:
        run = await _commit_scan_progress(session, run_id)

    _, scan_stop_event = _ensure_scan_runtime_primitives()
    skipped_for_resume = 0
    for abs_path, mg, root_path in iter_scan_paths(scannable_roots):
        if skipped_for_resume < resume_skip_count:
            skipped_for_resume += 1
            continue
        if scan_stop_event.is_set():
            return await interrupt_scan("Library scan interrupted by operator request")
        progress_context["current_library"] = root_path
        await process_one(abs_path, mg)

    if scan_stop_event.is_set():
        return await interrupt_scan("Library scan interrupted by operator request")

    final_total_files = total_files if total_files is not None else run.files_seen
    run.status = "completed"
    run.completed_at = dt.datetime.now(dt.UTC)
    run.notes = merge_scan_notes(
        run.notes,
        phase="completed",
        current_file=None,
        current_library=None,
        libraries=len(lib_roots),
        scanned_roots=len(scanned_roots),
        skipped_roots=len(skipped_roots),
        files_seen=run.files_seen,
        total_files=final_total_files,
        findings=run.suspicious_found,
        scanned_paths=scanned_roots[:6],
        skipped_paths=skipped_roots[:6],
    )
    await log_event(
        session,
        event_type="scan_completed",
        entity_type="scan_run",
        message=f"Scan finished: {run.files_seen} files, {run.suspicious_found} suspicious",
        entity_id=str(run.id),
        metadata={
            "libraries": len(lib_roots),
            "scanned_roots": scanned_roots[:20],
            "skipped_roots": skipped_roots[:20],
        },
        actor=actor,
    )
    log.info(
        "scan %s completed: files=%s findings=%s scanned_roots=%s skipped_roots=%s",
        run.id,
        run.files_seen,
        run.suspicious_found,
        len(scanned_roots),
        len(skipped_roots),
    )
    return run


async def _resolve_sonarr_verify_path(client: SonarrClient, finding: Finding) -> str | None:
    from app.services.match_service import parse_sonarr_entity_id

    entity_kind, entity_value = parse_sonarr_entity_id(finding.manager_entity_id)
    if entity_value is None:
        return None
    if entity_kind == "episode":
        episode = await client.get_episode_by_id(entity_value)
    else:
        season_number = finding.season_number
        episode_number = finding.episode_number
        if season_number is None or episode_number is None:
            parsed = parse_tv_from_path(Path(finding.file_name or finding.file_path))
            season_number = season_number if season_number is not None else parsed.season
            episode_number = episode_number if episode_number is not None else parsed.episode
        if season_number is None or episode_number is None:
            return None
        episodes = await client.episodes_for_series(entity_value)
        episode = next(
            (
                row
                for row in episodes
                if row.get("seasonNumber") == season_number
                and row.get("episodeNumber") == episode_number
            ),
            None,
        )
        if not episode:
            return None
    episode_file_id = episode.get("episodeFileId") if episode else None
    if not episode_file_id:
        return None
    episode_file = await client.get_episode_file(int(episode_file_id))
    return episode_file.get("path")


async def _resolve_radarr_verify_path(client: RadarrClient, finding: Finding) -> str | None:
    if not finding.manager_entity_id or not str(finding.manager_entity_id).isdigit():
        return None
    movie = await client.get_movie(int(finding.manager_entity_id))
    movie_file = movie.get("movieFile") or {}
    if movie_file.get("path"):
        return movie_file.get("path")
    movie_file_id = movie.get("movieFileId") or movie_file.get("id")
    if not movie_file_id:
        return None
    movie_file_row = await client.get_movie_file(int(movie_file_id))
    return movie_file_row.get("path")


async def _verification_target_path(
    session: AsyncSession,
    finding: Finding,
    *,
    sonarr,
    radarr,
    sonarr_api_key: str | None,
    radarr_api_key: str | None,
    pairs: dict[str, list[tuple[str, str]]],
) -> str | None:
    from app.services.match_service import relink_finding

    def find_local_replacement_candidate() -> str | None:
        source_path = Path(finding.file_path)
        candidate_dirs: list[Path] = []
        for candidate_dir in (source_path.parent, source_path.parent.parent):
            if candidate_dir.exists() and candidate_dir not in candidate_dirs:
                candidate_dirs.append(candidate_dir)

        if not candidate_dirs:
            return None

        source_name = source_path.name
        source_tv = parse_tv_from_path(source_path)
        source_movie = parse_movie_from_path(source_path)
        source_show = normalize_title_token(source_tv.show_hint or finding.title or "")
        source_movie_title = normalize_title_token(source_movie.title_hint or finding.title or "")

        def iter_candidates(parent: Path):
            try:
                for child in parent.iterdir():
                    if child.is_file() and child.suffix.lower() in VIDEO_EXTENSIONS:
                        yield child
            except OSError:
                return

        if finding.media_kind == MediaKind.TV.value:
            season_number = finding.season_number if finding.season_number is not None else source_tv.season
            episode_number = finding.episode_number if finding.episode_number is not None else source_tv.episode
            for parent in candidate_dirs:
                for child in iter_candidates(parent):
                    if child.name == source_name:
                        continue
                    parsed = parse_tv_from_path(child)
                    if parsed.season != season_number or parsed.episode != episode_number:
                        continue
                    child_show = normalize_title_token(parsed.show_hint or "")
                    if source_show and child_show and source_show != child_show:
                        continue
                    return str(child.resolve())

        if finding.media_kind == MediaKind.MOVIE.value:
            target_year = finding.year if finding.year is not None else source_movie.year
            for parent in candidate_dirs:
                for child in iter_candidates(parent):
                    if child.name == source_name:
                        continue
                    parsed = parse_movie_from_path(child)
                    child_title = normalize_title_token(parsed.title_hint or "")
                    if source_movie_title and child_title and source_movie_title != child_title:
                        continue
                    if target_year and parsed.year and parsed.year != target_year:
                        continue
                    return str(child.resolve())

        return None

    try:
        refreshed = await relink_finding(session, finding)
        if refreshed.manager_kind != ManagerKind.NONE and refreshed.manager_entity_id:
            finding.manager_kind = refreshed.manager_kind.value
            finding.manager_entity_id = refreshed.manager_entity_id
            finding.title = refreshed.title or finding.title
            finding.season_number = refreshed.season_number
            finding.episode_number = refreshed.episode_number
            finding.year = refreshed.year
    except Exception as ex:
        log.debug("verify relink failed for finding %s: %s", finding.id, ex)

    def resolve_visible_path(candidate: str | None, root_pairs: list[tuple[str, str]]) -> str | None:
        if not candidate:
            return None
        path_candidate = Path(candidate)
        if path_candidate.is_file():
            return str(path_candidate.resolve())
        translated = manager_path_to_local(candidate, root_pairs)
        if translated:
            translated_path = Path(translated)
            if translated_path.is_file():
                return str(translated_path.resolve())

        source_path = Path(finding.file_path)
        candidate_name = path_candidate.name
        candidate_suffix = path_candidate.suffix.lower()
        candidate_dir_name = path_candidate.parent.name.casefold()

        parent_candidates: list[Path] = []
        if source_path.parent.exists():
            parent_candidates.append(source_path.parent)
        if source_path.parent.parent.exists() and source_path.parent.parent not in parent_candidates:
            parent_candidates.append(source_path.parent.parent)

        for parent in parent_candidates:
            same_dir_candidate = parent / candidate_name
            if same_dir_candidate.is_file():
                return str(same_dir_candidate.resolve())

        for parent in parent_candidates:
            try:
                for child in parent.iterdir():
                    if not child.is_file():
                        continue
                    if child.suffix.lower() != candidate_suffix:
                        continue
                    if child.name == source_path.name:
                        continue
                    if child.name == candidate_name:
                        return str(child.resolve())
            except OSError:
                continue

        for parent in parent_candidates[1:]:
            try:
                for child in parent.rglob(candidate_name):
                    if child.is_file():
                        return str(child.resolve())
            except OSError:
                continue

        if source_path.parent.exists() and source_path.parent.name.casefold() == candidate_dir_name:
            try:
                for child in source_path.parent.iterdir():
                    if child.is_file() and child.suffix.lower() == candidate_suffix and child.name != source_path.name:
                        return str(child.resolve())
            except OSError:
                pass
        return None

    try:
        if finding.manager_kind == ManagerKind.SONARR.value and sonarr and sonarr.enabled and sonarr.base_url and sonarr_api_key:
            resolved = await _resolve_sonarr_verify_path(
                SonarrClient(sonarr.base_url, sonarr_api_key),
                finding,
            )
            visible = resolve_visible_path(resolved, pairs.get("sonarr", []))
            if visible:
                return visible
        if finding.manager_kind == ManagerKind.RADARR.value and radarr and radarr.enabled and radarr.base_url and radarr_api_key:
            resolved = await _resolve_radarr_verify_path(
                RadarrClient(radarr.base_url, radarr_api_key),
                finding,
            )
            visible = resolve_visible_path(resolved, pairs.get("radarr", []))
            if visible:
                return visible
    except Exception as ex:
        log.debug("verify path resolution failed for finding %s: %s", finding.id, ex)
    local_candidate = find_local_replacement_candidate()
    if local_candidate:
        return local_candidate
    source_path = Path(finding.file_path)
    if source_path.is_file():
        return str(source_path.resolve())
    return None


async def _mark_finding_resolved(
    session: AsyncSession,
    finding: Finding,
    run: ScanRun,
    *,
    actor: str | None,
    message: str,
) -> None:
    finding.status = FindingStatus.RESOLVED.value
    finding.last_scanned_at = dt.datetime.now(dt.UTC)
    finding.last_scan_run_id = run.id
    await log_event(
        session,
        event_type="finding_resolved",
        entity_type="finding",
        entity_id=str(finding.id),
        message=message,
        actor=actor,
    )


async def _scan_verify_target(
    session: AsyncSession,
    run: ScanRun,
    *,
    abs_path: str,
    media_guess: MediaKind,
    actor: str | None,
    rules,
    sonarr,
    radarr,
    sonarr_api_key: str | None,
    radarr_api_key: str | None,
    pairs: dict[str, list[tuple[str, str]]],
    exc_rows: list[RuleException],
    excluded_keywords: tuple[str, ...],
    extras: tuple[str, ...],
    caches: dict[str, list[dict] | None],
) -> dict[str, object]:
    path_obj = Path(abs_path)
    try:
        st = path_obj.stat()
    except OSError as e:
        _log_stat_failure(abs_path, e, verify=True)
        return {"persisted": False, "score": 0}

    size = st.st_size
    probe = await probe_file(abs_path)

    mkind = media_guess
    if mkind == MediaKind.UNKNOWN:
        if pairs["sonarr"] and local_under_roots(abs_path, pairs["sonarr"]):
            mkind = MediaKind.TV
        elif pairs["radarr"] and local_under_roots(abs_path, pairs["radarr"]):
            mkind = MediaKind.MOVIE

    siblings = _sibling_video_files(path_obj)

    mk_for_score = mkind if mkind != MediaKind.UNKNOWN else MediaKind.MOVIE
    scored = score_finding(
        file_path=abs_path,
        media_kind=mk_for_score,
        size_bytes=size,
        probe=probe,
        min_tv_size_bytes=rules.min_tv_size_bytes,
        min_movie_size_bytes=rules.min_movie_size_bytes,
        min_duration_tv=rules.min_duration_tv_seconds,
        min_duration_movie=rules.min_duration_movie_seconds,
        extras_keywords=extras,
        excluded_keywords=excluded_keywords,
        excluded_path_lines=rules.excluded_paths,
        ignored_pattern_lines=rules.ignored_patterns,
        siblings=siblings,
        has_manager_match=False,
        auto_remediation=rules.auto_remediation_enabled,
    )
    run.files_seen += 1
    if scored.score < MIN_SCORE_TO_PERSIST:
        return {"persisted": False, "score": scored.score, "path": abs_path}

    mo = None
    has_mgr = False
    try:
        if (
            mkind == MediaKind.TV
            and sonarr
            and sonarr.enabled
            and sonarr.base_url
            and sonarr_api_key
        ):
            if caches["sonarr_series"] is None:
                caches["sonarr_series"] = _compact_sonarr_series_rows(
                    await SonarrClient(sonarr.base_url, sonarr_api_key).all_series()
                )
            mo = await match_tv_path(
                session,
                abs_path,
                sonarr.base_url,
                sonarr_api_key,
                pairs=pairs["sonarr"],
                series_list=caches["sonarr_series"],
            )
            has_mgr = mo.manager_kind == ManagerKind.SONARR
        elif (
            mkind == MediaKind.MOVIE
            and radarr
            and radarr.enabled
            and radarr.base_url
            and radarr_api_key
        ):
            if caches["radarr_movies"] is None:
                caches["radarr_movies"] = _compact_radarr_movie_rows(
                    await RadarrClient(radarr.base_url, radarr_api_key).all_movies()
                )
            mo = await match_movie_path(
                session,
                abs_path,
                radarr.base_url,
                radarr_api_key,
                pairs=pairs["radarr"],
                movies=caches["radarr_movies"],
            )
            has_mgr = mo.manager_kind == ManagerKind.RADARR
    except Exception as ex:
        log.debug("verify match failed: %s", ex)

    if has_mgr:
        scored = score_finding(
            file_path=abs_path,
            media_kind=mk_for_score,
            size_bytes=size,
            probe=probe,
            min_tv_size_bytes=rules.min_tv_size_bytes,
            min_movie_size_bytes=rules.min_movie_size_bytes,
            min_duration_tv=rules.min_duration_tv_seconds,
            min_duration_movie=rules.min_duration_movie_seconds,
            extras_keywords=extras,
            excluded_keywords=excluded_keywords,
            excluded_path_lines=rules.excluded_paths,
            ignored_pattern_lines=rules.ignored_patterns,
            siblings=siblings,
            has_manager_match=True,
            auto_remediation=rules.auto_remediation_enabled,
        )

    title_guess = mo.title if mo else None
    matching_exception = _matching_rule_exception(
        abs_path,
        title_guess,
        mo.manager_kind.value if mo else None,
        mk_for_score.value,
        exc_rows,
    )
    ignored = bool(matching_exception and matching_exception.ignore_flag)
    action_override = matching_exception.action_override if matching_exception and matching_exception.action_override else None

    run.suspicious_found += 1
    await upsert_finding(
        session,
        run,
        abs_path,
        path_obj,
        mkind,
        mo,
        scored,
        _compact_ffprobe_json(probe.raw if probe and probe.ok else None),
        actor=actor,
        ignored=ignored,
        action_override=action_override,
    )
    return {"persisted": True, "score": scored.score, "path": abs_path}


async def _perform_verify_scan(
    session: AsyncSession,
    run: ScanRun,
    finding_ids: list[int],
    *,
    actor: str | None = None,
    emit_started_event: bool = True,
    commit_progress: bool = False,
) -> ScanRun:
    rules = await get_or_create_rule_settings(session)
    sonarr = await get_integration(session, IntegrationKind.SONARR)
    radarr = await get_integration(session, IntegrationKind.RADARR)
    sonarr_api_key = reveal_integration_api_key(sonarr)
    radarr_api_key = reveal_integration_api_key(radarr)
    excluded_keywords = excluded_keywords_tuple_from_settings(rules)
    extras = extras_tuple_from_settings(rules)
    pairs = await load_root_pairs(session)
    exc_rows = await _active_exception_snapshots(session)
    caches: dict[str, list[dict] | None] = {"sonarr_series": None, "radarr_movies": None}
    run_id = run.id
    target_count = len(finding_ids)

    if emit_started_event:
        await log_event(
            session,
            event_type="scan_started",
            entity_type="scan_run",
            message=f"Verify scan started for {target_count} finding(s)",
            entity_id=str(run.id),
            metadata={"scope": "verify", "finding_ids": finding_ids[:50]},
            actor=actor,
        )

    skipped: list[int] = []
    run.notes = merge_scan_notes(
        run.notes,
        phase="running",
        scope="verify",
        target_count=target_count,
        files_seen=run.files_seen,
        total_files=target_count,
        findings=run.suspicious_found,
        skipped_findings=skipped[:20],
    )
    if commit_progress:
        run = await _commit_scan_progress(session, run_id)

    progress_counter = 0
    resolved_count = 0
    moved_count = 0
    for finding_id in finding_ids:
        source_finding = await session.get(Finding, finding_id)
        if source_finding is None:
            skipped.append(finding_id)
            continue

        source_path = source_finding.file_path
        target_path = await _verification_target_path(
            session,
            source_finding,
            sonarr=sonarr,
            radarr=radarr,
            sonarr_api_key=sonarr_api_key,
            radarr_api_key=radarr_api_key,
            pairs=pairs,
        )
        if not target_path:
            skipped.append(source_finding.id)
            continue

        media_kind = (
            MediaKind(source_finding.media_kind)
            if source_finding.media_kind in {MediaKind.TV.value, MediaKind.MOVIE.value}
            else MediaKind.UNKNOWN
        )
        run.notes = merge_scan_notes(
            run.notes,
            phase="running",
            scope="verify",
            current_library=str(Path(target_path).parent),
            current_file=target_path,
            target_count=target_count,
            files_seen=run.files_seen,
            total_files=target_count,
            findings=run.suspicious_found,
            resolved_findings=resolved_count,
            moved_findings=moved_count,
            skipped_findings=skipped[:20],
        )
        if commit_progress:
            run = await _commit_scan_progress(session, run_id)
        result = await _scan_verify_target(
            session,
            run,
            abs_path=target_path,
            media_guess=media_kind,
            actor=actor,
            rules=rules,
            sonarr=sonarr,
            radarr=radarr,
            sonarr_api_key=sonarr_api_key,
            radarr_api_key=radarr_api_key,
            pairs=pairs,
            exc_rows=exc_rows,
            excluded_keywords=excluded_keywords,
            extras=extras,
            caches=caches,
        )
        if not result.get("persisted"):
            await _mark_finding_resolved(
                session,
                source_finding,
                run,
                actor=actor,
                message=f"Verified healthy after repair: {Path(target_path).name}",
            )
            resolved_count += 1
        elif str(target_path) != source_path:
            await _mark_finding_resolved(
                session,
                source_finding,
                run,
                actor=actor,
                message=f"Superseded by verified file at {target_path}",
            )
            moved_count += 1
        else:
            source_finding.last_scanned_at = dt.datetime.now(dt.UTC)
            source_finding.last_scan_run_id = run.id

        run.notes = merge_scan_notes(
            run.notes,
            phase="running",
            scope="verify",
            current_library=str(Path(target_path).parent),
            current_file=target_path,
            target_count=target_count,
            files_seen=run.files_seen,
            total_files=target_count,
            findings=run.suspicious_found,
            resolved_findings=resolved_count,
            moved_findings=moved_count,
            skipped_findings=skipped[:20],
        )
        progress_counter += 1
        if commit_progress and progress_counter >= VERIFY_PROGRESS_COMMIT_INTERVAL:
            run = await _commit_scan_progress(session, run_id)
            progress_counter = 0

    run.status = "completed"
    run.completed_at = dt.datetime.now(dt.UTC)
    run.notes = merge_scan_notes(
        run.notes,
        phase="completed",
        scope="verify",
        current_library=None,
        current_file=None,
        target_count=target_count,
        files_seen=run.files_seen,
        total_files=target_count,
        findings=run.suspicious_found,
        resolved_findings=resolved_count,
        moved_findings=moved_count,
        skipped_findings=skipped[:20],
    )
    await log_event(
        session,
        event_type="scan_completed",
        entity_type="scan_run",
        message=f"Verify finished: {run.files_seen} files checked, {resolved_count} resolved",
        entity_id=str(run.id),
        metadata={
            "scope": "verify",
            "target_count": target_count,
            "resolved_findings": resolved_count,
            "moved_findings": moved_count,
            "skipped_findings": skipped[:50],
        },
        actor=actor,
    )
    return run


async def run_scan(session: AsyncSession, *, actor: str | None = None) -> ScanRun:
    run = ScanRun(
        started_at=dt.datetime.now(dt.UTC),
        status="running",
        files_seen=0,
        suspicious_found=0,
        notes=merge_scan_notes(None, scope="library"),
    )
    session.add(run)
    await session.flush()
    return await _perform_scan(session, run, actor=actor, emit_started_event=True, commit_progress=False)


async def run_verify_scan(
    session: AsyncSession,
    finding_ids: list[int],
    *,
    actor: str | None = None,
) -> ScanRun:
    run = ScanRun(
        started_at=dt.datetime.now(dt.UTC),
        status="running",
        files_seen=0,
        suspicious_found=0,
    )
    session.add(run)
    await session.flush()
    return await _perform_verify_scan(
        session,
        run,
        finding_ids,
        actor=actor,
        emit_started_event=True,
        commit_progress=False,
    )


async def start_scan(actor: str | None = None, *, resume: bool = False) -> ScanRun | None:
    global _scan_task
    global _scan_run_id, _scan_scope
    scan_task_lock, scan_stop_event = _ensure_scan_runtime_primitives()
    async with scan_task_lock:
        if _scan_task and not _scan_task.done():
            return None
        scan_stop_event.clear()

        async with SessionLocal() as session:
            resume_after_file = None
            resume_from_scan_id = None
            files_seen = 0
            suspicious_found = 0
            notes = merge_scan_notes(None, scope="library")
            if resume:
                interrupted_run = await latest_resumable_library_scan(session)
                if interrupted_run is None:
                    return None
                interrupted_notes = parse_scan_notes(interrupted_run.notes)
                resume_after_file = interrupted_notes.get("resume_after_file")
                if not resume_after_file or not Path(str(resume_after_file)).exists():
                    return None
                resume_from_scan_id = interrupted_run.id
                files_seen = max(interrupted_run.files_seen, 0)
                suspicious_found = max(interrupted_run.suspicious_found, 0)
                notes = merge_scan_notes(
                    notes,
                    resumed_from_scan_id=resume_from_scan_id,
                    resume_after_file=resume_after_file,
                )
            run = ScanRun(
                started_at=dt.datetime.now(dt.UTC),
                status="running",
                files_seen=files_seen,
                suspicious_found=suspicious_found,
                notes=notes,
            )
            session.add(run)
            await session.flush()
            await log_event(
                session,
                event_type="scan_started",
                entity_type="scan_run",
                message="Library scan resumed" if resume else "Library scan started",
                entity_id=str(run.id),
                metadata={"scope": "library", "resumed_from_scan_id": resume_from_scan_id} if resume else None,
                actor=actor,
            )
            await session.commit()
            run_id = run.id

        _scan_run_id = run_id
        _scan_scope = "library"
        _scan_task = asyncio.create_task(
            _background_scan(
                run_id,
                actor=actor,
                resume_after_file=resume_after_file,
                resume_from_scan_id=resume_from_scan_id,
            )
        )

        async with SessionLocal() as session:
            persisted = await session.get(ScanRun, run_id)
            return persisted


async def start_verify_scan(finding_ids: list[int], actor: str | None = None) -> ScanRun | None:
    global _scan_task
    global _scan_run_id, _scan_scope
    finding_ids = _normalize_finding_ids(finding_ids)
    if not finding_ids:
        return None
    scan_task_lock, scan_stop_event = _ensure_scan_runtime_primitives()
    async with scan_task_lock:
        if _scan_task and not _scan_task.done():
            async with SessionLocal() as session:
                run = ScanRun(
                    started_at=dt.datetime.now(dt.UTC),
                    status="queued",
                    files_seen=0,
                    suspicious_found=0,
                    notes=merge_scan_notes(
                        None,
                        scope="verify",
                        phase="queued",
                        target_count=len(finding_ids),
                        finding_ids=finding_ids,
                    ),
                )
                session.add(run)
                await session.commit()
                return run
            return None
        scan_stop_event.clear()

        async with SessionLocal() as session:
            run = ScanRun(
                started_at=dt.datetime.now(dt.UTC),
                status="running",
                files_seen=0,
                suspicious_found=0,
                notes=merge_scan_notes(None, scope="verify", target_count=len(finding_ids)),
            )
            session.add(run)
            await session.flush()
            await log_event(
                session,
                event_type="scan_started",
                entity_type="scan_run",
                message=f"Verify scan started for {len(finding_ids)} finding(s)",
                entity_id=str(run.id),
                metadata={"scope": "verify", "finding_ids": finding_ids[:50]},
                actor=actor,
            )
            await session.commit()
            run_id = run.id

        _scan_run_id = run_id
        _scan_scope = "verify"
        _scan_task = asyncio.create_task(_background_verify_scan(run_id, finding_ids, actor=actor))

        async with SessionLocal() as session:
            persisted = await session.get(ScanRun, run_id)
            return persisted


async def _start_next_queued_verify_scan_locked(*, actor: str | None = "worker") -> ScanRun | None:
    global _scan_task, _scan_run_id, _scan_scope
    _ensure_scan_runtime_primitives()
    if _scan_task and not _scan_task.done():
        return None

    async with SessionLocal() as session:
        active_run = (
            await session.execute(
                select(ScanRun)
                .where(ScanRun.status == "running")
                .order_by(ScanRun.id.desc())
                .limit(1)
            )
        ).scalars().first()
        if active_run is not None:
            return None

        queued = (
            await session.execute(
                select(ScanRun)
                .where(ScanRun.status == "queued")
                .order_by(ScanRun.id.asc())
                .limit(1)
            )
        ).scalars().first()
        if not queued:
            return None

        notes = parse_scan_notes(queued.notes)
        if notes.get("scope") != "verify":
            return None

        finding_ids = _normalize_finding_ids(notes.get("finding_ids") or [])
        if not finding_ids:
            queued.status = "failed"
            queued.completed_at = dt.datetime.now(dt.UTC)
            queued.notes = merge_scan_notes(
                queued.notes,
                phase="failed",
                error="Queued verify scan has no valid findings",
            )
            await log_event(
                session,
                event_type="scan_failed",
                entity_type="scan_run",
                entity_id=str(queued.id),
                message="Queued verify scan has no valid findings",
                metadata={"scope": "verify"},
                actor=actor,
            )
            await session.commit()
            return None

        queued.status = "running"
        queued.started_at = dt.datetime.now(dt.UTC)
        queued.completed_at = None
        queued.files_seen = 0
        queued.suspicious_found = 0
        queued.notes = merge_scan_notes(
            queued.notes,
            scope="verify",
            phase="running",
            current_file=None,
            current_library=None,
            target_count=len(finding_ids),
            error=None,
        )
        await log_event(
            session,
            event_type="scan_started",
            entity_type="scan_run",
            message=f"Verify scan started for {len(finding_ids)} finding(s)",
            entity_id=str(queued.id),
            metadata={"scope": "verify", "finding_ids": finding_ids[:50]},
            actor=actor,
        )
        await session.commit()
        run_id = queued.id

    _scan_run_id = run_id
    _scan_scope = "verify"
    _scan_task = asyncio.create_task(_background_verify_scan(run_id, finding_ids, actor=actor))

    async with SessionLocal() as session:
        return await session.get(ScanRun, run_id)


async def start_next_queued_verify_scan(*, actor: str | None = "worker") -> ScanRun | None:
    scan_task_lock, _ = _ensure_scan_runtime_primitives()
    async with scan_task_lock:
        return await _start_next_queued_verify_scan_locked(actor=actor)


async def _background_scan(
    run_id: int,
    *,
    actor: str | None = None,
    resume_after_file: str | None = None,
    resume_from_scan_id: int | None = None,
) -> None:
    try:
        async with SessionLocal() as session:
            run = await session.get(ScanRun, run_id)
            if not run:
                return
            await _perform_scan(
                session,
                run,
                actor=actor,
                emit_started_event=False,
                commit_progress=True,
                resume_after_file=resume_after_file,
                resume_from_scan_id=resume_from_scan_id,
            )
            await session.commit()
    except Exception as exc:
        log.exception("scan %s failed", run_id)
        async with SessionLocal() as session:
            run = await session.get(ScanRun, run_id)
            if run:
                run.status = "failed"
                run.completed_at = dt.datetime.now(dt.UTC)
                run.notes = merge_scan_notes(
                    run.notes,
                    phase="failed",
                    current_file=None,
                    current_library=None,
                    error=str(exc)[:500],
                )
                await log_event(
                    session,
                    event_type="scan_failed",
                    entity_type="scan_run",
                    entity_id=str(run_id),
                    message=str(exc)[:500],
                    actor=actor,
                )
                await session.commit()
    finally:
        await _clear_scan_runtime(run_id)


async def _background_verify_scan(run_id: int, finding_ids: list[int], *, actor: str | None = None) -> None:
    try:
        async with SessionLocal() as session:
            run = await session.get(ScanRun, run_id)
            if not run:
                return
            await _perform_verify_scan(
                session,
                run,
                finding_ids,
                actor=actor,
                emit_started_event=False,
                commit_progress=True,
            )
            await session.commit()
    except Exception as exc:
        log.exception("verify scan %s failed", run_id)
        async with SessionLocal() as session:
            run = await session.get(ScanRun, run_id)
            if run:
                run.status = "failed"
                run.completed_at = dt.datetime.now(dt.UTC)
                run.notes = merge_scan_notes(
                    run.notes,
                    phase="failed",
                    current_file=None,
                    current_library=None,
                    error=str(exc)[:500],
                )
                await log_event(
                    session,
                    event_type="scan_failed",
                    entity_type="scan_run",
                    entity_id=str(run_id),
                    message=str(exc)[:500],
                    metadata={"scope": "verify"},
                    actor=actor,
                )
                await session.commit()
    finally:
        await _clear_scan_runtime(run_id)


async def stop_background_scan() -> None:
    global _scan_task
    scan_task_lock, scan_stop_event = _ensure_scan_runtime_primitives()
    async with scan_task_lock:
        task = _scan_task
        _scan_task = None
        scan_stop_event.clear()
        global _scan_run_id, _scan_scope
        _scan_run_id = None
        _scan_scope = None
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def request_scan_stop(*, actor: str | None = None) -> int | None:
    del actor
    scan_task_lock, scan_stop_event = _ensure_scan_runtime_primitives()
    async with scan_task_lock:
        if not _scan_task or _scan_task.done() or _scan_scope != "library" or _scan_run_id is None:
            return None
        scan_stop_event.set()
        return _scan_run_id


def scan_stop_requested() -> bool:
    _, scan_stop_event = _ensure_scan_runtime_primitives()
    return bool(
        _scan_task
        and not _scan_task.done()
        and _scan_scope == "library"
        and scan_stop_event.is_set()
    )


async def _clear_scan_runtime(run_id: int) -> None:
    global _scan_task, _scan_run_id, _scan_scope
    scan_task_lock, scan_stop_event = _ensure_scan_runtime_primitives()
    async with scan_task_lock:
        if _scan_run_id == run_id:
            _scan_task = None
            _scan_run_id = None
            _scan_scope = None
            scan_stop_event.clear()
            await _start_next_queued_verify_scan_locked(actor="worker")


async def recover_abandoned_scans(*, actor: str | None = "system") -> int:
    async with SessionLocal() as session:
        runs = (
            await session.execute(select(ScanRun).where(ScanRun.status == "running").order_by(ScanRun.id.asc()))
        ).scalars().all()
        if not runs:
            return 0

        now = dt.datetime.now(dt.UTC)
        recovered = 0
        for run in runs:
            notes = parse_scan_notes(run.notes)
            scope = notes.get("scope")
            message = "Verify scan interrupted because Mendarr restarted" if scope == "verify" else "Library scan interrupted because Mendarr restarted"
            run.status = "interrupted"
            run.completed_at = now
            run.notes = merge_scan_notes(
                run.notes,
                phase="interrupted",
                current_file=None,
                current_library=None,
                error=message,
            )
            await log_event(
                session,
                event_type="scan_interrupted",
                entity_type="scan_run",
                entity_id=str(run.id),
                message=message,
                metadata={"scope": scope or "library"},
                actor=actor,
            )
            recovered += 1

        await session.commit()
        return recovered
