from __future__ import annotations

import datetime as dt
import json

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import IntegrationKind, JobStatus, ManagerKind, MediaKind, RemediationAction
from app.integrations.radarr_client import RadarrClient
from app.integrations.sonarr_client import SonarrClient
from app.integrations.ffprobe_adapter import probe_file
from app.persistence.models import Finding, RemediationAttempt, RemediationJob, ScanRun
from app.services.audit_service import log_event
from app.services.integration_service import get_integration, reveal_integration_api_key
from app.services.match_service import parse_sonarr_entity_id, relink_finding
from app.services.match_service import MatchOutcome
from app.domain.scoring import score_finding
from app.services.rule_service import extras_tuple_from_settings, get_or_create_rule_settings
from app.services.scan_service import upsert_finding


def _outcome_from_finding(finding: Finding) -> MatchOutcome | None:
    if not finding.manager_kind or finding.manager_kind == ManagerKind.NONE.value:
        return None
    try:
        mk = ManagerKind(finding.manager_kind)
    except ValueError:
        return None
    return MatchOutcome(
        manager_kind=mk,
        manager_entity_id=finding.manager_entity_id,
        title=finding.title,
        season_number=finding.season_number,
        episode_number=finding.episode_number,
        year=finding.year,
        match_confidence=finding.confidence or "medium",
    )


def _payload_has_error(payload: object) -> bool:
    return isinstance(payload, dict) and "error" in payload


def _payload_error_message(payload: object) -> str:
    if not isinstance(payload, dict):
        return "Manager request failed"
    error = str(payload.get("error") or "").strip()
    if error:
        return error[:2000]
    status = payload.get("status")
    if status not in (None, ""):
        return f"HTTP {status}"
    return "Manager request failed"


def _exception_message(exc: Exception) -> str:
    text = str(exc).strip()
    if text:
        return text[:2000]
    return exc.__class__.__name__[:2000]


async def _execute_sonarr_delete_search(
    session: AsyncSession,
    client: SonarrClient,
    finding: Finding,
) -> list[tuple[str, dict]]:
    eid = finding.manager_entity_id
    if not eid:
        raise RuntimeError("No Sonarr entity id on finding")
    entity_kind, entity_value = parse_sonarr_entity_id(eid)
    if entity_kind != "episode" or entity_value is None:
        raise RuntimeError("Delete and replace requires a Sonarr episode link")

    episode = await client.get_episode_by_id(entity_value)
    episode_file_id = episode.get("episodeFileId")
    if not episode_file_id:
        raise RuntimeError("Sonarr does not have a current episode file to delete")

    delete_result = await client.delete_episode_file(int(episode_file_id))
    if _payload_has_error(delete_result):
        return [("DeleteEpisodeFile", delete_result)]

    search_result = await client.episode_search([entity_value])
    return [("DeleteEpisodeFile", delete_result), ("EpisodeSearch", search_result)]


async def _execute_radarr_delete_search(
    session: AsyncSession,
    client: RadarrClient,
    finding: Finding,
) -> list[tuple[str, dict]]:
    mid = finding.manager_entity_id
    if not mid or not str(mid).isdigit():
        raise RuntimeError("Delete and replace requires a Radarr movie link")

    movie = await client.get_movie(int(mid))
    movie_file = movie.get("movieFile") or {}
    movie_file_id = movie_file.get("id") or movie.get("movieFileId")
    if not movie_file_id:
        raise RuntimeError("Radarr does not have a current movie file to delete")

    delete_result = await client.delete_movie_file(int(movie_file_id))
    if _payload_has_error(delete_result):
        return [("DeleteMovieFile", delete_result)]

    search_result = await client.movies_search([int(mid)])
    return [("DeleteMovieFile", delete_result), ("MoviesSearch", search_result)]


async def execute_job(session: AsyncSession, job_id: int, *, actor: str | None = "worker") -> None:
    r = await session.execute(
        select(RemediationJob)
        .options(selectinload(RemediationJob.finding))
        .where(RemediationJob.id == job_id)
    )
    job = r.scalar_one_or_none()
    if not job or job.status != JobStatus.QUEUED.value:
        return

    finding = job.finding
    if not finding:
        job.status = JobStatus.FAILED.value
        job.last_error = "Finding missing"
        job.completed_at = dt.datetime.now(dt.UTC)
        return

    job.status = JobStatus.RUNNING.value
    job.started_at = dt.datetime.now(dt.UTC)
    job.attempt_count += 1
    await session.flush()

    action = RemediationAction(job.action_type)

    try:
        should_refresh_link = finding.media_kind in {MediaKind.TV.value, MediaKind.MOVIE.value}
        if should_refresh_link:
            relinked = await relink_finding(session, finding)
            if relinked.manager_kind != ManagerKind.NONE and relinked.manager_entity_id:
                link_changed = (
                    finding.manager_kind != relinked.manager_kind.value
                    or finding.manager_entity_id != relinked.manager_entity_id
                    or finding.title != (relinked.title or finding.title)
                    or finding.season_number != relinked.season_number
                    or finding.episode_number != relinked.episode_number
                    or finding.year != relinked.year
                )
                finding.manager_kind = relinked.manager_kind.value
                finding.manager_entity_id = relinked.manager_entity_id
                finding.title = relinked.title or finding.title
                finding.season_number = relinked.season_number
                finding.episode_number = relinked.episode_number
                finding.year = relinked.year
                if link_changed:
                    await log_event(
                        session,
                        event_type="finding_updated",
                        entity_type="finding",
                        entity_id=str(finding.id),
                        message=f"Linked finding to {relinked.manager_kind.value}",
                        actor=actor,
                    )
                    await session.flush()

        if finding.manager_kind == ManagerKind.SONARR.value:
            sonarr = await get_integration(session, IntegrationKind.SONARR)
            sonarr_api_key = reveal_integration_api_key(sonarr)
            if not sonarr or not sonarr.enabled or not sonarr_api_key:
                raise RuntimeError("Sonarr not configured")
            client = SonarrClient(sonarr.base_url, sonarr_api_key)
            eid = finding.manager_entity_id
            if not eid:
                raise RuntimeError("No Sonarr entity id on finding")
            entity_kind, entity_value = parse_sonarr_entity_id(eid)
            if entity_value is None:
                raise RuntimeError("Invalid Sonarr link on finding")
            if action == RemediationAction.RESCAN_ONLY:
                sid = None
                if entity_kind == "episode":
                    try:
                        ep = await client.get_episode_by_id(entity_value)
                        sid = ep.get("seriesId")
                    except Exception:
                        sid = entity_value
                else:
                    sid = entity_value
                cmd = await client.rescan_series(int(sid))
                await _record_attempt(session, job, "RescanSeries", cmd)
                if _payload_has_error(cmd):
                    raise RuntimeError(_payload_error_message(cmd))
            elif action == RemediationAction.DELETE_SEARCH_REPLACEMENT:
                for step_name, payload in await _execute_sonarr_delete_search(session, client, finding):
                    await _record_attempt(session, job, step_name, payload)
                    if _payload_has_error(payload):
                        raise RuntimeError(_payload_error_message(payload))
            else:
                if entity_kind == "episode":
                    cutoff_unmet = await client.get_cutoff_unmet_episode(entity_value)
                    if cutoff_unmet is None:
                        raise RuntimeError(
                            "Sonarr reports cutoff already met for this episode; replacement search will not force an upgrade"
                        )
                    cmd = await client.episode_search([entity_value])
                    await _record_attempt(session, job, "EpisodeSearch", cmd)
                    if _payload_has_error(cmd):
                        raise RuntimeError(_payload_error_message(cmd))
                elif entity_kind == "series":
                    cmd = await client.series_search(entity_value)
                    await _record_attempt(session, job, "SeriesSearch", cmd)
                    if _payload_has_error(cmd):
                        raise RuntimeError(_payload_error_message(cmd))
                else:
                    raise RuntimeError("Episode search requires a Sonarr episode link")
        elif finding.manager_kind == ManagerKind.RADARR.value:
            radarr = await get_integration(session, IntegrationKind.RADARR)
            radarr_api_key = reveal_integration_api_key(radarr)
            if not radarr or not radarr.enabled or not radarr_api_key:
                raise RuntimeError("Radarr not configured")
            client = RadarrClient(radarr.base_url, radarr_api_key)
            mid = finding.manager_entity_id
            if not mid:
                raise RuntimeError("No Radarr movie id on finding")
            if action == RemediationAction.RESCAN_ONLY:
                cmd = await client.refresh_movie([int(mid)])
                await _record_attempt(session, job, "RefreshMovie", cmd)
                if _payload_has_error(cmd):
                    raise RuntimeError(_payload_error_message(cmd))
            elif action == RemediationAction.DELETE_SEARCH_REPLACEMENT:
                for step_name, payload in await _execute_radarr_delete_search(session, client, finding):
                    await _record_attempt(session, job, step_name, payload)
                    if _payload_has_error(payload):
                        raise RuntimeError(_payload_error_message(payload))
            else:
                cmd = await client.movies_search([int(mid)])
                await _record_attempt(session, job, "MoviesSearch", cmd)
                if _payload_has_error(cmd):
                    raise RuntimeError(_payload_error_message(cmd))
        else:
            raise RuntimeError("Finding is not linked to Sonarr or Radarr - manual review only")

        if action == RemediationAction.RESCAN_ONLY:
            path_obj = Path(finding.file_path)
            mo = _outcome_from_finding(finding)
            rules = await get_or_create_rule_settings(session)
            extras = extras_tuple_from_settings(rules)
            mk = MediaKind(finding.media_kind) if finding.media_kind in ("tv", "movie") else MediaKind.MOVIE

            pg = await probe_file(str(path_obj)) if path_obj.exists() else None
            size = path_obj.stat().st_size if path_obj.exists() else 0
            scored = score_finding(
                file_path=finding.file_path,
                media_kind=mk,
                size_bytes=size,
                probe=pg,
                min_tv_size_bytes=rules.min_tv_size_bytes,
                min_movie_size_bytes=rules.min_movie_size_bytes,
                min_duration_tv=rules.min_duration_tv_seconds,
                min_duration_movie=rules.min_duration_movie_seconds,
                extras_keywords=extras,
                excluded_path_lines=rules.excluded_paths,
                ignored_pattern_lines=rules.ignored_patterns,
                has_manager_match=bool(finding.manager_entity_id),
                auto_remediation=rules.auto_remediation_enabled,
            )
            sr_row = finding.last_scan_run_id
            if not sr_row:
                sr = ScanRun(status="completed", started_at=dt.datetime.now(dt.UTC), completed_at=dt.datetime.now(dt.UTC))
                session.add(sr)
                await session.flush()
                sr_row = sr.id
            sr = await session.get(ScanRun, sr_row)
            if not sr:
                sr = ScanRun(status="completed", started_at=dt.datetime.now(dt.UTC), completed_at=dt.datetime.now(dt.UTC))
                session.add(sr)
                await session.flush()
            await upsert_finding(
                session,
                sr,
                finding.file_path,
                path_obj,
                mk,
                mo,
                scored,
                pg.raw if pg and pg.ok else None,
            )

            refreshed = await session.get(Finding, finding.id)
            if refreshed and refreshed.suspicion_score < 15:
                refreshed.status = "resolved"

        job.status = JobStatus.SUCCEEDED.value
        job.completed_at = dt.datetime.now(dt.UTC)
        await log_event(
            session,
            event_type="job_succeeded",
            entity_type="remediation_job",
            message=f"Job {job.id} completed",
            entity_id=str(job.id),
            actor=actor,
        )
    except Exception as e:
        job.status = JobStatus.FAILED.value
        job.last_error = _exception_message(e)
        job.completed_at = dt.datetime.now(dt.UTC)
        await _record_attempt(session, job, "error", {"error": _exception_message(e)})
        await log_event(
            session,
            event_type="job_failed",
            entity_type="remediation_job",
            message=_exception_message(e)[:500],
            entity_id=str(job.id),
            actor=actor,
        )


async def _record_attempt(session: AsyncSession, job: RemediationJob, step: str, payload: object) -> None:
    summary = payload if isinstance(payload, str) else json.dumps(payload, default=str)[:8000]
    failed = isinstance(payload, dict) and "error" in payload
    att = RemediationAttempt(
        job_id=job.id,
        step_name=step,
        status="failed" if failed else "succeeded",
        request_summary=None,
        response_summary=summary,
        created_at=dt.datetime.now(dt.UTC),
    )
    session.add(att)
