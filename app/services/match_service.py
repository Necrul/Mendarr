from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import IntegrationKind, ManagerKind, MediaKind
from app.domain.matching import (
    local_to_manager_relative,
    parse_movie_from_path,
    parse_tv_from_path,
    radarr_movie_match_score,
    sonarr_series_match_score,
)
from app.integrations.radarr_client import RadarrClient
from app.integrations.sonarr_client import SonarrClient
from app.persistence.models import Finding, LibraryRoot
from app.services.integration_service import get_integration, reveal_integration_api_key


@dataclass
class MatchOutcome:
    manager_kind: ManagerKind
    manager_entity_id: str | None
    title: str | None
    season_number: int | None
    episode_number: int | None
    year: int | None
    match_confidence: str


def sonarr_episode_entity_id(episode_id: int | str) -> str:
    return f"episode:{episode_id}"


def sonarr_series_entity_id(series_id: int | str) -> str:
    return f"series:{series_id}"


def parse_sonarr_entity_id(raw: str | None) -> tuple[str | None, int | None]:
    if not raw:
        return None, None
    text = str(raw).strip()
    if text.startswith("episode:"):
        value = text.split(":", 1)[1]
        return "episode", int(value) if value.isdigit() else None
    if text.startswith("series:"):
        value = text.split(":", 1)[1]
        return "series", int(value) if value.isdigit() else None
    return "episode", int(text) if text.isdigit() else None


def _manager_parse_candidates(path: Path, pairs: list[tuple[str, str]] | None = None) -> list[str]:
    candidates: list[str] = []
    if pairs:
        mapped = local_to_manager_relative(str(path), pairs)
        if mapped:
            manager_root, relative_path = mapped
            manager_root_clean = manager_root.replace("\\", "/").rstrip("/")
            relative_clean = relative_path.replace("\\", "/").lstrip("/")
            if manager_root_clean and relative_clean:
                candidates.append(f"{manager_root_clean}/{relative_clean}")
            if relative_clean:
                candidates.append(relative_clean)
    if path.parent.name:
        candidates.append(f"{path.parent.name}/{path.name}")
    candidates.append(path.name)

    seen: set[str] = set()
    unique: list[str] = []
    for value in candidates:
        cleaned = str(value).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        unique.append(cleaned)
    return unique


async def load_root_pairs(session: AsyncSession) -> dict[str, list[tuple[str, str]]]:
    r = await session.execute(select(LibraryRoot).where(LibraryRoot.enabled.is_(True)))
    roots = list(r.scalars().all())
    out: dict[str, list[tuple[str, str]]] = {"sonarr": [], "radarr": []}
    for lr in roots:
        k = lr.manager_kind.lower()
        if k in out and lr.manager_root_path.strip():
            out[k].append((lr.manager_root_path, lr.local_root_path))
    return out


async def _match_tv_path_via_parse_candidates(
    file_path: str,
    client: SonarrClient,
    *,
    pairs: list[tuple[str, str]] | None = None,
    title_hint: str | None = None,
    season_hint: int | None = None,
    episode_hint: int | None = None,
) -> MatchOutcome | None:
    path = Path(file_path)
    for candidate in _manager_parse_candidates(path, pairs):
        parsed = await client.parse_path(candidate)
        episode = (parsed.get("episodes") or [None])[0]
        if not episode or episode.get("id") is None:
            continue
        series = parsed.get("series") or {}
        return MatchOutcome(
            ManagerKind.SONARR,
            sonarr_episode_entity_id(episode["id"]),
            series.get("title") or title_hint,
            episode.get("seasonNumber", season_hint),
            episode.get("episodeNumber", episode_hint),
            None,
            "high",
        )
    return None


async def _match_movie_path_via_parse_candidates(
    file_path: str,
    client: RadarrClient,
    *,
    pairs: list[tuple[str, str]] | None = None,
    title_hint: str | None = None,
    year_hint: int | None = None,
) -> MatchOutcome | None:
    path = Path(file_path)
    for candidate in _manager_parse_candidates(path, pairs):
        parsed = await client.parse_path(candidate)
        movie = parsed.get("movie") or {}
        if movie.get("id") is None:
            continue
        return MatchOutcome(
            ManagerKind.RADARR,
            str(movie["id"]),
            movie.get("title") or title_hint,
            None,
            None,
            movie.get("year") or year_hint,
            "medium",
        )
    return None


async def match_tv_path(
    session: AsyncSession,
    file_path: str,
    sonarr_url: str,
    sonarr_key: str,
    *,
    pairs: list[tuple[str, str]] | None = None,
    client: SonarrClient | None = None,
    series_list: list[dict[str, Any]] | None = None,
) -> MatchOutcome:
    pairs = pairs if pairs is not None else (await load_root_pairs(session))["sonarr"]
    path = Path(file_path)
    tv = parse_tv_from_path(path)
    client = client or SonarrClient(sonarr_url, sonarr_key)
    parsed_match = await _match_tv_path_via_parse_candidates(
        file_path,
        client,
        pairs=pairs,
        title_hint=tv.show_hint,
        season_hint=tv.season,
        episode_hint=tv.episode,
    )
    if parsed_match is not None:
        return parsed_match
    series_list = series_list if series_list is not None else await client.all_series()
    series, score = sonarr_series_match_score(tv.show_hint, series_list)
    episode_id = None
    title = tv.show_hint
    season = tv.season
    episode = tv.episode
    if series:
        title = series.get("title") or title
        sid = series.get("id")
        if sid is not None:
            try:
                for candidate in _manager_parse_candidates(path, pairs):
                    parsed = await client.parse_path(candidate)
                    ep = (parsed.get("episodes") or [None])[0]
                    if ep and ep.get("id"):
                        episode_id = sonarr_episode_entity_id(ep["id"])
                        season = ep.get("seasonNumber", season)
                        episode = ep.get("episodeNumber", episode)
                        break
                if episode_id is None and season is not None and episode is not None:
                    episodes = await client.episodes_for_series(int(sid))
                    matched = next(
                        (
                            row
                            for row in episodes
                            if row.get("seasonNumber") == season and row.get("episodeNumber") == episode
                        ),
                        None,
                    )
                    if matched and matched.get("id") is not None:
                        episode_id = sonarr_episode_entity_id(matched["id"])
            except Exception:
                pass
        conf = "high" if score >= 90 else "medium" if score >= 50 else "low"
        return MatchOutcome(
            ManagerKind.SONARR,
            episode_id or (sonarr_series_entity_id(series.get("id")) if series.get("id") is not None else None),
            title,
            season,
            episode,
            None,
            conf,
        )
    return MatchOutcome(ManagerKind.NONE, None, title, season, episode, None, "low")


async def match_movie_path(
    session: AsyncSession,
    file_path: str,
    radarr_url: str,
    radarr_key: str,
    *,
    pairs: list[tuple[str, str]] | None = None,
    client: RadarrClient | None = None,
    movies: list[dict[str, Any]] | None = None,
) -> MatchOutcome:
    pairs = pairs if pairs is not None else (await load_root_pairs(session))["radarr"]
    path = Path(file_path)
    mv = parse_movie_from_path(path)
    client = client or RadarrClient(radarr_url, radarr_key)
    parsed_match = await _match_movie_path_via_parse_candidates(
        file_path,
        client,
        pairs=pairs,
        title_hint=mv.title_hint,
        year_hint=mv.year,
    )
    if parsed_match is not None:
        return parsed_match
    movies = movies if movies is not None else await client.all_movies()
    movie, score = radarr_movie_match_score(mv.title_hint, mv.year, movies)
    if movie:
        mid = movie.get("id")
        conf = "high" if score >= 90 else "medium" if score >= 50 else "low"
        return MatchOutcome(
            ManagerKind.RADARR,
            str(mid) if mid is not None else None,
            movie.get("title") or mv.title_hint,
            None,
            None,
            movie.get("year") or mv.year,
            conf,
        )
    return MatchOutcome(
        ManagerKind.NONE,
        None,
        mv.title_hint,
        None,
        None,
        mv.year,
        "low",
    )


def infer_media_kind_from_roots(file_path: str, pairs_tv: list[tuple[str, str]], pairs_movie: list[tuple[str, str]]) -> MediaKind:
    if local_to_manager_relative(file_path, pairs_tv):
        return MediaKind.TV
    if local_to_manager_relative(file_path, pairs_movie):
        return MediaKind.MOVIE
    return MediaKind.UNKNOWN


async def relink_finding(
    session: AsyncSession,
    finding: Finding,
    *,
    allow_library_lookup: bool = True,
) -> MatchOutcome:
    pairs = await load_root_pairs(session)

    preferred = finding.manager_kind or ""
    candidates: list[str]
    if preferred in {"sonarr", "radarr"}:
        candidates = [preferred]
    elif finding.media_kind == MediaKind.TV.value:
        candidates = ["sonarr"]
    elif finding.media_kind == MediaKind.MOVIE.value:
        candidates = ["radarr"]
    else:
        candidates = ["sonarr", "radarr"]

    for kind in candidates:
        integration_kind = IntegrationKind.SONARR if kind == "sonarr" else IntegrationKind.RADARR
        integration = await get_integration(session, integration_kind)
        api_key = reveal_integration_api_key(integration)
        if not integration or not integration.enabled or not integration.base_url or not api_key:
            continue

        if kind == "sonarr":
            client = SonarrClient(integration.base_url, api_key)
            if not allow_library_lookup:
                try:
                    outcome = await _match_tv_path_via_parse_candidates(
                        finding.file_path,
                        client,
                        pairs=pairs.get("sonarr", []),
                        title_hint=finding.title,
                        season_hint=finding.season_number,
                        episode_hint=finding.episode_number,
                    )
                except Exception:
                    outcome = None
                if outcome and outcome.manager_entity_id:
                    return outcome
                continue
            outcome = await match_tv_path(
                session,
                finding.file_path,
                integration.base_url,
                api_key,
                pairs=pairs.get("sonarr", []),
                client=client,
            )
            if outcome.manager_kind == ManagerKind.SONARR and outcome.manager_entity_id:
                return outcome
        else:
            client = RadarrClient(integration.base_url, api_key)
            if not allow_library_lookup:
                try:
                    outcome = await _match_movie_path_via_parse_candidates(
                        finding.file_path,
                        client,
                        pairs=pairs.get("radarr", []),
                        title_hint=finding.title,
                        year_hint=finding.year,
                    )
                except Exception:
                    outcome = None
                if outcome and outcome.manager_entity_id:
                    return outcome
                continue
            outcome = await match_movie_path(
                session,
                finding.file_path,
                integration.base_url,
                api_key,
                pairs=pairs.get("radarr", []),
                client=client,
            )
            if outcome.manager_kind == ManagerKind.RADARR and outcome.manager_entity_id:
                return outcome

    return MatchOutcome(
        manager_kind=ManagerKind.NONE,
        manager_entity_id=None,
        title=finding.title,
        season_number=finding.season_number,
        episode_number=finding.episode_number,
        year=finding.year,
        match_confidence="low",
    )
