"""Path and filename helpers for TV / movie matching."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class TvParseResult:
    show_hint: str | None
    season: int | None
    episode: int | None
    is_specials: bool


@dataclass
class MovieParseResult:
    title_hint: str | None
    year: int | None


_SXXEYY = re.compile(r"(?i)\bS(\d{1,2})E(\d{1,3})\b")
_SXX_EYY = re.compile(r"(?i)\bSeason\s*(\d{1,2})\s*[^0-9]{0,3}Episode\s*(\d{1,3})\b")
_DOT_STYLE = re.compile(r"(?i)\b(\d{1,2})x(\d{1,3})\b")
_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
_MANAGER_ID_TAG = re.compile(r"(?i)[\[{(]\s*(?:tvdb|tmdb|imdb)(?:id)?\s*[-:=]?\s*[^)\]}]+[\]})]")


def normalize_title_token(s: str) -> str:
    s = _MANAGER_ID_TAG.sub(" ", s)
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = s.replace(".", " ").replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s.casefold()


def _title_tokens(value: str) -> list[str]:
    normalized = normalize_title_token(value)
    return [token for token in normalized.split(" ") if token]


def parse_tv_from_path(path: Path) -> TvParseResult:
    parts = [normalize_title_token(p) for p in path.parts]
    show_hint = None
    season = None
    episode = None
    is_specials = False
    for i, part in enumerate(parts):
        pl = part.lower()
        if pl in ("specials", "season 00", "season 0"):
            is_specials = True
        m = re.match(r"^season\s*(\d{1,2})$", pl)
        if m:
            season = int(m.group(1))
            continue
        m = re.match(r"^specials?$", pl)
        if m:
            is_specials = True
    stem = path.stem
    for rx in (_SXXEYY, _DOT_STYLE):
        m = rx.search(stem)
        if m:
            season = int(m.group(1))
            episode = int(m.group(2))
            break
    if season is None:
        m = _SXX_EYY.search(stem)
        if m:
            season = int(m.group(1))
            episode = int(m.group(2))
    if not show_hint:
        for j in range(len(path.parts) - 1, -1, -1):
            cand = path.parts[j]
            if cand.lower() in ("season 0", "specials"):
                continue
            if re.match(r"(?i)^season\s*\d+$", cand):
                if j > 0:
                    show_hint = path.parts[j - 1]
                break
        if show_hint is None and len(path.parts) >= 2:
            show_hint = path.parts[-2] if re.search(r"(?i)S\d+E\d+", path.name) else path.parts[0]

    return TvParseResult(
        show_hint=str(show_hint) if show_hint else None,
        season=season,
        episode=episode,
        is_specials=is_specials,
    )


def parse_movie_from_path(path: Path) -> MovieParseResult:
    stem = path.stem
    year = None
    m = _YEAR.search(stem)
    if m:
        year = int(m.group(1))
    elif _YEAR.search(path.parent.name):
        year = int(_YEAR.search(path.parent.name).group(1))
    title = stem
    if year:
        title = _YEAR.sub("", stem).strip(" .-_")
    if not title and path.parent.name:
        title = path.parent.name
    return MovieParseResult(title_hint=title or None, year=year)


def manager_path_to_local(manager_path: str, roots: list[tuple[str, str]]) -> str | None:
    """Map path prefix from Sonarr/Radarr root to local root."""
    mp = manager_path.replace("\\", "/").rstrip("/")
    for mgr_root, local_root in roots:
        m = mgr_root.replace("\\", "/").rstrip("/")
        if mp == m or mp.startswith(m + "/"):
            suffix = mp[len(m) :].lstrip("/")
            loc = Path(local_root) / suffix if suffix else Path(local_root)
            return str(loc.resolve()) if suffix else str(Path(local_root).resolve())
    return None


def local_to_manager_relative(local_file: str, roots: list[tuple[str, str]]) -> tuple[str, str] | None:
    """Return (manager_root_path, relative unix-style path under that root) or None."""
    try:
        lf = Path(local_file).resolve()
    except OSError:
        return None
    for mgr_root, local_root in roots:
        try:
            lr = Path(local_root).resolve()
        except OSError:
            continue
        try:
            rel = lf.relative_to(lr)
        except ValueError:
            continue
        rel_u = rel.as_posix()
        return mgr_root, rel_u
    return None


def sonarr_series_match_score(
    show_hint: str | None,
    series_list: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, int]:
    if not show_hint:
        return None, 0
    nh = normalize_title_token(show_hint)
    hint_tokens = set(_title_tokens(show_hint))
    best = None
    best_score = 0
    for s in series_list:
        title = s.get("title") or ""
        stitles = s.get("alternateTitles") or []
        variants = [normalize_title_token(title)] + [normalize_title_token(x.get("title", "")) for x in stitles]
        for v in variants:
            if not v:
                continue
            if v == nh:
                return s, 100
            variant_tokens = set(token for token in v.split(" ") if token)
            if not variant_tokens:
                continue
            overlap = hint_tokens & variant_tokens
            if overlap:
                coverage = len(overlap) / max(1, len(hint_tokens))
                if coverage >= 0.75:
                    score = 90
                elif coverage >= 0.5 and len(hint_tokens) >= 2:
                    score = 70
                else:
                    score = 0
                if score > best_score:
                    best, best_score = s, score
                    continue
            if len(nh) >= 5 and len(v) >= 5 and (nh in v or v in nh):
                score = 60
                if score > best_score:
                    best, best_score = s, score
    return best, best_score


def radarr_movie_match_score(
    title_hint: str | None,
    year: int | None,
    movies: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, int]:
    if not title_hint:
        return None, 0
    nh = normalize_title_token(title_hint)
    best = None
    best_score = 0
    for m in movies:
        title = m.get("title") or ""
        vt = normalize_title_token(title)
        score = 0
        if vt == nh:
            score = 100
        elif len(nh) >= 4 and len(vt) >= 4 and (nh in vt or vt in nh):
            score = 75
        my = m.get("year")
        if year and my == year:
            score += 15
        if score > best_score:
            best, best_score = m, score
    return best, best_score
