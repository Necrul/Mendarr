"""
Rule-based suspicion scoring. See docs/scoring-engine.md for semantics.
"""

from __future__ import annotations

from pathlib import Path

from app.domain.enums import Confidence, MediaKind, ProposedAction
from app.domain.value_objects import (
    BAD_EXTENSION,
    BITRATE_ANOMALY,
    DUPLICATE_VARIANT,
    KEYWORD_SAMPLE_TRAILER,
    NO_AUDIO,
    NO_DURATION,
    NO_RESOLUTION,
    NO_VIDEO_CODEC,
    NO_VIDEO_STREAM,
    PROBE_FAILED,
    PATH_EXCLUDED,
    RULE_IGNORE_PATTERN,
    SHORT_DURATION,
    VERY_SMALL,
    ZERO_BYTE,
    ProbeResult,
    ReasonSignal,
    ScoreResult,
)

VIDEO_EXTENSIONS = frozenset({
    ".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".mov", ".mpg", ".mpeg", ".ts", ".m2ts", ".webm"
})

DEFAULT_EXTRAS_KEYWORDS = (
    "sample",
    "trailer",
    "tv spot",
    "promo",
    "featurette",
    "gag reel",
    "extras",
    "interview",
    "behind the scenes",
    "deleted scene",
    "clip",
    "reel",
    "audition",
)


def _lower_name(path: Path) -> str:
    return f"{path.name} {path.as_posix()}".lower()


def path_matches_ignored_patterns(file_path: str, patterns: list[str]) -> bool:
    p = Path(file_path)
    for pat in patterns:
        pat = pat.strip()
        if not pat:
            continue
        try:
            if p.match(pat) or p.as_posix().lower().endswith(pat.lower().lstrip("*")):
                return True
            if fnmatch_simple(p.name, pat):
                return True
        except Exception:
            continue
    return False


def fnmatch_simple(name: str, pattern: str) -> bool:
    import fnmatch

    return fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(name.lower(), pattern.lower())


def path_is_excluded(file_path: str, excluded_roots: list[str]) -> bool:
    try:
        real = Path(file_path).resolve()
        for ex in excluded_roots:
            ex = ex.strip()
            if not ex:
                continue
            try:
                er = Path(ex).resolve()
                if real == er or str(real).startswith(str(er) + "/") or str(real).startswith(str(er) + "\\"):
                    return True
            except OSError:
                continue
    except OSError:
        return False
    return False


def collect_duplicate_signals(siblings: list[Path], target: Path) -> list[ReasonSignal]:
    """Same basename different ext in folder — possible duplicate rips."""
    out: list[ReasonSignal] = []
    stem_counts: dict[str, list[str]] = {}
    for s in siblings:
        stem_counts.setdefault(s.stem.lower(), []).append(s.suffix.lower())
    tstem = target.stem.lower()
    if len(stem_counts.get(tstem, [])) > 1:
        out.append(
            ReasonSignal(
                code=DUPLICATE_VARIANT,
                message=f"Multiple files share base name '{target.stem}' in folder",
                severity="warn",
                weight=15,
            )
        )
    return out


def keyword_hits(text: str, keywords: tuple[str, ...]) -> list[str]:
    hits = []
    t = text.lower()
    for kw in keywords:
        if kw.lower() in t:
            hits.append(kw)
    return hits


def filtered_keyword_hits(
    text: str,
    keywords: tuple[str, ...],
    excluded_keywords: tuple[str, ...] | None,
) -> list[str]:
    hits = keyword_hits(text, keywords)
    if not excluded_keywords:
        return hits
    excluded = {kw.lower() for kw in excluded_keywords if kw.strip()}
    return [hit for hit in hits if hit.lower() not in excluded]


def _confidence_from_score(score: int, severities: list[str]) -> Confidence:
    if "critical" in severities or score >= 85:
        return Confidence.HIGH
    if score >= 45 or "warn" in severities:
        return Confidence.MEDIUM
    return Confidence.LOW


def _action_from_signals(
    score: int,
    confidence: Confidence,
    reasons: list[ReasonSignal],
    has_manager_match: bool,
    auto_remediation: bool,
) -> ProposedAction:
    codes = {r.code for r in reasons}
    if PATH_EXCLUDED in codes or RULE_IGNORE_PATTERN in codes:
        return ProposedAction.IGNORE
    if not has_manager_match and score >= 20:
        return ProposedAction.REVIEW
    if not has_manager_match:
        return ProposedAction.REVIEW
    if confidence == Confidence.LOW and score < 25:
        return ProposedAction.REVIEW
    if confidence == Confidence.HIGH and KEYWORD_SAMPLE_TRAILER in codes:
        return ProposedAction.SEARCH_REPLACEMENT
    if confidence == Confidence.HIGH:
        if auto_remediation:
            return ProposedAction.SEARCH_REPLACEMENT
        return ProposedAction.RESCAN_ONLY
    if confidence == Confidence.MEDIUM:
        return ProposedAction.RESCAN_ONLY
    return ProposedAction.REVIEW


def score_finding(
    *,
    file_path: str,
    media_kind: MediaKind,
    size_bytes: int,
    probe: ProbeResult | None,
    min_tv_size_bytes: int,
    min_movie_size_bytes: int,
    min_duration_tv: float,
    min_duration_movie: float,
    extras_keywords: tuple[str, ...] | None = None,
    excluded_keywords: tuple[str, ...] | None = None,
    excluded_path_lines: str = "",
    ignored_pattern_lines: str = "",
    siblings: list[Path] | None = None,
    has_manager_match: bool = False,
    auto_remediation: bool = False,
) -> ScoreResult:
    reasons: list[ReasonSignal] = []
    p = Path(file_path)
    text_blob = _lower_name(p)

    excluded = [ln for ln in (excluded_path_lines or "").splitlines() if ln.strip()]
    if path_is_excluded(file_path, excluded):
        reasons.append(
            ReasonSignal(
                code=PATH_EXCLUDED,
                message="Path is under a configured exclusion",
                severity="info",
                weight=0,
            )
        )
    ign = [ln for ln in (ignored_pattern_lines or "").splitlines() if ln.strip()]
    if path_matches_ignored_patterns(file_path, ign):
        reasons.append(
            ReasonSignal(
                code=RULE_IGNORE_PATTERN,
                message="Path matched an ignore pattern",
                severity="info",
                weight=0,
            )
        )

    if size_bytes == 0:
        reasons.append(
            ReasonSignal(code=ZERO_BYTE, message="File is zero bytes", severity="critical", weight=100)
        )

    min_size = min_tv_size_bytes if media_kind == MediaKind.TV else min_movie_size_bytes
    if 0 < size_bytes < min_size:
        reasons.append(
            ReasonSignal(
                code=VERY_SMALL,
                message=f"File smaller than minimum for {media_kind.value} ({size_bytes} < {min_size})",
                severity="warn",
                weight=35,
            )
        )

    suf = p.suffix.lower()
    if suf and suf not in VIDEO_EXTENSIONS:
        reasons.append(
            ReasonSignal(
                code=BAD_EXTENSION,
                message=f"Unexpected video extension '{suf}'",
                severity="warn",
                weight=20,
            )
        )

    if siblings:
        reasons.extend(collect_duplicate_signals(siblings, p))

    if probe:
        if not probe.ok and probe.error:
            reasons.append(
                ReasonSignal(
                    code=PROBE_FAILED,
                    message=f"ffprobe failed: {probe.error[:200]}",
                    severity="critical",
                    weight=70,
                )
            )
        elif probe.ok:
            streams = (probe.raw or {}).get("streams") or []
            has_video = any(s.get("codec_type") == "video" for s in streams)
            if streams and not has_video:
                reasons.append(
                    ReasonSignal(
                        code=NO_VIDEO_STREAM,
                        message="Container has no video stream",
                        severity="critical",
                        weight=80,
                    )
                )
            if probe.duration_seconds is None or probe.duration_seconds <= 0:
                reasons.append(
                    ReasonSignal(code=NO_DURATION, message="No duration reported", severity="warn", weight=40)
                )
            else:
                need = min_duration_tv if media_kind == MediaKind.TV else min_duration_movie
                if probe.duration_seconds + 1e-6 < need:
                    reasons.append(
                        ReasonSignal(
                            code=SHORT_DURATION,
                            message=f"Duration {probe.duration_seconds:.1f}s below minimum {need}s",
                            severity="warn",
                            weight=45,
                        )
                    )
            if probe.ok and not (probe.width and probe.height):
                reasons.append(
                    ReasonSignal(code=NO_RESOLUTION, message="Missing video resolution", severity="warn", weight=25)
                )
            if probe.ok and not probe.video_codec:
                reasons.append(
                    ReasonSignal(code=NO_VIDEO_CODEC, message="Missing video codec", severity="warn", weight=20)
                )
            if media_kind in (MediaKind.TV, MediaKind.MOVIE) and probe.ok:
                if not probe.audio_codecs:
                    reasons.append(
                        ReasonSignal(
                            code=NO_AUDIO,
                            message="No audio stream (expected for main feature/episode)",
                            severity="warn",
                            weight=25,
                        )
                    )
            fmt = (probe.raw or {}).get("format") or {}
            br = fmt.get("bit_rate")
            if br and str(br).isdigit() and probe.duration_seconds and probe.duration_seconds > 60:
                bps = int(br)
                if bps < 50_000:
                    reasons.append(
                        ReasonSignal(
                            code=BITRATE_ANOMALY,
                            message=f"Very low reported bitrate ({bps} bps)",
                            severity="warn",
                            weight=15,
                        )
                    )

    score = min(100, sum(r.weight for r in reasons))
    severities = [r.severity for r in reasons]
    confidence = _confidence_from_score(score, severities)
    action = _action_from_signals(score, confidence, reasons, has_manager_match, auto_remediation)
    return ScoreResult(score=score, confidence=confidence, proposed_action=action, reasons=reasons)
