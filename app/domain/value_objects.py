from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.domain.enums import Confidence, ProposedAction


@dataclass
class ProbeResult:
    ok: bool
    duration_seconds: float | None
    width: int | None
    height: int | None
    video_codec: str | None
    audio_codecs: list[str]
    raw: dict[str, Any] | None
    error: str | None = None


@dataclass
class ReasonSignal:
    code: str
    message: str
    severity: str  # info, warn, critical
    weight: int


@dataclass
class ScoreResult:
    score: int
    confidence: Confidence
    proposed_action: ProposedAction
    reasons: list[ReasonSignal] = field(default_factory=list)


# Reason codes (stable identifiers for UI and docs)
ZERO_BYTE = "FS_ZERO_BYTE"
VERY_SMALL = "FS_VERY_SMALL"
BAD_EXTENSION = "FS_BAD_EXTENSION"
DUPLICATE_VARIANT = "FS_DUPLICATE_VARIANT"
KEYWORD_SAMPLE_TRAILER = "FS_KEYWORD_EXTRA"
MISSING_EXPECTED = "FS_MISSING_EXPECTED"
PROBE_FAILED = "MD_PROBE_FAILED"
NO_VIDEO_STREAM = "MD_NO_VIDEO_STREAM"
NO_DURATION = "MD_NO_DURATION"
SHORT_DURATION = "MD_SHORT_DURATION"
NO_RESOLUTION = "MD_NO_RESOLUTION"
NO_VIDEO_CODEC = "MD_NO_VIDEO_CODEC"
NO_AUDIO = "MD_NO_AUDIO_EXPECTED"
BITRATE_ANOMALY = "MD_BITRATE_ANOMALY"
EXTENSION_MISMATCH = "MD_EXTENSION_MISMATCH"
CTX_TRAILER_IN_MAIN = "CTX_TRAILER_IN_MAIN"
MANAGER_METADATA_MISMATCH = "CTX_MANAGER_MISMATCH"
PATH_EXCLUDED = "RULE_PATH_EXCLUDED"
RULE_IGNORE_PATTERN = "RULE_IGNORE_PATTERN"
