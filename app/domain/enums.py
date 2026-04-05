from enum import Enum


class MediaKind(str, Enum):
    TV = "tv"
    MOVIE = "movie"
    UNKNOWN = "unknown"


class ManagerKind(str, Enum):
    SONARR = "sonarr"
    RADARR = "radarr"
    NONE = "none"


class FindingStatus(str, Enum):
    OPEN = "open"
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    IGNORED = "ignored"
    FAILED = "failed"


class Confidence(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ProposedAction(str, Enum):
    IGNORE = "ignore"
    REVIEW = "review"
    RESCAN_ONLY = "rescan_only"
    SEARCH_REPLACEMENT = "search_replacement"
    BLOCKED = "blocked"


class RemediationAction(str, Enum):
    RESCAN_ONLY = "rescan_only"
    SEARCH_REPLACEMENT = "search_replacement"
    DELETE_SEARCH_REPLACEMENT = "delete_search_replacement"


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ScanRunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class IntegrationKind(str, Enum):
    SONARR = "sonarr"
    RADARR = "radarr"


class AttemptStatus(str, Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
