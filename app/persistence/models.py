from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class IntegrationConfig(Base):
    __tablename__ = "integration_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kind: Mapped[str] = mapped_column(String(32), index=True)
    name: Mapped[str] = mapped_column(String(128), default="default")
    base_url: Mapped[str] = mapped_column(String(1024), default="")
    api_key: Mapped[str] = mapped_column(Text, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: dt.datetime.now(dt.UTC),
        onupdate=lambda: dt.datetime.now(dt.UTC),
    )


class LibraryRoot(Base):
    __tablename__ = "library_root"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    manager_kind: Mapped[str] = mapped_column(String(16), index=True)
    manager_root_path: Mapped[str] = mapped_column(String(2048))
    local_root_path: Mapped[str] = mapped_column(String(2048))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: dt.datetime.now(dt.UTC),
        onupdate=lambda: dt.datetime.now(dt.UTC),
    )


class AppUser(Base):
    __tablename__ = "app_user"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(256))
    password_salt: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))


class ScanRun(Base):
    __tablename__ = "scan_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
    completed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="running", index=True)
    files_seen: Mapped[int] = mapped_column(Integer, default=0)
    suspicious_found: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    findings: Mapped[list[Finding]] = relationship(back_populates="last_scan_run")


class Finding(Base):
    __tablename__ = "finding"
    __table_args__ = (UniqueConstraint("file_path", name="uq_finding_file_path"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_path: Mapped[str] = mapped_column(String(4096), index=True)
    file_name: Mapped[str] = mapped_column(String(1024), default="")
    media_kind: Mapped[str] = mapped_column(String(16), index=True)
    manager_kind: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    manager_entity_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    season_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    episode_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    file_size_bytes: Mapped[int] = mapped_column(Integer, default=0)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    resolution: Mapped[str | None] = mapped_column(String(32), nullable=True)
    codec_video: Mapped[str | None] = mapped_column(String(64), nullable=True)
    codec_audio: Mapped[str | None] = mapped_column(String(64), nullable=True)
    suspicion_score: Mapped[int] = mapped_column(Integer, default=0, index=True)
    confidence: Mapped[str] = mapped_column(String(16), default="low", index=True)
    proposed_action: Mapped[str] = mapped_column(String(32), default="review", index=True)
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)
    ignored: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    first_seen_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
    last_seen_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
    last_scanned_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
    last_scan_run_id: Mapped[int | None] = mapped_column(ForeignKey("scan_run.id"), nullable=True)
    ffprobe_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    reasons: Mapped[list[FindingReason]] = relationship(back_populates="finding", cascade="all, delete-orphan")
    jobs: Mapped[list[RemediationJob]] = relationship(back_populates="finding")
    last_scan_run: Mapped[ScanRun | None] = relationship(back_populates="findings")


class FindingReason(Base):
    __tablename__ = "finding_reason"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    finding_id: Mapped[int] = mapped_column(ForeignKey("finding.id", ondelete="CASCADE"), index=True)
    code: Mapped[str] = mapped_column(String(64), index=True)
    message: Mapped[str] = mapped_column(String(512))
    severity: Mapped[str] = mapped_column(String(16), default="info")

    finding: Mapped[Finding] = relationship(back_populates="reasons")


class RemediationJob(Base):
    __tablename__ = "remediation_job"
    __table_args__ = (
        Index(
            "ux_remediation_job_active",
            "finding_id",
            "action_type",
            unique=True,
            sqlite_where=text("status IN ('queued', 'running')"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    finding_id: Mapped[int] = mapped_column(ForeignKey("finding.id"), index=True)
    action_type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    requested_by: Mapped[str] = mapped_column(String(128), default="ui")
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
    started_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    finding: Mapped[Finding] = relationship(back_populates="jobs")
    attempts: Mapped[list[RemediationAttempt]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
    )


class RemediationAttempt(Base):
    __tablename__ = "remediation_attempt"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("remediation_job.id", ondelete="CASCADE"), index=True)
    step_name: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16))
    request_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    response_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))

    job: Mapped[RemediationJob] = relationship(back_populates="attempts")


class RuleException(Base):
    __tablename__ = "rule_exception"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    path_pattern: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    title_pattern: Mapped[str | None] = mapped_column(String(512), nullable=True)
    manager_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    media_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    action_override: Mapped[str | None] = mapped_column(String(32), nullable=True)
    ignore_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))


class AuditEvent(Base):
    __tablename__ = "audit_event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    message: Mapped[str] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
    actor: Mapped[str | None] = mapped_column(String(128), nullable=True)


class RuleSettingsRow(Base):
    """Singleton-style persisted thresholds (one row with id=1)."""

    __tablename__ = "rule_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    min_tv_size_bytes: Mapped[int] = mapped_column(Integer, default=50_000)
    min_movie_size_bytes: Mapped[int] = mapped_column(Integer, default=100_000)
    min_duration_tv_seconds: Mapped[float] = mapped_column(Float, default=60.0)
    min_duration_movie_seconds: Mapped[float] = mapped_column(Float, default=300.0)
    excluded_keywords: Mapped[str] = mapped_column(Text, default="")  # comma-separated
    extras_keywords: Mapped[str] = mapped_column(Text, default="")  # comma-separated
    excluded_paths: Mapped[str] = mapped_column(Text, default="")  # newline-separated
    ignored_patterns: Mapped[str] = mapped_column(Text, default="")  # newline glob
    auto_remediation_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), default=lambda: dt.datetime.now(dt.UTC))
