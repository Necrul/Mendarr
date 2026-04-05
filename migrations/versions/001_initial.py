"""initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-30

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "integration_config",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("base_url", sa.String(length=1024), nullable=False),
        sa.Column("api_key", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_integration_config_kind", "integration_config", ["kind"])
    op.create_table(
        "library_root",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("manager_kind", sa.String(length=16), nullable=False),
        sa.Column("manager_root_path", sa.String(length=2048), nullable=False),
        sa.Column("local_root_path", sa.String(length=2048), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_library_root_manager_kind", "library_root", ["manager_kind"])
    op.create_table(
        "app_user",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("username", sa.String(length=128), nullable=False),
        sa.Column("password_hash", sa.String(length=256), nullable=False),
        sa.Column("password_salt", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_app_user_username", "app_user", ["username"], unique=True)
    op.create_table(
        "scan_run",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("files_seen", sa.Integer(), nullable=False),
        sa.Column("suspicious_found", sa.Integer(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_scan_run_status", "scan_run", ["status"])
    op.create_table(
        "rule_settings",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("min_tv_size_bytes", sa.Integer(), nullable=False),
        sa.Column("min_movie_size_bytes", sa.Integer(), nullable=False),
        sa.Column("min_duration_tv_seconds", sa.Float(), nullable=False),
        sa.Column("min_duration_movie_seconds", sa.Float(), nullable=False),
        sa.Column("excluded_keywords", sa.Text(), nullable=False),
        sa.Column("extras_keywords", sa.Text(), nullable=False),
        sa.Column("excluded_paths", sa.Text(), nullable=False),
        sa.Column("ignored_patterns", sa.Text(), nullable=False),
        sa.Column("auto_remediation_enabled", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "rule_exception",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("path_pattern", sa.String(length=1024), nullable=True),
        sa.Column("title_pattern", sa.String(length=512), nullable=True),
        sa.Column("manager_kind", sa.String(length=16), nullable=True),
        sa.Column("media_kind", sa.String(length=16), nullable=True),
        sa.Column("action_override", sa.String(length=32), nullable=True),
        sa.Column("ignore_flag", sa.Boolean(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "audit_event",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("entity_id", sa.String(length=64), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_audit_event_event_type", "audit_event", ["event_type"])
    op.create_index("ix_audit_event_entity_type", "audit_event", ["entity_type"])
    op.create_index("ix_audit_event_entity_id", "audit_event", ["entity_id"])
    op.create_table(
        "finding",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("file_path", sa.String(length=4096), nullable=False),
        sa.Column("file_name", sa.String(length=1024), nullable=False),
        sa.Column("media_kind", sa.String(length=16), nullable=False),
        sa.Column("manager_kind", sa.String(length=16), nullable=True),
        sa.Column("manager_entity_id", sa.String(length=64), nullable=True),
        sa.Column("title", sa.String(length=512), nullable=True),
        sa.Column("season_number", sa.Integer(), nullable=True),
        sa.Column("episode_number", sa.Integer(), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("file_size_bytes", sa.Integer(), nullable=False),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("resolution", sa.String(length=32), nullable=True),
        sa.Column("codec_video", sa.String(length=64), nullable=True),
        sa.Column("codec_audio", sa.String(length=64), nullable=True),
        sa.Column("suspicion_score", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.String(length=16), nullable=False),
        sa.Column("proposed_action", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("ignored", sa.Boolean(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_scanned_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_scan_run_id", sa.Integer(), nullable=True),
        sa.Column("ffprobe_json", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["last_scan_run_id"], ["scan_run.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("file_path", name="uq_finding_file_path"),
    )
    op.create_index("ix_finding_file_path", "finding", ["file_path"])
    op.create_index("ix_finding_media_kind", "finding", ["media_kind"])
    op.create_index("ix_finding_manager_kind", "finding", ["manager_kind"])
    op.create_index("ix_finding_manager_entity_id", "finding", ["manager_entity_id"])
    op.create_index("ix_finding_suspicion_score", "finding", ["suspicion_score"])
    op.create_index("ix_finding_confidence", "finding", ["confidence"])
    op.create_index("ix_finding_proposed_action", "finding", ["proposed_action"])
    op.create_index("ix_finding_status", "finding", ["status"])
    op.create_index("ix_finding_ignored", "finding", ["ignored"])
    op.create_table(
        "finding_reason",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("finding_id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("message", sa.String(length=512), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.ForeignKeyConstraint(["finding_id"], ["finding.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_finding_reason_finding_id", "finding_reason", ["finding_id"])
    op.create_index("ix_finding_reason_code", "finding_reason", ["code"])
    op.create_table(
        "remediation_job",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("finding_id", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("requested_by", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["finding_id"], ["finding.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_remediation_job_finding_id", "remediation_job", ["finding_id"])
    op.create_index("ix_remediation_job_status", "remediation_job", ["status"])
    op.create_table(
        "remediation_attempt",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("step_name", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("request_summary", sa.Text(), nullable=True),
        sa.Column("response_summary", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["remediation_job.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_remediation_attempt_job_id", "remediation_attempt", ["job_id"])


def downgrade() -> None:
    op.drop_table("remediation_attempt")
    op.drop_table("remediation_job")
    op.drop_table("finding_reason")
    op.drop_table("finding")
    op.drop_table("audit_event")
    op.drop_table("rule_exception")
    op.drop_table("rule_settings")
    op.drop_table("scan_run")
    op.drop_table("app_user")
    op.drop_table("library_root")
    op.drop_table("integration_config")
