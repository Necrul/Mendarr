from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_SECRET_KEY = "dev-insecure-mendarr-secret-key-min-32chars!"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MENDARR_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(default="0.0.0.0", description="Bind host")
    port: int = Field(default=8095, ge=1, le=65535)
    secret_key: str = Field(
        default=DEFAULT_SECRET_KEY,
        min_length=16,
        description="Session & CSRF signing",
    )
    encryption_key: str = Field(
        default="",
        description="Optional separate server-side key seed for encrypting integration secrets at rest",
    )
    data_dir: Path = Field(default=Path("./data"))
    database_url: str = Field(
        default="sqlite+aiosqlite:///./data/mendarr.db",
        description="SQLAlchemy async URL",
    )

    admin_username: str = "admin"
    admin_password: str = Field(default="", description="Initial password if no DB admin")
    admin_password_hash: str = Field(default="", description="PBKDF2 hash (optional)")
    admin_password_salt: str = Field(default="", description="Salt for admin hash")

    ffprobe_path: str = Field(default="ffprobe")
    mediainfo_path: str = Field(default="")
    scan_concurrency: int = Field(default=4, ge=1, le=32)
    scan_precount_enabled: bool = Field(
        default=False,
        description="Perform an upfront full-library file count before scanning for exact progress totals",
    )
    scan_path_hints: str = Field(
        default="",
        description="Optional semicolon or comma separated absolute paths to surface as likely scan roots",
    )
    path_mappings: str = Field(
        default="",
        description="Optional semicolon separated path mappings in source=>target format for cross-host library paths",
    )

    min_tv_size_bytes: int = Field(default=50_000, ge=0)
    min_movie_size_bytes: int = Field(default=100_000, ge=0)
    min_duration_tv_seconds: float = Field(default=60.0, ge=0)
    min_duration_movie_seconds: float = Field(default=300.0, ge=0)
    auto_remediation_enabled: bool = False

    log_level: str = "INFO"
    timezone: str = Field(default="UTC")

    cors_origins: str = ""

    rate_limit_remediation: str = "20/minute"
    trust_proxy_headers: bool = Field(
        default=False,
        description="Trust X-Forwarded-* headers from a known reverse proxy",
    )
    app_version: str = Field(default="1.0.1", description="Application version label")
    public_repo: str = Field(
        default="necrul/Mendarr",
        description="Public GitHub repo slug or URL used for update checks",
    )
    update_check_enabled: bool = Field(default=True)
    update_check_interval_hours: int = Field(default=12, ge=1, le=168)

    @field_validator("data_dir", mode="before")
    @classmethod
    def expand_data_dir(cls, v):
        if isinstance(v, str):
            return Path(v)
        return v


def validate_runtime_secret_key(secret_key: str) -> None:
    if len(secret_key) < 16:
        raise RuntimeError("MENDARR_SECRET_KEY must be at least 16 characters long")
    if secret_key == DEFAULT_SECRET_KEY:
        raise RuntimeError(
            "MENDARR_SECRET_KEY is still using Mendarr's built-in fallback value. "
            "Set a unique secret before starting the app."
        )


@lru_cache
def get_settings() -> Settings:
    s = Settings()
    s.data_dir.mkdir(parents=True, exist_ok=True)
    if "sqlite" in s.database_url and "./data/" in s.database_url:
        s.data_dir.mkdir(parents=True, exist_ok=True)
    return s
