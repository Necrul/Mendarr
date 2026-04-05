# Configuration

This document covers the runtime knobs that matter when you move Mendarr from local testing into a real self-hosted deployment.

All environment variables use the `MENDARR_` prefix. An optional `.env` file in the repository root is supported.

## Core settings

| Variable | Description | Default |
|----------|-------------|---------|
| `MENDARR_HOST` | Bind host | `0.0.0.0` |
| `MENDARR_PORT` | Bind port | `8095` |
| `MENDARR_SECRET_KEY` | Session and CSRF signing secret | required in production |
| `MENDARR_ENCRYPTION_KEY` | Optional separate key seed for encrypting integration secrets at rest | empty |
| `MENDARR_DATA_DIR` | Data directory for SQLite and local state | `./data` |
| `MENDARR_DATABASE_URL` | SQLAlchemy async database URL | SQLite in `data_dir` |
| `MENDARR_LOG_LEVEL` | Logging level | `INFO` |
| `MENDARR_TIMEZONE` | Operator timezone label | `UTC` |
| `MENDARR_SCAN_PATH_HINTS` | Optional scan-root hints shown in Setup | empty |
| `MENDARR_PATH_MAPPINGS` | Optional `source=>target` path mappings for cross-host library paths | empty |

## Admin bootstrap

| Variable | Description | Default |
|----------|-------------|---------|
| `MENDARR_ADMIN_USERNAME` | Initial admin username | `admin` |
| `MENDARR_ADMIN_PASSWORD` | Initial plaintext password used to seed the first admin | empty |
| `MENDARR_ADMIN_PASSWORD_HASH` | Precomputed password hash for initial admin bootstrap | empty |
| `MENDARR_ADMIN_PASSWORD_SALT` | Salt paired with `MENDARR_ADMIN_PASSWORD_HASH` | empty |

Use either plaintext password bootstrap or hash plus salt bootstrap. If no admin exists and none of these are set, the UI will start without creating a default admin account.

## Scanner and media validation

| Variable | Description | Default |
|----------|-------------|---------|
| `MENDARR_FFPROBE_PATH` | `ffprobe` binary path | `ffprobe` |
| `MENDARR_MEDIAINFO_PATH` | Optional `mediainfo` path | empty |
| `MENDARR_SCAN_CONCURRENCY` | Reserved concurrency knob for future scan parallelism | `4` |
| `MENDARR_SCAN_PRECOUNT_ENABLED` | Count every media file before scanning to show exact progress totals; disable to avoid the extra filesystem walk | `false` |
| `MENDARR_MIN_TV_SIZE_BYTES` | Initial TV size threshold before rule settings exist | `50000` |
| `MENDARR_MIN_MOVIE_SIZE_BYTES` | Initial movie size threshold before rule settings exist | `100000` |
| `MENDARR_MIN_DURATION_TV_SECONDS` | Initial TV duration threshold before rule settings exist | `60` |
| `MENDARR_MIN_DURATION_MOVIE_SECONDS` | Initial movie duration threshold before rule settings exist | `300` |
| `MENDARR_AUTO_REMEDIATION_ENABLED` | Default automation flag before persisted rule settings exist | `false` |

## Web and rate limiting

| Variable | Description | Default |
|----------|-------------|---------|
| `MENDARR_CORS_ORIGINS` | Reserved for deployments that need explicit CORS | empty |
| `MENDARR_RATE_LIMIT_REMEDIATION` | Informational operator setting for remediation rate limits | `20/minute` |
| `MENDARR_TRUST_PROXY_HEADERS` | Trust `X-Forwarded-*` headers from a known reverse proxy | `false` |
| `MENDARR_APP_VERSION` | App version label shown in the UI | `1.0.0` |
| `MENDARR_PUBLIC_REPO` | Public GitHub repo slug or URL used for update checks | `necrul/Mendarr` |
| `MENDARR_UPDATE_CHECK_ENABLED` | Enable release checks against the public repo | `true` |
| `MENDARR_UPDATE_CHECK_INTERVAL_HOURS` | Cache lifetime for repo update checks | `12` |

## Persisted UI-managed configuration

These settings are stored in the database and managed from the UI:

- Sonarr base URL and API key
- Radarr base URL and API key
- Root path mappings
- Rule thresholds
- Excluded keywords
- Extras keywords
- Excluded paths
- Ignore patterns
- Auto-remediation toggle

## Notes

- The Docker image also uses port `8095` by default.
- Mendarr refuses to start if `MENDARR_SECRET_KEY` is still set to the built-in fallback value.
- Sonarr and Radarr API keys are encrypted before being persisted in the database.
- If `MENDARR_ENCRYPTION_KEY` is unset, Mendarr derives an encryption key from `MENDARR_SECRET_KEY`.
- When Mendarr and Sonarr or Radarr see the same absolute path, use the same path directly instead of creating a second container-only path.
- `MENDARR_PATH_MAPPINGS` accepts semicolon-separated `source=>target` rules such as `X:\=>/mnt/media`.
- `MENDARR_SCAN_PRECOUNT_ENABLED=false` avoids an up-front directory walk, which reduces duplicate filesystem churn and is usually the better choice for very large libraries.
- `MENDARR_TRUST_PROXY_HEADERS` should stay `false` unless Mendarr is behind a reverse proxy that you control.
- Delete-and-replace is a supported manager-owned action. Keep Mendarr mounted read-only and let Sonarr or Radarr handle the deletion and replacement workflow.

## Related docs

- [Architecture](architecture.md)
- [Security](security.md)
