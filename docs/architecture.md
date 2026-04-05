# Architecture

Mendarr is a single-host, self-hosted audit service for Sonarr and Radarr libraries. The architecture is intentionally conservative: a server-rendered web UI, SQLite persistence, local media probing, and Arr-owned remediation boundaries.

## Major components

- **Web layer** (`app/main.py`, `app/api/*`): FastAPI routers for HTML pages and form posts; static assets and Jinja templates; auth middleware; SlowAPI rate limits on heavy endpoints.
- **Domain** (`app/domain/*`): Enums, scoring rules, path/filename TV and movie parsing helpers, small policy constants.
- **Integrations** (`app/integrations/*`): httpx-based Sonarr/Radarr v3 clients; ffprobe subprocess adapter; optional mediainfo stub.
- **Services** (`app/services/*`): Scan orchestration, finding upsert, matching (via *arr metadata), remediation execution, jobs, rules, audit logging, admin bootstrap.
- **Persistence** (`app/persistence/*`): SQLAlchemy models, async SQLite engine/session, `get_db` dependency.

## Boundaries

- **Mendarr never talks to the filesystem outside configured library roots** except through validated paths derived from those roots.
- **All automation toward repair goes through Sonarr/Radarr HTTP APIs** (`/api/v3/command`, series/movie metadata, parse endpoints). No direct file deletes.
- **Secrets** (API keys) are stored in SQLite and never rendered in full in the UI (masked placeholders).

## Why this stack

- **FastAPI + Jinja + HTMX** keeps a dynamic admin UI without a separate SPA build chain.
- **SQLite** is sufficient for single-host audit data volume in v1 and simplifies backups (one file under `MENDARR_DATA_DIR`).
- **ffprobe** is the industry-standard probe for stream sanity checks without bundling heavy ML.

## Audit-first, non-destructive design

Every scan and remediation step can emit **audit_events**. Findings retain reasons and scores for forensics. Remediation jobs record **attempts** with API response summaries (truncated) so operators can see what was asked of Sonarr/Radarr without guessing.

## Related docs

- [Configuration](configuration.md)
- [Security](security.md)
- [Remediation flow](remediation-flow.md)
