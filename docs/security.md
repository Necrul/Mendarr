# Security

Mendarr is intended for self-hosted operators who want audit visibility without turning the app into a file owner. The security model follows that boundary.

## Secret handling

- `MENDARR_SECRET_KEY` signs session cookies and CSRF tokens.
- `MENDARR_ENCRYPTION_KEY` can be set to a separate server-side key seed for encrypting integration secrets at rest. If unset, Mendarr derives encryption from `MENDARR_SECRET_KEY`.
- Sonarr and Radarr API keys remain server-side only.
- The UI only shows masked API key placeholders when editing integrations.
- Integration API keys are encrypted at rest before being written to SQLite and only decrypted server-side when Mendarr needs to call Sonarr or Radarr.
- Legacy plaintext integration keys are migrated to encrypted storage on startup.
- Logging redacts common secret-bearing fields such as API keys, bearer tokens, passwords, and the Mendarr session cookie before emitting messages.

## Authentication

- The UI uses a signed session cookie with `HttpOnly` and `SameSite=lax`.
- The session cookie is marked `Secure` when the request reaches Mendarr over HTTPS, or when `MENDARR_TRUST_PROXY_HEADERS=true` and the trusted proxy sets `X-Forwarded-Proto=https`.
- Mendarr only trusts `X-Forwarded-*` proxy headers when `MENDARR_TRUST_PROXY_HEADERS=true`.
- Anonymous users are redirected to `/login`.
- API-style paths under `/api/` return `401` instead of HTML redirects.
- Sensitive settings and remediation routes are protected by authentication and CSRF checks; Mendarr does not expose unauthenticated configuration-write endpoints.
- The first admin account is seeded from environment variables only when no user exists yet.
- Login is rate-limited to reduce low-effort password spraying.

## CSRF protection

- All state-changing form posts include a signed CSRF token.
- Tokens are timestamped and validated server-side.
- State-changing routes reject missing or invalid tokens.

## Path safety

- Scans only walk configured library roots.
- Local root paths entered in the UI are resolved and must exist as directories.
- Path-based exclusions and ignore patterns are applied during scan scoring.
- Recommended deployment mounts media libraries read-only unless there is a specific reason not to.

## Integration boundaries

- Mendarr only talks to explicitly configured Sonarr and Radarr hosts.
- Remediation actions go through the Sonarr and Radarr HTTP APIs.
- Mendarr does not delete files on disk in v1.

## Rate limiting

- Scan and remediation endpoints are rate-limited with SlowAPI.
- Bulk actions are also limited to reduce accidental repeated job creation.

## Deployment guidance

- Prefer a reverse proxy with TLS if exposing the UI remotely.
- For local-only access, bind to `127.0.0.1`.
- Keep the SQLite database volume private.
- Rotate the secret key and admin password before exposing the app outside a trusted LAN.
- Keep `MENDARR_TRUST_PROXY_HEADERS=false` unless the proxy in front of Mendarr is trusted and strips incoming spoofed forwarding headers.

## Related docs

- [Configuration](configuration.md)
- [Architecture](architecture.md)
