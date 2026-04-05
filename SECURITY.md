# Security Policy

## Supported Version

The current public baseline is `v1.0.0`.

## Reporting A Vulnerability

If you believe you found a security issue in Mendarr:

1. Do not open a public issue with exploit details.
2. Send a private report with reproduction steps, impact, and affected version.
3. Include whether the issue requires local access, authenticated access, or only network reachability.

If you are using this project from the public repo and need an initial contact point, start with the support link in the main [README.md](README.md).

## Deployment Notes

- Keep Mendarr behind a trusted reverse proxy if exposing it remotely.
- Leave `MENDARR_TRUST_PROXY_HEADERS=false` unless the proxy is controlled by you and strips spoofed forwarding headers.
- Rotate `MENDARR_SECRET_KEY`, `MENDARR_ENCRYPTION_KEY`, and the admin password before wider exposure.
- Mount media libraries read-only unless you explicitly need a different model.
