# Remediation Flow

Mendarr treats remediation as a controlled queue, not an immediate file mutation. The app decides what looks suspicious, but Sonarr and Radarr remain the systems that actually perform repairs.

## Lifecycle

1. A scan creates or updates a finding.
2. Mendarr determines a proposed action.
3. An operator queues the job manually, or Mendarr auto-queues it when high-confidence automation is enabled.
4. The background worker picks the next queued job.
5. Mendarr calls the matching Sonarr or Radarr command.
6. Rescan-only jobs immediately re-probe the local file. Search and delete-and-search jobs await an operator-triggered verify scan or a subsequent library scan.
7. A readable, successfully checked file can clear its finding. A missing or unreadable verification target is skipped, not considered repaired. Ignored findings remain ignored until explicitly restored.

The worker commits its job claim before sending a manager request. If Mendarr restarts with a job still running, the job is marked failed with an unknown-outcome message. It is not automatically replayed; verify the finding before requesting another action. Run a single Mendarr application process per database.

If a finding is already resolved or ignored when its job reaches the worker, the job is cancelled before contacting the manager.

If an operator stops a library scan, Mendarr finishes the current file and then marks the scan `interrupted` instead of dropping the run abruptly. The dashboard exposes a `Resume scan` action for interrupted library runs, and that resume continues from the last completed file instead of restarting from the top.

## Sonarr actions

- `rescan_only` -> `RescanSeries`
- `search_replacement` -> `EpisodeSearch`

## Radarr actions

- `rescan_only` -> `RefreshMovie`
- `search_replacement` -> `MoviesSearch`

## Accepted request and verification

- Job status becomes `succeeded` when the manager accepts the requests; this is not proof of a completed download or import
- A remediation attempt row is written
- An audit event is recorded
- A verify scan or library scan re-scores the current file; a library scan also refreshes an existing finding when a file has become healthy
- Findings below the scan persistence threshold are resolved after that check, except explicitly ignored findings

Delete-and-replace first checks the manager's current file path (using configured root mappings) and size against the finding. A changed or unidentifiable file is refused with a request to verify again. Deletion is recorded before attempting the replacement search, so a search timeout retains the successful deletion step. The combined delete/search operation is never automatically retried inside a job.

Path and size checks are conservative guards, not a content fingerprint. They cannot distinguish same-path, same-size replacements or eliminate a manager-side race between inspection and deletion.

## Unresolved path

If the file still looks suspicious after refresh or search:

- the job still completes as an executed command
- the finding stays open for manual review
- the audit trail preserves what Mendarr asked Sonarr or Radarr to do

## Failure path

If the manager is not configured, the entity id is missing, or the API call fails:

- job status becomes `failed`
- `last_error` is populated
- a remediation attempt row is written with failure context
- an audit event is recorded

## Non-destructive guarantee

Mendarr does not delete media files directly in v1. Repair remains manager-owned.

## Related docs

- [Architecture](architecture.md)
- [Scoring engine](scoring-engine.md)
