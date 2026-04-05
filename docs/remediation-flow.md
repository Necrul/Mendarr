# Remediation Flow

Mendarr treats remediation as a controlled queue, not an immediate file mutation. The app decides what looks suspicious, but Sonarr and Radarr remain the systems that actually perform repairs.

## Lifecycle

1. A scan creates or updates a finding.
2. Mendarr determines a proposed action.
3. An operator queues the job manually, or Mendarr auto-queues it when high-confidence automation is enabled.
4. The background worker picks the next queued job.
5. Mendarr calls the matching Sonarr or Radarr command.
6. Mendarr re-probes the file and refreshes the finding record.
7. The finding is left open, marked ignored, or marked resolved depending on the re-check outcome.

If an operator stops a library scan, Mendarr finishes the current file and then marks the scan `interrupted` instead of dropping the run abruptly. The dashboard exposes a `Resume scan` action for interrupted library runs, and that resume continues from the last completed file instead of restarting from the top.

## Sonarr actions

- `rescan_only` -> `RescanSeries`
- `search_replacement` -> `EpisodeSearch`

## Radarr actions

- `rescan_only` -> `RefreshMovie`
- `search_replacement` -> `MoviesSearch`

## Success path

- Job status becomes `succeeded`
- A remediation attempt row is written
- An audit event is recorded
- The finding is re-scored from current file state
- If the score drops below the low threshold, the finding is marked `resolved`

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
