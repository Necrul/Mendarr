# Mendarr review and reliability improvements

Reviewed from commit `03e9f06` on branch `codex/reliability-review`. Astra performed the investigation, implementation, and final review directly.

## Intended product

Mendarr is a media-library integrity auditor. It should inspect media visible through configured roots, identify credible problems, associate each finding with Sonarr or Radarr, preserve operator decisions, request manager-owned remediation, and establish whether the media is healthy afterward. Its most important promise is trustworthy findings and repair outcomes, especially across long scans and restarts.

The current FastAPI/Jinja/SQLite design is a reasonable foundation for a single-host tool. A rewrite is not required to fix the defects found here. The main weakness was lifecycle correctness and missing failure-path coverage, rather than a lack of screens or settings.

## Defects addressed

| Area | Previous behavior | Change |
| --- | --- | --- |
| Library rescans | Healthy files returned early, leaving old findings open indefinitely | Existing findings receive fresh probe evidence and resolve when healthy |
| Background verification | Progress commits detached the source finding; later status edits could be silently lost | Reload the source in the active session before persisting the outcome |
| Missing/unreadable targets | A failed stat was returned as a zero-score healthy result | Record a skipped target and preserve its unresolved state |
| Ignore decisions | A subsequent upsert cleared the ignored flag and could allow automatic repair | Preserve ignore decisions; automatic queueing checks the persisted flag; rule-ignore actions also suppress findings |
| Manager outages | An unsuccessful match could erase the previously stored manager link | Preserve existing manager identity when no replacement link was established |
| Verification candidates | A local alternate could be preferred over the original still-existing file; a fallback could choose an unrelated same-extension neighbor | Prefer the original before heuristic alternatives; remove the arbitrary-neighbor fallback |
| Resume checkpoint | Progress could commit before the current suspicious finding was saved | Save the completed-file checkpoint with the file's changes, and use stable traversal/root ordering |
| Probe installation failure | Missing ffprobe became a critical media finding for every file | Fail the operation as an unavailable-tool error; retain ordinary bad-container findings |
| Delete-and-replace | The manager's latest file was deleted without comparing it to the scanned file | Check mapped path and size first; refuse stale or incomplete metadata |
| Partial repair failure | A search exception could lose the successful deletion attempt and trigger a retry of the combined operation | Record deletion immediately in the job transaction; do not retry the destructive sequence |
| Restarted repair jobs | An uncommitted running state could roll back to queued after an external request | Commit the claim first; recover interrupted jobs as failed with an unknown outcome |
| Obsolete queued work | A repair could run after its finding had been resolved or ignored | Cancel before making any manager requests |
| Manager queue parsing | Normal object responses returned an empty list; list responses attempted dict access | Handle both response shapes and test nonempty queues |
| Release checks | Container publication had no test dependency | Add Linux/Python 3.12 and Windows/Python 3.13 CI; require tests before publication |

No production media was scanned or deleted. Manager operations were exercised through controlled stubs. The existing application data was not migrated or modified.

## Verification

The original suite passed **143 tests**, with a background-thread teardown warning. Seven initial regression cases then produced **six failures**, demonstrating stale findings, lost ignore decisions, lost background updates, false resolution on two filesystem errors, and incorrect verification-target selection.

New coverage includes isolated SQLite sessions, background progress commits, renamed replacements, ignored findings with automation enabled, interruption before a finding is saved, durable job claims, restart recovery, current-file deletion guards for both managers, search timeouts, queue response shapes, and actual ffmpeg-generated media probed with ffprobe. Existing scan tests now wait for background cleanup before closing their event loops. A shared test fixture's scan deletion also respects the finding foreign key now that verification correctly persists it.

Commands run from the repository root:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m pip check
git diff --check
```

Final local result: **180 passed in 54.61 seconds, without warnings** (37 additional cases). `pip check` reported no broken requirements, `git diff --check` passed, and both workflow YAML files parsed successfully. Real-media tests ran successfully; they were not skipped on this machine.

At the time of the local review, the new GitHub Actions matrix had not run remotely. Docker's CLI was installed, but its Linux daemon was unavailable, so a local container build/runtime check was not completed. No live Sonarr/Radarr calls or large-library performance benchmark were performed. Release validation is available in the repository's GitHub Actions runs.

## Remaining work, in recommended order

1. **Database upgrade lifecycle.** Startup calls `metadata.create_all`; it does not run Alembic upgrades. Existing databases therefore do not automatically acquire a newly added index or schema change. Alembic's environment also takes its URL from the INI rather than the application's resolved configuration. Establish and test adoption of pre-Alembic databases, backups, duplicate-job cleanup, and upgrades before introducing schema-dependent features.
2. **Repair completion tracking.** Search acceptance is not a completed repair. Poll manager command/import outcomes with bounded deadlines, then schedule verification when an import is actually present. A search may not replace a broken file that already meets the manager's quality cutoff. Preserve the distinction between request accepted, imported, and media verified.
3. **Shorter write transactions.** Repair execution still holds a database transaction while making external requests. A slow manager can block other SQLite writers. Separate durable step records from network waits without allowing automatic replay of uncertain destructive steps.
4. **Large-library execution.** `scan_concurrency` is declared but unused. Traversal and sibling inspection are synchronous, every scan probes every file, and duplicate detection repeatedly lists directories. Add bounded probing, directory-level caching, and file-signature-based incremental scans with measurements on representative libraries. Persist a manifest if resume must be exact while files are added, removed, or renamed; the current path checkpoint assumes a largely stable library.
5. **Actual playback integrity.** ffprobe reads container/stream metadata; it is not a full decode test. Add an optional bounded or full ffmpeg decode mode, distinguish a timeout from proven corruption, and handle attached artwork and unusual stream metadata explicitly. Do not claim that metadata-only scans establish complete playback health.
6. **Clarify extras detection.** The README/scoring documentation still describe filename/extras signals, but scoring currently does not use those keyword inputs, and an existing test explicitly expects no finding from extras keywords. Decide and document a conservative opt-in behavior with false-positive fixtures before restoring it. Titles that contain words such as “Interview” must not be treated as corrupt solely on that basis.
7. **Stronger identity and matching.** Filename parsing and fuzzy manager matching remain imperfect for anime, multi-episode files, editions, and renamed titles. Prefer verified manager file identities and persist a content/change fingerprint for destructive requests. Path-plus-size checks cannot detect every replacement. Verification still has filename/title-based fallback heuristics and needs representative live-library validation.
8. **Operational validation.** Run container startup and persistent-volume upgrade checks, a read-only pilot against real Sonarr/Radarr instances, and a large-library benchmark. Review dependency pins before a release. Several configuration fields, including concurrency and remediation rate settings, do not currently control their advertised behavior.

These are remaining product and engineering gaps, not claims that this branch delivers 100% library integrity or has passed live-manager acceptance testing.
