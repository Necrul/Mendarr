# Scoring Engine

Mendarr uses a deterministic scoring model rather than a black-box heuristic. Operators should be able to understand why a file was flagged and what the proposed next step means.

## Inputs

Mendarr combines multiple signals rather than relying on file size alone.

- Filesystem size
- File extension
- Filename and path keywords
- Duplicate file variants in the same folder
- `ffprobe` success or failure
- Stream presence and codec metadata
- Duration and resolution
- Bitrate sanity checks
- Manager match state
- Persisted rule settings

## Rule inputs

The persisted rule settings influence scoring and action mapping:

- minimum TV size
- minimum movie size
- minimum TV duration
- minimum movie duration
- excluded keywords
- extras keywords override
- excluded paths
- ignored glob patterns
- auto-remediation enabled flag

Excluded keywords suppress matching extras keywords when operators want to ignore specific words.

## Stable reason codes

Examples:

- `FS_ZERO_BYTE`
- `FS_VERY_SMALL`
- `FS_BAD_EXTENSION`
- `FS_DUPLICATE_VARIANT`
- `FS_KEYWORD_EXTRA`
- `MD_PROBE_FAILED`
- `MD_NO_VIDEO_STREAM`
- `MD_NO_DURATION`
- `MD_SHORT_DURATION`
- `MD_NO_RESOLUTION`
- `MD_NO_VIDEO_CODEC`
- `MD_NO_AUDIO_EXPECTED`
- `MD_BITRATE_ANOMALY`
- `RULE_PATH_EXCLUDED`
- `RULE_IGNORE_PATTERN`
- `CTX_TRAILER_IN_MAIN`

The canonical definitions live in `app/domain/value_objects.py`.

## Score calculation

- Each matched signal contributes a fixed weight.
- The final score is capped at `100`.
- Higher score means more concern, not a probability percentage.

## Confidence mapping

- `high`: critical signal present or score `>= 85`
- `medium`: score `>= 45` or at least one warning signal
- `low`: everything else

## Recommended action mapping

- Ignore signal present: `ignore`
- No manager match: `review`
- High confidence with extras-style keywords: `search_replacement`
- High confidence with manager match: `rescan_only`, or `search_replacement` when auto-remediation is enabled
- Medium confidence with manager match: `rescan_only`
- Low confidence: `review`

## Auto-remediation behavior

When persisted rules enable auto-remediation, Mendarr will automatically queue a remediation job only when all of the following are true:

- confidence is `high`
- the finding is matched to Sonarr or Radarr
- the finding is not ignored by an exception
- the proposed action is `rescan_only` or `search_replacement`

## Related docs

- [Architecture](architecture.md)
- [Remediation flow](remediation-flow.md)
