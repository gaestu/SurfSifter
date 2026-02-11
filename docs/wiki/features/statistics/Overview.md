# Statistics (Tab)

Statistics Tab - Summary cards showing extractor run statistics.

## Purpose
- Summarize extractor run status and counts for the current evidence.
- Quickly spot failures, partial runs, or skipped extractors.

## When to use
- After ingestion to confirm what was discovered and ingested.
- When diagnosing extraction issues or gaps in data.

## Data sources
- Evidence database extractor run statistics tables.
- In-memory statistics collected during extraction runs.

## Key controls
- Read-only cards with per-extractor status and counts.
- Aggregated totals card across all extractors.

## Outputs
- Visual summary only; no exported output from this tab.

## Subtabs
- None

## Notes
- This tab is added as a subtab within each evidence's QTabWidget (per-evidence).
