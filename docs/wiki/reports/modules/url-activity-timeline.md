# URL Activity Timeline (Report Module)

Summarizes domain activity with first/last seen ranges and counts.

## Purpose
- Answer when specific domains or URLs were accessed.
- Highlight the most active domains in a time window.

## Inputs
- Timeline URL events (derived from browsing artifacts).
- URL tags and reference list matches.

## Filters and controls
- Filter by Tag: All URLs, Any Tagged URL, or a specific tag.
- Filter by Match: All URLs, Any Matched URL, or a specific list.
- Domain Contains: Text filter for domain names.
- Minimum Occurrences: Minimum number of events per domain.
- Show Individual URLs: Show URLs under each domain.
- URLs per Domain: Limit URLs shown when expanded.
- Sort Domains By: Order by count or first/last seen.
- Max Domains to Show: Limit the domain list size.

## Output
- Per-domain activity summary with counts and first/last seen timestamps.
- Optional per-URL lists under each domain.

## Notes
- Requires timeline events that include URLs; run extraction before reporting.
