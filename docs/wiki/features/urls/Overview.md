# URLs (Tab)

Browse, filter, and tag extracted URLs with optional matching against reference lists.

## Purpose
- Provide a searchable list of all URLs extracted from the evidence.
- Support grouping, tagging, and list matching to triage large URL sets.

## When to use
- When you need to review or filter extracted URLs by domain, source, or tag.
- When you want to match URLs against reference lists or deduplicate entries.

## Data sources
- Case database URL tables (including sources, tags, and match results).

## Key controls
- Filters: domain, source, tag, match status, and text search (Search button applies text).
- Group by domain toggle (table vs grouped tree view).
- Actions: Match Against URL Lists, Deduplicate, Tag Selected, Export CSV.

## Outputs
- CSV export of the current URL list.
- Updated tags and match results stored in the case database.

## Subtabs
- None

## Notes
- Downloads are managed in the Downloads tab (URL tab no longer triggers downloads).
- The flat URL list loads in the background, both for the first page and when you filter, page with Previous/Next, or refresh. The page counter shows "Loading..." until the new page arrives, and the UI stays responsive. A newer filter or page request cancels the older one, so only the latest request is shown.
- Refresh and filter changes reload only the visible view (table or grouped tree). The other view reloads when you switch to it.
