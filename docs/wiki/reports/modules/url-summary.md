# URL Summary (Report Module)

Lists distinct URLs with occurrence counts and timestamps.

## Purpose
- Provide a compact summary of URL activity.
- Group by domain when a cleaner view is required.

## Inputs
- URL records extracted from browser artifacts.
- URL tags and reference list matches.
- Discovery sources from extractors.

## Filters and controls
- Source: Filter by discovery source (extractors or import sources).
- Matches: Filter by reference list matches (All, Any Match, or a specific list).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Group by Domain: Aggregate URLs under each domain.
- Show Dates: Show first/last seen columns.
- Sub-URLs to Show: When grouped, how many URLs to show per domain.
- Sort By: Order by counts, dates, URL, or domain length.
- Show Filter Info: Display selected filters below the table.
- Section Title: Optional custom heading (leave empty to hide it).
- Shorten URLs: Truncate long URLs with ellipsis for readability.

## Output
- URL table (or domain summary with optional sub-URLs) including counts and timestamps.

## Notes
- Source and match lists are data-driven and appear only when values exist.
