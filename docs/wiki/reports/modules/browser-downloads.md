# Browser Downloads (Report Module)

Displays browser download history entries with status and metadata.

## Purpose
- Review files downloaded by the browser with timestamps and state information.
- Filter by browser or tag to focus on specific download activity.

## Inputs
- Download history artifacts extracted from browser profiles.
- Tags applied to download entries.

## Filters and controls
- Section Title: Custom heading text.
- Limit: Maximum number of entries (25/50/100/250/500/Unlimited).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Show Browser: Toggle browser column visibility.
- Show State: Toggle download state column.
- Show Size: Toggle file size column.
- Show End Time: Toggle download completion time column.
- Shorten URLs: Truncate long URLs for readability.
- Sort By: Order by start time, end time, filename, URL, browser, state, or size.
- Show Filter Info: Display selected filters below the table.

## Output
- Table of download records with filename, URL, browser, state, size, and start/end times.

## Notes
- This module shows browser download history, not files fetched by the Downloads tab.
