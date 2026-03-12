# Browser History (Report Module)

Displays browser history entries with visit metadata.

## Purpose
- Include browsing history in a report with timestamps and visit context.
- Filter by browser, tag, or transition type for focused reporting.

## Inputs
- Browser history artifacts extracted from browser profiles.
- Tags applied to history entries.

## Filters and controls
- Section Title: Custom heading text.
- Section Description: Override the default description text.
- Show Default Description: Toggle explanatory text for non-technical readers.
- Limit: Maximum number of entries (10/25/50/100/250/500/Unlimited).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Show Browser: Toggle browser column visibility.
- Show Profile: Toggle profile column visibility.
- Show Visit Count: Toggle visit count column.
- Show Transition Type: Toggle transition type column (link, typed, redirect, etc.).
- Sort By: Order by visit time, title, URL, visit count, or browser.
- Show Filter Info: Display selected filters below the table.

## Output
- Table of history entries with title, URL, visit time, visit count, browser, profile, and transition type.

## Notes
- Tag and browser lists only populate when matching data exists.
