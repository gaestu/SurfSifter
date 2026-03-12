# Bookmarks (Report Module)

Displays saved browser bookmarks as a filterable table.

## Purpose
- Review bookmarks extracted from browser profiles.
- Filter by browser or tag to focus on relevant saved links.

## Inputs
- Bookmark artifacts extracted from browser profiles.
- Tags applied to bookmark entries.

## Filters and controls
- Section Title: Custom heading text.
- Limit: Maximum number of entries (10/25/50/100/Unlimited).
- Tags: Filter by tag (All, Any Tag, or a specific tag).
- Show Folder: Toggle folder column visibility.
- Show Browser: Toggle browser column visibility.
- Show Date Added: Toggle date column visibility.
- Sort By: Order by date added, title, folder, or browser.
- Show Filter Info: Display selected filters below the table.

## Output
- Table of bookmarks with title, URL, folder path, browser, and date added (depending on toggles).

## Notes
- Only URL-type bookmarks are displayed (folder entries are excluded).
- Tag and browser lists only populate when matching data exists.
