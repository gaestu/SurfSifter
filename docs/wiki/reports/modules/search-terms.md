# Search Terms (Report Module)

Displays browser search terms with multi-select tag filtering.

## Purpose
- Review what terms a user searched for across browsers and search engines.
- Filter by tags to focus on investigator-flagged search terms.

## Inputs
- Search term records populated by browser extractors (Chromium, Firefox, IE/Legacy Edge).
- Tags applied to search term entries.

## Filters and controls
- Section Title: Custom heading text.
- Section Description: Custom description text.
- Tags: Multi-select tag filter (shows only tags used on search terms).
- Browser: Filter by browser (list is data-driven).
- Search Engine: Filter by search engine (list is data-driven).
- Column visibility: URL, browser, search engine, search time, profile.
- Sort By: Order by search time, term, browser, or search engine.
- Limit: Maximum number of entries (25/50/100/250/500/Unlimited).
- Show Filter Info: Display selected filters below the table.

## Output
- Table of search term records with term, URL, browser, search engine, search time, and profile.

## Notes
- The tag filter uses OR logic: selecting multiple tags shows terms matching any of the selected tags.
- Search engines are extracted from the URL or stored by the browser; availability depends on browser type.
