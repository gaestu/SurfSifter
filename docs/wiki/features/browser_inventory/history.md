# History (Subtab)

History subtab - parsed browser history entries.

## Purpose
- Review visited URLs with timestamps and visit metadata.
- Tag and inspect history records for analysis.

## Inputs
- Browser history tables in the evidence database.

## Filters and controls
- Filters: browser, profile, visit type, and URL text.
- Apply button to run the current filters.
- Context menu for details, open/copy URL, and tagging.

## Outputs
- Tags stored on selected history records.
- Detail dialog for a single record.

## Notes
- URL actions open in the sandbox browser when available.
- After ingestion, History reloads with the filters shown above the table. Browser and profile options are rebuilt from the evidence data; a selected browser or profile that no longer exists falls back to "All".
