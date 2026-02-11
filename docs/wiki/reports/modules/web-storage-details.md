# Web Storage Details (Report Module)

Displays localStorage and sessionStorage key-value pairs grouped by site.

## Purpose
- Review stored web storage content for specific sites.
- Provide readable key/value listings for reporting.

## Inputs
- Stored site records and storage entries from browser storage extraction.
- Tags applied to stored sites.

## Filters and controls
- Show Title: Toggle module title.
- Show Description: Include explanatory text for non-technical readers.
- Tags: Multi-select tag filter.
- Storage Type: All, Local Storage only, or Session Storage only.
- Max Entries per Site: Limit entries shown per origin.
- Truncate Long Values: Shorten long values to improve readability.
- Show Filter Info: Display selected filters below the content.

## Output
- Per-site sections with storage key/value pairs and counts.

## Notes
- Only sites with matching storage entries appear in the report.
