# Credentials (Report Module)

Lists saved login entries with browser and tag filters.

## Purpose
- Review saved credential records and their usage timestamps.
- Identify which browsers stored logins for specific sites.

## Inputs
- Credential artifacts extracted from browser login databases.
- Tags applied to credential entries.

## Filters and controls
- Browser: Filter by browser (list is data-driven).
- Tags: Multi-select tag filter.
- Show Profile Column: Toggle profile column visibility.
- Show Password Column: Show whether a password is stored (not the password itself).
- Show Date Columns: Toggle created and last-used dates.
- Sort By: Choose ordering (last used, created, origin, browser).
- Show Filter Info: Display selected filters below the table.

## Output
- Table of credential records with origin URL, username, and date metadata.

## Notes
- The module does not decrypt or display passwords; it only indicates if a password value exists.
